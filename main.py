import os
import json
import re
import time
from flask import Flask, request, abort
import telebot
from telebot import types
import redis
import threading

# Environment variables
TOKEN = os.environ['TOKEN']
OWNER_ID = int(os.environ['OWNER_ID'])
WEBHOOK_URL = os.environ['WEBHOOK_URL']
REDIS_URL = os.environ['REDIS_URL']

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

# Defaults
if r.get('link_only_global') is None:
    r.set('link_only_global', 'False')
if r.get('global_start_reply') is None:
    r.set('global_start_reply', 'Hello! This bot is managed by its owner. Use groups for features.')

# Helpers (existing)
def get_groups():
    return [int(g) for g in r.smembers('groups')]

def add_group(chat_id):
    r.sadd('groups', str(chat_id))

def remove_group(chat_id):
    r.srem('groups', str(chat_id))
    r.delete(f'last_sent:{chat_id}')
    r.delete(f'link_only:{chat_id}')
    r.delete(f'repeat_task:{chat_id}')
    r.delete(f'repeat_interval:{chat_id}')
    r.delete(f'repeat_text:{chat_id}')
    r.delete(f'repeat_autodelete:{chat_id}')
    r.delete(f'group_start_reply:{chat_id}')

def is_link_only(chat_id):
    group_specific = r.get(f'link_only:{chat_id}')
    if group_specific is not None:
        return group_specific == 'True'
    return r.get('link_only_global') == 'True'

def set_link_only(chat_id, value):
    if chat_id is None:
        r.set('link_only_global', 'True' if value else 'False')
    else:
        r.set(f'link_only:{chat_id}', 'True' if value else 'False')

def save_last_sent(chat_id, message_id):
    sent_list_key = f'sent_messages:{chat_id}'
    r.lpush(sent_list_key, str(message_id))
    r.expire(sent_list_key, 172800)  # 48 hours

def get_sent_messages(chat_id):
    key = f'sent_messages:{chat_id}'
    msgs = r.lrange(key, 0, -1)
    return [int(m) for m in msgs if m.isdigit()]

def clear_sent_messages(chat_id):
    r.delete(f'sent_messages:{chat_id}')

def get_group_info(chat_id):
    try:
        chat = bot.get_chat(chat_id)
        title = chat.title or f"Group {chat_id}"
        member = bot.get_chat_member(chat_id, bot.get_me().id)
        status = "Admin" if member.status in ['administrator', 'creator'] else ("Member" if member.status == 'member' else "Other")
        return title, status
    except Exception:
        return f"Unknown Group {chat_id}", "Error"

# Repeating message logic (background threads per group)
active_repeat_threads = {}

def repeat_message_task(chat_id):
    while r.get(f'repeat_task:{chat_id}') == 'True':
        text = r.get(f'repeat_text:{chat_id}')
        if not text:
            break
        try:
            autodelete = r.get(f'repeat_autodelete:{chat_id}') == 'True'
            if autodelete:
                prev_id = get_last_sent(chat_id)
                if prev_id:
                    bot.delete_message(chat_id, prev_id)
            sent = bot.send_message(chat_id, text)
            save_last_sent(chat_id, sent.message_id)
        except Exception:
            pass
        interval = int(r.get(f'repeat_interval:{chat_id}') or 3600)
        time.sleep(interval)

def start_repeat_thread(chat_id):
    key = f'repeat_task:{chat_id}'
    if r.get(key) == 'True':
        thread = threading.Thread(target=repeat_message_task, args=(chat_id,), daemon=True)
        thread.start()
        active_repeat_threads[chat_id] = thread

# Bot added / removed (existing, unchanged)
@bot.message_handler(content_types=['new_chat_members'])
def handle_new_chat_members(message):
    for member in message.new_chat_members:
        if member.id == bot.get_me().id:
            chat_id = message.chat.id
            add_group(chat_id)
            kicked = False

            title = "No title"
            chat_type = "Unknown"
            member_count = "Unknown"
            admins_text = "Cannot fetch admins (likely no rights or kicked)"

            try:
                chat = bot.get_chat(chat_id)
                title = chat.title or "No title"
                chat_type = chat.type
            except telebot.apihelper.ApiTelegramException as e:
                if '403' in str(e) and 'kicked' in str(e).lower():
                    kicked = True
                title = f"Group {chat_id} (fetch failed: {str(e)})"

            if not kicked:
                try:
                    member_count = bot.get_chat_member_count(chat_id)
                except telebot.apihelper.ApiTelegramException as e:
                    if '403' in str(e) and 'kicked' in str(e).lower():
                        kicked = True
                    member_count = f"Unknown ({str(e)})"

            if not kicked:
                try:
                    admins = bot.get_chat_administrators(chat_id)
                    admin_list = []
                    for admin in admins:
                        user = admin.user
                        name = user.full_name
                        username = f"@{user.username}" if user.username else ""
                        role = "Owner" if admin.status == 'creator' else "Admin"
                        admin_list.append(f"{role}: {name} {username}")
                    admins_text = "\n".join(admin_list) if admin_list else "No admins visible"
                except telebot.apihelper.ApiTelegramException as e:
                    if '403' in str(e) and 'kicked' in str(e).lower():
                        kicked = True
                    admins_text = f"Cannot fetch ({str(e)})"

            notification = (
                f"Bot was added to a new group!{' (but kicked immediately)' if kicked else ''}\n\n"
                f"Group Title: {title}\n"
                f"Chat ID: {chat_id}\n"
                f"Type: {chat_type}\n"
                f"Approx. members: {member_count}\n\n"
                f"Admins / Owner:\n{admins_text}"
            )

            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("Go Back", callback_data="back"))
            bot.send_message(OWNER_ID, notification, reply_markup=markup)

            invite_link = "https://t.me/AllMusicShazamandlyrics_bot?startgroup=true&admin=change_info+delete_messages+restrict_members+invite_users+pin_messages+manage_video_chats+anonymous+manage_chat+post_stories+edit_stories+delete_stories"
            try:
                bot.reply_to(message, invite_link)
            except telebot.apihelper.ApiTelegramException as e:
                if '403' in str(e):
                    kicked = True

            if kicked:
                remove_group(chat_id)

@bot.message_handler(content_types=['left_chat_member'])
def handle_left_chat_member(message):
    if message.left_chat_member.id == bot.get_me().id:
        remove_group(message.chat.id)

# /start in private (global custom reply)
@bot.message_handler(commands=['start'], chat_types=['private'])
def start(message):
    if message.from_user.id == OWNER_ID:
        parts = message.text.split()
        if len(parts) > 1:
            # Deep link - ignore or handle if needed
            pass
        show_main_menu(message.chat.id, "Welcome! Choose action:")
    else:
        reply = r.get('global_start_reply') or "Hello! This bot is managed by its owner."
        bot.reply_to(message, reply)

# /start@bot in groups (per-group custom reply)
@bot.message_handler(commands=['start@AllMusicShazamandlyrics_bot'], chat_types=['group', 'supergroup'])
def group_start_command(message):
    chat_id = message.chat.id
    custom_reply = r.get(f'group_start_reply:{chat_id}')
    if custom_reply:
        bot.reply_to(message, custom_reply)
    # else silent (as per your previous behavior)

# Main menu (added global start reply button)
def show_main_menu(chat_id, text, message_id=None):
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(
        types.InlineKeyboardButton("Broadcast to All Groups", callback_data="broadcast_all"),
        types.InlineKeyboardButton("Send to One Group", callback_data="my_groups_send"),
        types.InlineKeyboardButton("Toggle Global Link-Only", callback_data="toggle_global"),
        types.InlineKeyboardButton("Set Global /start Reply", callback_data="set_global_start_reply"),
        types.InlineKeyboardButton("My Groups", callback_data="my_groups"),
    )
    if message_id:
        bot.edit_message_text(text, chat_id, message_id, reply_markup=markup)
    else:
        bot.send_message(chat_id, text, reply_markup=markup)

# Callback handler (added new features)
@bot.callback_query_handler(func=lambda call: True)
def callback(call):
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "Only owner can use this bot.")
        return

    cid = call.message.chat.id
    mid = call.message.message_id

    if call.data == "set_global_start_reply":
        bot.edit_message_text("Send the new custom reply for private /start to the bot.\nSend 'reset' to remove.", cid, mid)
        bot.register_next_step_handler(call.message, process_global_start_reply)
        bot.answer_callback_query(call.id)

    elif call.data.startswith("set_group_start_reply:"):
        _, chat_id_str = call.data.split(":", 1)
        chat_id = int(chat_id_str)
        bot.edit_message_text("Send the new custom reply for /start@AllMusicShazamandlyrics_bot in this group.\nSend 'reset' to remove.", cid, mid)
        bot.register_next_step_handler(call.message, lambda m: process_group_start_reply(m, chat_id))
        bot.answer_callback_query(call.id)

    elif call.data == "broadcast_all":
        bot.edit_message_text("Send the message to broadcast to **all groups**.", cid, mid)
        bot.register_next_step_handler(call.message, process_broadcast_all)
        bot.answer_callback_query(call.id)

    elif call.data == "toggle_global":
        current = r.get('link_only_global') == 'True'
        r.set('link_only_global', 'False' if current else 'True')
        status = "OFF" if current else "ON"
        bot.answer_callback_query(call.id, f"Global link-only → {status}")
        show_main_menu(cid, f"Global link-only is now {status}", mid)

    elif call.data in ["my_groups", "my_groups_send"]:
        groups = get_groups()
        if not groups:
            text = "No groups added yet.\nAdd the bot to some groups first!"
            markup = types.InlineKeyboardMarkup()
            markup.add(
                types.InlineKeyboardButton("Refresh", callback_data="refresh_groups"),
                types.InlineKeyboardButton("Go Back", callback_data="back"),
            )
            bot.edit_message_text(text, cid, mid, reply_markup=markup)
            bot.answer_callback_query(call.id, "No groups")
            return

        markup = types.InlineKeyboardMarkup(row_width=1)
        for g in groups:
            title, status = get_group_info(g)
            btn_text = f"{title} ({status})"
            data = f"group_menu:{g}" if call.data == "my_groups" else f"send_to_group:{g}"
            markup.add(types.InlineKeyboardButton(btn_text, callback_data=data))

        markup.add(
            types.InlineKeyboardButton("Refresh List", callback_data="refresh_groups"),
            types.InlineKeyboardButton("Go Back", callback_data="back"),
        )

        text = "Your Groups:" if call.data == "my_groups" else "Choose group to send message:"
        bot.edit_message_text(text, cid, mid, reply_markup=markup)
        bot.answer_callback_query(call.id)

    elif call.data == "refresh_groups":
        groups = get_groups()[:]
        removed = 0
        for g in groups:
            try:
                bot.get_chat(g)
            except telebot.apihelper.ApiTelegramException as e:
                if "chat not found" in str(e).lower() or "forbidden" in str(e).lower():
                    remove_group(g)
                    removed += 1
        bot.answer_callback_query(call.id, f"Refreshed. Removed {removed} invalid groups.")
        callback(types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, data="my_groups"))

    elif call.data.startswith("group_menu:"):
        _, chat_id_str = call.data.split(":", 1)
        chat_id = int(chat_id_str)
        title, status = get_group_info(chat_id)
        is_admin = status == "Admin"
        link_only_this = is_link_only(chat_id)
        repeat_on = r.get(f'repeat_task:{chat_id}') == 'True'
        autodelete = r.get(f'repeat_autodelete:{chat_id}') == 'True'

        text = (
            f"Group: {title}\n"
            f"Status: {status}\n"
            f"Link-only mode: {'ON' if link_only_this else 'OFF'}\n"
            f"Repeating messages: {'ON' if repeat_on else 'OFF'}\n\n"
            f"Choose action:"
        )

        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(types.InlineKeyboardButton("Send Message", callback_data=f"send_to_group:{chat_id}"))
        markup.add(types.InlineKeyboardButton("Setup Repeating Messages", callback_data=f"setup_repeat:{chat_id}"))
        if is_admin and get_last_sent(chat_id):
            markup.add(types.InlineKeyboardButton("Pin Last Sent Message", callback_data=f"pin_last:{chat_id}"))
        if len(get_sent_messages(chat_id)) > 0:
            markup.add(types.InlineKeyboardButton("Purge All My Messages", callback_data=f"purge:{chat_id}"))
        markup.add(types.InlineKeyboardButton("Set /start@ Reply for Group", callback_data=f"set_group_start_reply:{chat_id}"))
        markup.add(types.InlineKeyboardButton(f"{'Disable' if link_only_this else 'Enable'} Link-Only Here", 
                                             callback_data=f"toggle_group:{chat_id}"))
        markup.add(types.InlineKeyboardButton("Go Back", callback_data="my_groups"))

        bot.edit_message_text(text, cid, mid, reply_markup=markup)
        bot.answer_callback_query(call.id)

    elif call.data.startswith("setup_repeat:"):
        _, chat_id_str = call.data.split(":", 1)
        chat_id = int(chat_id_str)
        repeat_on = r.get(f'repeat_task:{chat_id}') == 'True'
        interval_sec = r.get(f'repeat_interval:{chat_id}') or "3600"
        autodelete = r.get(f'repeat_autodelete:{chat_id}') == 'True'

        text = (
            f"Repeating messages setup for group {chat_id}\n"
            f"Status: {'ON' if repeat_on else 'OFF'}\n"
            f"Interval: {interval_sec} seconds\n"
            f"Auto-delete previous: {'ON' if autodelete else 'OFF'}\n\n"
            f"Choose:"
        )

        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton("Turn ON", callback_data=f"repeat_on:{chat_id}"),
            types.InlineKeyboardButton("Turn OFF", callback_data=f"repeat_off:{chat_id}")
        )
        markup.add(
            types.InlineKeyboardButton("Set interval (sec)", callback_data=f"set_interval_sec:{chat_id}"),
            types.InlineKeyboardButton("Set interval (min)", callback_data=f"set_interval_min:{chat_id}")
        )
        markup.add(
            types.InlineKeyboardButton(f"Auto-delete prev: {'ON' if autodelete else 'OFF'}", callback_data=f"toggle_autodelete:{chat_id}")
        )
        markup.add(types.InlineKeyboardButton("Go Back", callback_data=f"group_menu:{chat_id}"))

        bot.edit_message_text(text, cid, mid, reply_markup=markup)
        bot.answer_callback_query(call.id)

    elif call.data.startswith("repeat_on:"):
        _, chat_id_str = call.data.split(":", 1)
        chat_id = int(chat_id_str)
        r.set(f'repeat_task:{chat_id}', 'True')
        start_repeat_thread(chat_id)
        bot.answer_callback_query(call.id, "Repeating ON")
        callback(types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, data=f"setup_repeat:{chat_id}"))

    elif call.data.startswith("repeat_off:"):
        _, chat_id_str = call.data.split(":", 1)
        chat_id = int(chat_id_str)
        r.set(f'repeat_task:{chat_id}', 'False')
        bot.answer_callback_query(call.id, "Repeating OFF")
        callback(types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, data=f"setup_repeat:{chat_id}"))

    elif call.data.startswith("set_interval_sec:"):
        _, chat_id_str = call.data.split(":", 1)
        chat_id = int(chat_id_str)
        bot.edit_message_text("Send number of seconds for repeat interval (e.g. 300 for 5 min)", cid, mid)
        bot.register_next_step_handler(call.message, lambda m: process_interval(m, chat_id, "sec"))
        bot.answer_callback_query(call.id)

    elif call.data.startswith("set_interval_min:"):
        _, chat_id_str = call.data.split(":", 1)
        chat_id = int(chat_id_str)
        bot.edit_message_text("Send number of minutes for repeat interval (e.g. 5)", cid, mid)
        bot.register_next_step_handler(call.message, lambda m: process_interval(m, chat_id, "min"))
        bot.answer_callback_query(call.id)

    elif call.data.startswith("toggle_autodelete:"):
        _, chat_id_str = call.data.split(":", 1)
        chat_id = int(chat_id_str)
        current = r.get(f'repeat_autodelete:{chat_id}') == 'True'
        r.set(f'repeat_autodelete:{chat_id}', 'False' if current else 'True')
        bot.answer_callback_query(call.id, f"Auto-delete previous: {'OFF' if current else 'ON'}")
        callback(types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, data=f"setup_repeat:{chat_id}"))

    elif call.data.startswith("purge:"):
        _, chat_id_str = call.data.split(":", 1)
        chat_id = int(chat_id_str)
        msg_ids = get_sent_messages(chat_id)
        deleted = 0
        for mid in msg_ids:
            try:
                bot.delete_message(chat_id, mid)
                deleted += 1
            except:
                pass
        clear_sent_messages(chat_id)
        bot.answer_callback_query(call.id, f"Purged {deleted} messages (some may be too old)")
        callback(types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, data=f"group_menu:{chat_id}"))

    elif call.data.startswith("send_to_group:"):
        _, chat_id_str = call.data.split(":", 1)
        chat_id = int(chat_id_str)
        bot.edit_message_text("Send the message now (text/link/etc). It will be sent and offer pinning if admin.", cid, mid)
        bot.register_next_step_handler(call.message, lambda m: process_single_message(m, chat_id))
        bot.answer_callback_query(call.id)

    elif call.data.startswith("pin_last:"):
        _, chat_id_str = call.data.split(":", 1)
        chat_id = int(chat_id_str)
        msg_id = get_last_sent(chat_id)
        if not msg_id:
            bot.answer_callback_query(call.id, "No last message tracked.", show_alert=True)
            return
        try:
            bot.pin_chat_message(chat_id, msg_id)
            bot.answer_callback_query(call.id, "Pinned successfully!")
        except Exception as e:
            bot.answer_callback_query(call.id, f"Cannot pin: {str(e)}", show_alert=True)

    elif call.data.startswith("toggle_group:"):
        _, chat_id_str = call.data.split(":", 1)
        chat_id = int(chat_id_str)
        current = is_link_only(chat_id)
        set_link_only(chat_id, not current)
        status = "OFF" if current else "ON"
        bot.answer_callback_query(call.id, f"Link-only for this group: {status}")
        callback(types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, data=f"group_menu:{chat_id}"))

    elif call.data == "back":
        show_main_menu(cid, "Main menu:", mid)
        bot.answer_callback_query(call.id)

# Process global /start reply
def process_global_start_reply(message):
    if message.from_user.id != OWNER_ID: return
    text = message.text.strip()
    if text.lower() == 'reset':
        r.delete('global_start_reply')
        bot.send_message(message.chat.id, "Global /start reply reset.")
    else:
        r.set('global_start_reply', text)
        bot.send_message(message.chat.id, "Global /start reply updated.")
    show_main_menu(message.chat.id,
