import os
import json
import re
import time
from flask import Flask, request, abort
import telebot
from telebot import types
import redis

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

# Helpers
def get_groups():
    return [int(g) for g in r.smembers('groups')]

def add_group(chat_id):
    r.sadd('groups', str(chat_id))

def remove_group(chat_id):
    r.srem('groups', str(chat_id))
    r.delete(f'last_sent:{chat_id}')
    r.delete(f'link_only:{chat_id}')

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
    r.set(f'last_sent:{chat_id}', str(message_id))

def get_last_sent(chat_id):
    val = r.get(f'last_sent:{chat_id}')
    return int(val) if val else None

def get_group_info(chat_id):
    try:
        chat = bot.get_chat(chat_id)
        title = chat.title or f"Group {chat_id}"
        member = bot.get_chat_member(chat_id, bot.get_me().id)
        if member.status in ['administrator', 'creator']:
            status = "Admin"
        elif member.status == 'member':
            status = "Member"
        else:
            status = "Other"
        return title, status
    except Exception:
        return f"Unknown Group {chat_id}", "Error"

# Bot added / removed
@bot.message_handler(content_types=['new_chat_members'])
def handle_new_chat_members(message):
    for member in message.new_chat_members:
        if member.id == bot.get_me().id:
            chat_id = message.chat.id
            add_group(chat_id)
            kicked = False

            # Safe fetch
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

# /start only for owner + deep link support
@bot.message_handler(commands=['start'], chat_types=['private'])
def start(message):
    if message.from_user.id != OWNER_ID:
        return
    parts = message.text.split()
    if len(parts) > 1:
        param = ' '.join(parts[1:])
        # Note: broadcast_message function was missing in previous code - assuming it's not used or replace with actual broadcast
        # If you need deep link broadcast, add the function or comment this out
        bot.send_message(message.chat.id, "Deep link received but broadcast not implemented in this version.")
        return
    show_main_menu(message.chat.id, "Welcome! Choose action:")

def show_main_menu(chat_id, text, message_id=None):
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(
        types.InlineKeyboardButton("Broadcast to All Groups", callback_data="broadcast_all"),
        types.InlineKeyboardButton("Send to One Group", callback_data="my_groups_send"),
        types.InlineKeyboardButton("Toggle Global Link-Only", callback_data="toggle_global"),
        types.InlineKeyboardButton("My Groups", callback_data="my_groups"),
    )
    if message_id:
        bot.edit_message_text(text, chat_id, message_id, reply_markup=markup)
    else:
        bot.send_message(chat_id, text, reply_markup=markup)

# Callback central handler
@bot.callback_query_handler(func=lambda call: True)
def callback(call):
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "Only owner can use this bot.")
        return

    cid = call.message.chat.id
    mid = call.message.message_id

    if call.data == "broadcast_all":
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
        text = f"Group: {title}\nStatus: {status}\nLink-only mode: {'ON' if link_only_this else 'OFF'}\n\nChoose action:"

        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(types.InlineKeyboardButton("Send Message", callback_data=f"send_to_group:{chat_id}"))
        if is_admin and get_last_sent(chat_id):
            markup.add(types.InlineKeyboardButton("Pin Last Sent Message", callback_data=f"pin_last:{chat_id}"))
        markup.add(types.InlineKeyboardButton(f"{'Disable' if link_only_this else 'Enable'} Link-Only Here", 
                                             callback_data=f"toggle_group:{chat_id}"))
        markup.add(types.InlineKeyboardButton("Go Back", callback_data="my_groups"))

        bot.edit_message_text(text, cid, mid, reply_markup=markup)
        bot.answer_callback_query(call.id)

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

# Broadcast to all
def process_broadcast_all(message):
    if message.from_user.id != OWNER_ID: return
    text = message.text
    groups = get_groups()
    if not groups:
        bot.send_message(message.chat.id, "No groups yet.")
        show_main_menu(message.chat.id, "Choose action:")
        return
    sent_count = 0
    for group in groups:
        try:
            sent = bot.send_message(group, text)
            save_last_sent(group, sent.message_id)
            sent_count += 1
        except:
            pass
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("Go Back", callback_data="back"))
    bot.send_message(message.chat.id, f"Sent to {sent_count} groups.\n(Last message saved per group for pinning)", reply_markup=markup)
    show_main_menu(message.chat.id, "Choose action:")

# Single group send
def process_single_message(message, group_id):
    if message.from_user.id != OWNER_ID: return
    text = message.text
    try:
        sent = bot.send_message(group_id, text)
        save_last_sent(group_id, sent.message_id)
        markup = types.InlineKeyboardMarkup()
        btn_pin = types.InlineKeyboardButton("Pin this message", callback_data=f"pin_last:{group_id}")
        markup.add(btn_pin)
        bot.send_message(message.chat.id, "Message sent.", reply_markup=markup)
    except Exception as e:
        bot.send_message(message.chat.id, f"Error: {str(e)}")
    show_main_menu(message.chat.id, "Choose action:")

# Delete non-links when mode on
@bot.message_handler(func=lambda m: m.chat.type in ['group', 'supergroup'])
def check_message(message):
    if message.from_user.id == OWNER_ID:
        return
    if not is_link_only(message.chat.id):
        return
    content = (message.text or "") + (message.caption or "")
    if not re.search(r'https?://[^\s]+', content):
        try:
            bot.delete_message(message.chat.id, message.message_id)
        except:
            pass

# Webhook
@app.route('/', methods=['GET', 'HEAD'])
def index():
    return ''

@app.route('/', methods=['POST'])
def webhook_handler():
    if request.headers.get('content-type') == 'application/json':
        json_string = request.get_data().decode('utf-8')
        update = telebot.types.Update.de_json(json_string)
        try:
            bot.process_new_updates([update])
        except Exception as e:
            print(f"Error processing update: {str(e)}")
        return ''
    abort(403)

if __name__ == '__main__':
    bot.remove_webhook()
    time.sleep(1)
    bot.set_webhook(WEBHOOK_URL)
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
