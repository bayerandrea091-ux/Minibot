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

# Initialize bot with threaded=False for Render free tier compatibility
bot = telebot.TeleBot(TOKEN, threaded=False)

# Flask app for webhook
app = Flask(__name__)

# Redis connection
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

# If no link_only set, default to False
if r.get('link_only') is None:
    r.set('link_only', 'False')

# DB helper functions
def get_groups():
    return [int(g) for g in r.smembers('groups')]

def add_group(chat_id):
    r.sadd('groups', str(chat_id))

def remove_group(chat_id):
    r.srem('groups', str(chat_id))

def get_link_only():
    return r.get('link_only') == 'True'

def set_link_only(value):
    r.set('link_only', 'True' if value else 'False')

# Handler for bot added to group
@bot.message_handler(content_types=['new_chat_members'])
def handle_new_chat_members(message):
    for member in message.new_chat_members:
        if member.id == bot.get_me().id:
            add_group(message.chat.id)
            bot.reply_to(message, "Bot added to group. Owner can now manage it.")

# Handler for bot removed from group
@bot.message_handler(content_types=['left_chat_member'])
def handle_left_chat_member(message):
    if message.left_chat_member.id == bot.get_me().id:
        remove_group(message.chat.id)

# Private /start only for owner, with optional parameter for deep linking
@bot.message_handler(commands=['start'], chat_types=['private'])
def start(message):
    if message.from_user.id != OWNER_ID:
        return
    parts = message.text.split()
    if len(parts) > 1:
        param = parts[1]
        # Assume param is a link, broadcast it to all groups
        groups = get_groups()
        if not groups:
            bot.send_message(message.chat.id, "No groups to send the link to.")
            show_menu(message.chat.id, "Choose an action:")
            return
        sent_count = 0
        last_broadcast = {}
        for group in groups:
            try:
                sent = bot.send_message(group, param)
                last_broadcast[group] = sent.message_id
                sent_count += 1
            except:
                pass
        if sent_count > 0:
            r.set('last_broadcast', json.dumps(last_broadcast))
            markup = types.InlineKeyboardMarkup()
            btn_pin_all = types.InlineKeyboardButton("Pin in All", callback_data="pin_all")
            markup.add(btn_pin_all)
            bot.send_message(message.chat.id, f"Link sent to {sent_count} groups.", reply_markup=markup)
        else:
            bot.send_message(message.chat.id, "Failed to send link to any groups.")
        show_menu(message.chat.id, "Choose an action:", message_id=None)
        return
    show_menu(message.chat.id, "Welcome, owner! Choose an action:")

# Show main menu
def show_menu(chat_id, text, message_id=None):
    markup = types.InlineKeyboardMarkup(row_width=1)
    btn_broadcast = types.InlineKeyboardButton("Broadcast/Send Message", callback_data="broadcast")
    btn_toggle = types.InlineKeyboardButton("Toggle Link-Only Mode", callback_data="toggle")
    btn_list_groups = types.InlineKeyboardButton("List Groups", callback_data="list_groups")
    markup.add(btn_broadcast, btn_toggle, btn_list_groups)
    if message_id:
        bot.edit_message_text(text, chat_id, message_id, reply_markup=markup)
    else:
        bot.send_message(chat_id, text, reply_markup=markup)

# Callback query handler
@bot.callback_query_handler(func=lambda call: True)
def callback(call):
    if call.from_user.id != OWNER_ID:
        return
    if call.data == "broadcast":
        markup = types.InlineKeyboardMarkup(row_width=1)
        btn_all = types.InlineKeyboardButton("Broadcast to All Groups", callback_data="broadcast_all")
        btn_single = types.InlineKeyboardButton("Send to Single Group", callback_data="broadcast_single")
        btn_back = types.InlineKeyboardButton("Go Back", callback_data="back")
        markup.add(btn_all, btn_single, btn_back)
        bot.edit_message_text("Choose send type:", call.message.chat.id, call.message.message_id, reply_markup=markup)
        bot.answer_callback_query(call.id, "Opened broadcast menu.")
    elif call.data == "toggle":
        current = get_link_only()
        set_link_only(not current)
        status = "ON" if get_link_only() else "OFF"
        bot.answer_callback_query(call.id, f"Link-only mode toggled to {status}.")
        show_menu(call.message.chat.id, f"Link-only mode is now {status}. Choose an action:", call.message.message_id)
    elif call.data == "list_groups":
        groups_list = get_groups()
        text = "Groups (chat IDs):\n" + "\n".join(str(g) for g in groups_list) if groups_list else "No groups yet."
        markup = types.InlineKeyboardMarkup()
        btn_back = types.InlineKeyboardButton("Go Back", callback_data="back")
        markup.add(btn_back)
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup)
        bot.answer_callback_query(call.id, "Listed groups.")
    elif call.data == "back":
        show_menu(call.message.chat.id, "Back to main menu:", call.message.message_id)
        bot.answer_callback_query(call.id, "Returned to menu.")
    elif call.data == "broadcast_all":
        bot.edit_message_text("Send me the message to broadcast (it will be sent to all groups).", call.message.chat.id, call.message.message_id)
        bot.register_next_step_handler(call.message, process_broadcast_all)
        bot.answer_callback_query(call.id, "Waiting for broadcast message.")
    elif call.data == "broadcast_single":
        bot.edit_message_text("Send me the group chat ID (e.g., -123456789).", call.message.chat.id, call.message.message_id)
        bot.register_next_step_handler(call.message, process_group_id)
        bot.answer_callback_query(call.id, "Waiting for group ID.")
    elif call.data.startswith("pin:"):
        parts = call.data.split(":")
        if len(parts) == 3:
            _, group_id_str, msg_id_str = parts
            group_id = int(group_id_str)
            msg_id = int(msg_id_str)
            try:
                member = bot.get_chat_member(group_id, bot.get_me().id)
                if member.status == 'administrator':
                    bot.pin_chat_message(group_id, msg_id)
                    bot.answer_callback_query(call.id, "Message pinned.")
                else:
                    bot.answer_callback_query(call.id, "Bot is not admin, can't pin.")
            except Exception as e:
                bot.answer_callback_query(call.id, f"Error pinning: {str(e)}")
    elif call.data == "pin_all":
        last_broadcast_str = r.get('last_broadcast')
        if last_broadcast_str:
            last_broadcast = json.loads(last_broadcast_str)
            pinned_count = 0
            for g_str, mid in last_broadcast.items():
                group_id = int(g_str)
                try:
                    member = bot.get_chat_member(group_id, bot.get_me().id)
                    if member.status == 'administrator':
                        bot.pin_chat_message(group_id, int(mid))
                        pinned_count += 1
                except:
                    pass
            bot.answer_callback_query(call.id, f"Pinned in {pinned_count} groups.")
        else:
            bot.answer_callback_query(call.id, "No recent broadcast to pin.")

# Process broadcast to all
def process_broadcast_all(message):
    if message.from_user.id != OWNER_ID:
        return
    text = message.text
    groups = get_groups()
    if not groups:
        bot.send_message(message.chat.id, "No groups to broadcast to.")
        show_menu(message.chat.id, "Choose an action:")
        return
    sent_count = 0
    last_broadcast = {}
    for group in groups:
        try:
            sent = bot.send_message(group, text)
            last_broadcast[str(group)] = sent.message_id
            sent_count += 1
        except:
            pass  # Skip if error (e.g., no send permission)
    r.set('last_broadcast', json.dumps(last_broadcast))
    feedback_text = f"Broadcast sent to {sent_count} groups."
    markup = types.InlineKeyboardMarkup()
    btn_pin_all = types.InlineKeyboardButton("Pin in All", callback_data="pin_all")
    markup.add(btn_pin_all)
    bot.send_message(message.chat.id, feedback_text, reply_markup=markup)
    show_menu(message.chat.id, "Choose an action:")

# Process group ID for single send
def process_group_id(message):
    if message.from_user.id != OWNER_ID:
        return
    try:
        group_id = int(message.text)
        groups = get_groups()
        if group_id not in groups:
            bot.send_message(message.chat.id, "Bot is not in that group or invalid ID.")
            show_menu(message.chat.id, "Choose an action:")
            return
        bot.send_message(message.chat.id, "Now send me the message to send.")
        bot.register_next_step_handler(message, lambda m: process_single_message(m, group_id))
    except ValueError:
        bot.send_message(message.chat.id, "Invalid chat ID format.")
        show_menu(message.chat.id, "Choose an action:")

# Process single message send
def process_single_message(message, group_id):
    if message.from_user.id != OWNER_ID:
        return
    text = message.text
    try:
        sent = bot.send_message(group_id, text)
        feedback_text = f"Message sent to group {group_id}."
        markup = types.InlineKeyboardMarkup()
        btn_pin = types.InlineKeyboardButton("Pin Message", callback_data=f"pin:{group_id}:{sent.message_id}")
        markup.add(btn_pin)
        bot.send_message(message.chat.id, feedback_text, reply_markup=markup)
    except Exception as e:
        bot.send_message(message.chat.id, f"Error sending: {str(e)}")
    show_menu(message.chat.id, "Choose an action:")

# Delete non-link messages in groups when toggle is on
@bot.message_handler(func=lambda m: m.chat.type in ['group', 'supergroup'])
def check_message(message):
    if get_link_only() and message.from_user.id != OWNER_ID:
        content = (message.text or "") + (message.caption or "")
        if not re.search(r'https?://[^\s]+', content):  # Simple URL regex check
            try:
                bot.delete_message(message.chat.id, message.message_id)
            except:
                pass  # If no delete permission, skip

# Webhook routes
@app.route('/', methods=['GET', 'HEAD'])
def index():
    return ''

@app.route('/', methods=['POST'])
def webhook_handler():
    if request.headers.get('content-type') == 'application/json':
        json_string = request.get_data().decode('utf-8')
        update = telebot.types.Update.de_json(json_string)
        bot.process_new_updates([update])
        return ''
    else:
        abort(403)

if __name__ == '__main__':
    bot.remove_webhook()
    time.sleep(1)
    bot.set_webhook(WEBHOOK_URL)
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
