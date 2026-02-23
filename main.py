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

# ─── Defaults ────────────────────────────────────────────────────────────────
if r.get('link_only_global') is None:
    r.set('link_only_global', 'False')
if r.get('global_start_reply') is None:
    r.set('global_start_reply', 'Hello! This bot is managed by its owner. Use groups for features.')
if r.get('global_join_reply_enabled') is None:
    r.set('global_join_reply_enabled', 'False')
if r.get('global_join_reply_text') is None:
    r.set('global_join_reply_text', 'Welcome to the group!')

# ─── Group helpers ────────────────────────────────────────────────────────────
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
    r.delete(f'join_reply_enabled:{chat_id}')
    r.delete(f'join_reply_text:{chat_id}')
    r.delete(f'global_repeat_task:{chat_id}')
    # Also clear cache entries
    r.delete(f'cache_group_title:{chat_id}')
    r.delete(f'cache_group_status:{chat_id}')

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
    r.expire(sent_list_key, 604800)  # 7 days

def get_sent_messages(chat_id):
    key = f'sent_messages:{chat_id}'
    msgs = r.lrange(key, 0, -1)
    return [int(m) for m in msgs if m.isdigit()]

def clear_sent_messages(chat_id):
    r.delete(f'sent_messages:{chat_id}')

# FIX #4: Cache group title and bot status in Redis (10 min TTL).
# The group list now loads instantly from cache instead of making 2 live
# Telegram API calls per group every time the list is opened.
def get_group_info(chat_id, force_refresh=False):
    title_key = f'cache_group_title:{chat_id}'
    status_key = f'cache_group_status:{chat_id}'
    if not force_refresh:
        cached_title = r.get(title_key)
        cached_status = r.get(status_key)
        if cached_title and cached_status:
            return cached_title, cached_status
    # Cache miss or forced refresh — hit the Telegram API
    try:
        chat = bot.get_chat(chat_id)
        title = chat.title or f"Group {chat_id}"
        member = bot.get_chat_member(chat_id, bot.get_me().id)
        status = "Admin" if member.status in ['administrator', 'creator'] else (
            "Member" if member.status == 'member' else "Other"
        )
        r.set(title_key, title, ex=600)    # 10 min TTL
        r.set(status_key, status, ex=600)
        return title, status
    except Exception:
        return f"Group {chat_id}", "Error"

def bot_can_add_members(chat_id):
    """Check if bot has can_invite_users or can_promote_members permission."""
    try:
        me = bot.get_me()
        member = bot.get_chat_member(chat_id, me.id)
        if member.status == 'creator':
            return True, True  # can_invite, can_promote
        if member.status == 'administrator':
            can_invite = getattr(member, 'can_invite_users', False)
            can_promote = getattr(member, 'can_promote_members', False)
            return can_invite, can_promote
        return False, False
    except Exception:
        return False, False

def bot_can_pin(chat_id):
    """Check if bot can pin messages based on actual permissions."""
    try:
        me = bot.get_me()
        member = bot.get_chat_member(chat_id, me.id)
        if member.status == 'creator':
            return True
        if member.status == 'administrator':
            return getattr(member, 'can_pin_messages', False)
        # Bot is a regular member — check if the group allows members to pin
        chat = bot.get_chat(chat_id)
        permissions = getattr(chat, 'permissions', None)
        if permissions:
            return getattr(permissions, 'can_pin_messages', False)
        return False
    except Exception:
        return False

# ─── User tracking ────────────────────────────────────────────────────────────
def track_user(user_id, username=None, full_name=None):
    r.sadd('bot_users', str(user_id))
    r.hset('user_info', str(user_id), json.dumps({
        'username': username,
        'full_name': full_name,
        'first_seen': r.hget('user_first_seen', str(user_id)) or str(int(time.time()))
    }))
    if not r.hexists('user_first_seen', str(user_id)):
        r.hset('user_first_seen', str(user_id), str(int(time.time())))

def get_all_users():
    return [int(u) for u in r.smembers('bot_users')]

def save_private_sent(user_id, message_id):
    key = f'private_sent:{user_id}'
    r.lpush(key, str(message_id))
    r.expire(key, 604800)

def get_private_sent(user_id):
    key = f'private_sent:{user_id}'
    msgs = r.lrange(key, 0, -1)
    return [int(m) for m in msgs if m.isdigit()]

def clear_private_sent(user_id):
    r.delete(f'private_sent:{user_id}')

# ─── Repeating message logic (per-group) ─────────────────────────────────────
active_repeat_threads = {}

def repeat_message_task(chat_id):
    while r.get(f'repeat_task:{chat_id}') == 'True':
        text = r.get(f'repeat_text:{chat_id}')
        if not text:
            break
        try:
            autodelete_prev = r.get(f'repeat_autodelete:{chat_id}') == 'True'
            self_delete_after = r.get(f'repeat_self_delete:{chat_id}')

            if autodelete_prev:
                prev_id = r.get(f'last_sent:{chat_id}')
                if prev_id:
                    try:
                        bot.delete_message(chat_id, int(prev_id))
                    except Exception:
                        pass

            sent = bot.send_message(chat_id, text)
            r.set(f'last_sent:{chat_id}', sent.message_id)
            save_last_sent(chat_id, sent.message_id)

            if self_delete_after:
                secs = int(self_delete_after)
                def _delayed_delete(cid, mid, delay):
                    time.sleep(delay)
                    try:
                        bot.delete_message(cid, mid)
                    except Exception:
                        pass
                t = threading.Thread(target=_delayed_delete, args=(chat_id, sent.message_id, secs), daemon=True)
                t.start()

        except Exception:
            pass
        interval = int(r.get(f'repeat_interval:{chat_id}') or 3600)
        time.sleep(interval)

def start_repeat_thread(chat_id):
    key = f'repeat_task:{chat_id}'
    if r.get(key) == 'True':
        existing = active_repeat_threads.get(chat_id)
        if existing and existing.is_alive():
            return
        thread = threading.Thread(target=repeat_message_task, args=(chat_id,), daemon=True)
        thread.start()
        active_repeat_threads[chat_id] = thread

# ─── Global repeating message logic (all groups) ─────────────────────────────
global_repeat_thread = None

def global_repeat_task():
    while r.get('global_repeat_task') == 'True':
        text = r.get('global_repeat_text')
        if not text:
            break
        interval = int(r.get('global_repeat_interval') or 3600)
        self_delete_after = r.get('global_repeat_self_delete')
        autodelete_prev = r.get('global_repeat_autodelete') == 'True'

        for chat_id in get_groups():
            try:
                if autodelete_prev:
                    prev_id = r.get(f'global_last_sent:{chat_id}')
                    if prev_id:
                        try:
                            bot.delete_message(chat_id, int(prev_id))
                        except Exception:
                            pass
                sent = bot.send_message(chat_id, text)
                r.set(f'global_last_sent:{chat_id}', sent.message_id)

                if self_delete_after:
                    secs = int(self_delete_after)
                    def _del(cid, mid, delay):
                        time.sleep(delay)
                        try:
                            bot.delete_message(cid, mid)
                        except Exception:
                            pass
                    threading.Thread(target=_del, args=(chat_id, sent.message_id, secs), daemon=True).start()
            except Exception:
                pass
        time.sleep(interval)

def start_global_repeat_thread():
    global global_repeat_thread
    if r.get('global_repeat_task') == 'True':
        if global_repeat_thread and global_repeat_thread.is_alive():
            return
        global_repeat_thread = threading.Thread(target=global_repeat_task, daemon=True)
        global_repeat_thread.start()

# ─────────────────────────────────────────────────────────────────────────────
#  GROUP EVENT HANDLERS
# ─────────────────────────────────────────────────────────────────────────────

@bot.message_handler(content_types=['new_chat_members'])
def handle_new_chat_members(message):
    chat_id = message.chat.id

    # ── Handle bot joining ──────────────────────────────────────────────────
    for member in message.new_chat_members:
        if member.id == bot.get_me().id:
            add_group(chat_id)
            kicked = False
            title = "No title"
            chat_type = "Unknown"
            member_count = "Unknown"
            admins_text = "Cannot fetch admins"
            group_link = None
            bot_is_admin = False

            try:
                chat = bot.get_chat(chat_id)
                title = chat.title or "No title"
                chat_type = chat.type
                group_link = chat.invite_link or f"https://t.me/c/{str(chat_id).replace('-100','')}"
                # Warm the cache immediately when bot joins
                r.set(f'cache_group_title:{chat_id}', title, ex=600)
            except telebot.apihelper.ApiTelegramException as e:
                if '403' in str(e) and 'kicked' in str(e).lower():
                    kicked = True
                title = f"Group {chat_id} (fetch failed)"

            if not kicked:
                try:
                    member_count = bot.get_chat_member_count(chat_id)
                except Exception:
                    pass

            if not kicked:
                try:
                    bot_member = bot.get_chat_member(chat_id, bot.get_me().id)
                    bot_is_admin = bot_member.status in ['administrator', 'creator']
                    status_str = "Admin" if bot_is_admin else "Member"
                    r.set(f'cache_group_status:{chat_id}', status_str, ex=600)
                except Exception:
                    pass

            if not kicked:
                try:
                    admins = bot.get_chat_administrators(chat_id)
                    admin_list = []
                    for admin in admins:
                        user = admin.user
                        name = user.full_name
                        username = f"@{user.username}" if user.username else ""
                        profile_link = f"[{name}](tg://user?id={user.id})"
                        role = "👑 Owner" if admin.status == 'creator' else "🛡 Admin"
                        admin_list.append(f"{role}: {profile_link} {username}")
                    admins_text = "\n".join(admin_list) if admin_list else "No admins visible"
                except Exception as e:
                    admins_text = f"Cannot fetch ({str(e)})"

            notification = (
                f"{'⚠️ Bot added but kicked immediately!' if kicked else '✅ Bot added to a new group!'}\n\n"
                f"📌 *Group Title:* {title}\n"
                f"🆔 *Chat ID:* `{chat_id}`\n"
                f"📂 *Type:* {chat_type}\n"
                f"👥 *Members:* {member_count}\n"
                f"🤖 *Bot status:* {'Admin' if bot_is_admin else 'Member'}\n"
            )

            if group_link and bot_is_admin:
                notification += f"🔗 *Group Link:* {group_link}\n"

            notification += f"\n*Admins / Owner:*\n{admins_text}"

            markup = types.InlineKeyboardMarkup(row_width=1)
            if bot_is_admin and not kicked:
                markup.add(types.InlineKeyboardButton(
                    "➕ Add Account(s) as Admin", callback_data=f"add_to_group:{chat_id}:admin"
                ))
            markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data="back"))

            try:
                bot.send_message(OWNER_ID, notification, parse_mode='Markdown', reply_markup=markup)
            except Exception:
                bot.send_message(OWNER_ID, notification.replace('*','').replace('`',''), reply_markup=markup)

            invite_link = "https://t.me/AllMusicShazamandlyrics_bot?startgroup=true&admin=change_info+delete_messages+restrict_members+invite_users+pin_messages+manage_video_chats+anonymous+manage_chat+post_stories+edit_stories+delete_stories"
            try:
                bot.reply_to(message, invite_link)
            except telebot.apihelper.ApiTelegramException as e:
                if '403' in str(e):
                    kicked = True

            if kicked:
                remove_group(chat_id)
            return

    # ── Handle regular user joining → join reply ────────────────────────────
    for member in message.new_chat_members:
        if member.id == bot.get_me().id:
            continue
        group_enabled = r.get(f'join_reply_enabled:{chat_id}')
        global_enabled = r.get('global_join_reply_enabled') == 'True'

        if group_enabled == 'True':
            text = r.get(f'join_reply_text:{chat_id}') or r.get('global_join_reply_text') or "Welcome!"
            try:
                bot.reply_to(message, text)
            except Exception:
                pass
        elif group_enabled != 'False' and global_enabled:
            text = r.get('global_join_reply_text') or "Welcome!"
            try:
                bot.reply_to(message, text)
            except Exception:
                pass


@bot.message_handler(content_types=['left_chat_member'])
def handle_left_chat_member(message):
    if message.left_chat_member.id == bot.get_me().id:
        remove_group(message.chat.id)


# ─────────────────────────────────────────────────────────────────────────────
#  PRIVATE COMMANDS
# ─────────────────────────────────────────────────────────────────────────────

@bot.message_handler(commands=['start'], chat_types=['private'])
def start(message):
    user = message.from_user
    track_user(user.id, user.username, user.full_name)
    if user.id == OWNER_ID:
        show_main_menu(message.chat.id, "👋 Welcome back! Choose action:")
    else:
        reply = r.get('global_start_reply') or "Hello! This bot is managed by its owner."
        bot.reply_to(message, reply)

@bot.message_handler(commands=['stats'], chat_types=['private'])
def stats_command(message):
    if message.from_user.id != OWNER_ID:
        return
    total = r.scard('bot_users')
    now = int(time.time())
    thirty_days_ago = now - (30 * 24 * 3600)
    new_users = 0
    for uid in r.smembers('bot_users'):
        first_seen = r.hget('user_first_seen', uid)
        if first_seen and int(first_seen) >= thirty_days_ago:
            new_users += 1
    groups_count = r.scard('groups')
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🏠 Main Menu", callback_data="back"))
    bot.send_message(
        message.chat.id,
        f"📊 *Bot Statistics*\n\n"
        f"👤 Total users: *{total}*\n"
        f"🆕 New users (last 30 days): *{new_users}*\n"
        f"👥 Active groups: *{groups_count}*",
        parse_mode='Markdown',
        reply_markup=markup
    )

# FIX #1: /start@ group command handler.
# pyTeleBot strips @BotUsername from commands before matching, so the old
# commands=['start@AllMusicShazamandlyrics_bot'] decorator never fired.
# Using func= with a raw text check is the correct and reliable fix.
@bot.message_handler(
    func=lambda m: (
        m.chat.type in ['group', 'supergroup'] and
        m.text is not None and
        m.text.strip().lower().startswith('/start@allmusicshazamandlyrics_bot')
    )
)
def group_start_command(message):
    chat_id = message.chat.id
    # Group-specific reply takes priority over global
    custom_reply = r.get(f'group_start_reply:{chat_id}')
    if custom_reply:
        bot.reply_to(message, custom_reply)
        return
    # Fall back to global group /start@ reply
    global_reply = r.get('global_group_start_reply')
    if global_reply:
        bot.reply_to(message, global_reply)


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN MENU
# ─────────────────────────────────────────────────────────────────────────────

def show_main_menu(chat_id, text, message_id=None):
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(
        types.InlineKeyboardButton("📢 Broadcast to All Groups", callback_data="broadcast_all"),
        types.InlineKeyboardButton("📨 Send to One Group", callback_data="my_groups_send"),
        types.InlineKeyboardButton("🔗 Toggle Global Link-Only", callback_data="toggle_global"),
        types.InlineKeyboardButton("💬 Set Global /start Reply", callback_data="set_global_start_reply"),
        types.InlineKeyboardButton("📝 Set Global /start@ Group Reply", callback_data="set_global_group_start_reply"),
        types.InlineKeyboardButton("👋 Join Reply (Global)", callback_data="global_join_reply_menu"),
        types.InlineKeyboardButton("🔁 Global Broadcast Repeat", callback_data="global_repeat_menu"),
        types.InlineKeyboardButton("📣 Broadcast to Bot Users", callback_data="broadcast_users"),
        types.InlineKeyboardButton("🗑 Delete All Private Sent Msgs", callback_data="delete_all_private"),
        types.InlineKeyboardButton("➕ Add Account to Group", callback_data="add_account_menu"),
        types.InlineKeyboardButton("📊 Bot Stats", callback_data="bot_stats"),
        types.InlineKeyboardButton("👥 My Groups", callback_data="my_groups"),
    )
    if message_id:
        try:
            bot.edit_message_text(text, chat_id, message_id, reply_markup=markup)
        except Exception:
            bot.send_message(chat_id, text, reply_markup=markup)
    else:
        bot.send_message(chat_id, text, reply_markup=markup)


# ─────────────────────────────────────────────────────────────────────────────
#  BACK BUTTON HELPER
# ─────────────────────────────────────────────────────────────────────────────

def _back_markup(callback_data):
    """Returns an inline keyboard with a single Go Back button pointing to
    the correct previous screen — fixes the skip/jump back button issue."""
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data=callback_data))
    return markup


# ─────────────────────────────────────────────────────────────────────────────
#  CALLBACK HANDLER
# ─────────────────────────────────────────────────────────────────────────────

@bot.callback_query_handler(func=lambda call: True)
def callback(call):
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "⛔ Only owner can use this bot.")
        return

    cid = call.message.chat.id
    mid = call.message.message_id
    data = call.data

    def edit(text, markup=None, parse_mode=None):
        try:
            bot.edit_message_text(text, cid, mid, reply_markup=markup, parse_mode=parse_mode)
        except Exception:
            pass

    def answer(text="", alert=False):
        try:
            bot.answer_callback_query(call.id, text, show_alert=alert)
        except Exception:
            pass

    # ── MAIN MENU ────────────────────────────────────────────────────────────
    if data == "back":
        show_main_menu(cid, "🏠 Main menu:", mid)
        answer()

    # ── BOT STATS ────────────────────────────────────────────────────────────
    elif data == "bot_stats":
        total = r.scard('bot_users')
        now = int(time.time())
        thirty_days_ago = now - (30 * 24 * 3600)
        new_users = sum(
            1 for uid in r.smembers('bot_users')
            if (fs := r.hget('user_first_seen', uid)) and int(fs) >= thirty_days_ago
        )
        groups_count = r.scard('groups')
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data="back"))
        edit(
            f"📊 *Bot Statistics*\n\n"
            f"👤 Total users: *{total}*\n"
            f"🆕 New users (last 30 days): *{new_users}*\n"
            f"👥 Active groups: *{groups_count}*",
            markup, parse_mode='Markdown'
        )
        answer()

    # ── GLOBAL /start REPLY ──────────────────────────────────────────────────
    elif data == "set_global_start_reply":
        edit("✏️ Send new custom reply for private /start.\nSend 'reset' to remove.")
        bot.register_next_step_handler(call.message, process_global_start_reply)
        answer()

    # ── GLOBAL /start@ GROUP REPLY ───────────────────────────────────────────
    elif data == "set_global_group_start_reply":
        current = r.get('global_group_start_reply') or "Not set"
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(
            types.InlineKeyboardButton("✏️ Set / Edit Reply", callback_data="do_set_global_group_start_reply"),
            types.InlineKeyboardButton("🗑 Remove Reply", callback_data="reset_global_group_start_reply"),
            types.InlineKeyboardButton("🔙 Go Back", callback_data="back"),
        )
        edit(f"📝 *Global /start@ Group Reply*\n\nCurrent:\n_{current}_", markup, parse_mode='Markdown')
        answer()

    elif data == "do_set_global_group_start_reply":
        edit("✏️ Send the new global reply for /start@AllMusicShazamandlyrics_bot in groups:")
        bot.register_next_step_handler(call.message, process_global_group_start_reply)
        answer()

    elif data == "reset_global_group_start_reply":
        r.delete('global_group_start_reply')
        answer("✅ Global group /start@ reply removed.", alert=True)
        # FIX #3: return to the correct previous screen, not main menu
        callback(types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, data="set_global_group_start_reply"))

    # ── BROADCAST TO ALL GROUPS ───────────────────────────────────────────────
    elif data == "broadcast_all":
        edit("📢 Send the message you want to broadcast to all groups:")
        bot.register_next_step_handler(call.message, process_broadcast_all)
        answer()

    # ── TOGGLE GLOBAL LINK-ONLY ───────────────────────────────────────────────
    elif data == "toggle_global":
        current = r.get('link_only_global') == 'True'
        r.set('link_only_global', 'False' if current else 'True')
        status = "OFF" if current else "ON"
        answer(f"🔗 Global link-only → {status}")
        show_main_menu(cid, f"🔗 Global link-only now {status}", mid)

    # ── MY GROUPS / SEND TO GROUP ─────────────────────────────────────────────
    elif data in ["my_groups", "my_groups_send"]:
        groups = get_groups()
        if not groups:
            markup = types.InlineKeyboardMarkup()
            markup.add(
                types.InlineKeyboardButton("🔄 Refresh", callback_data="refresh_groups"),
                types.InlineKeyboardButton("🔙 Go Back", callback_data="back"),
            )
            edit("❌ No groups added yet.\nAdd the bot to groups first!", markup)
            answer("No groups")
            return

        markup = types.InlineKeyboardMarkup(row_width=1)
        for g in groups:
            # FIX #4: reads from Redis cache — instant, no API calls per group
            title, status = get_group_info(g)
            btn_text = f"{title} ({status})"
            btn_data = f"group_menu:{g}" if data == "my_groups" else f"send_to_group:{g}"
            markup.add(types.InlineKeyboardButton(btn_text, callback_data=btn_data))
        markup.add(
            types.InlineKeyboardButton("🔄 Refresh List", callback_data="refresh_groups"),
            types.InlineKeyboardButton("🔙 Go Back", callback_data="back"),
        )
        text = "👥 Your Groups:" if data == "my_groups" else "📨 Select group to send:"
        edit(text, markup)
        answer()

    # ── REFRESH GROUPS ────────────────────────────────────────────────────────
    elif data == "refresh_groups":
        groups = list(get_groups())
        removed = 0
        for g in groups:
            try:
                bot.get_chat(g)
                # Force-refresh cache so fresh info is stored in Redis
                get_group_info(g, force_refresh=True)
            except telebot.apihelper.ApiTelegramException as e:
                if "chat not found" in str(e).lower() or "forbidden" in str(e).lower():
                    remove_group(g)
                    removed += 1
        answer(f"✅ Refreshed. Removed {removed} invalid groups.")
        callback(types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, data="my_groups"))

    # ── GROUP MENU ────────────────────────────────────────────────────────────
    elif data.startswith("group_menu:"):
        _, chat_id_str = data.split(":", 1)
        chat_id = int(chat_id_str)
        title, status = get_group_info(chat_id)
        is_admin = status == "Admin"
        link_only_this = is_link_only(chat_id)
        repeat_on = r.get(f'repeat_task:{chat_id}') == 'True'
        join_reply_on = r.get(f'join_reply_enabled:{chat_id}') == 'True'

        text = (
            f"👥 *Group:* {title}\n"
            f"🤖 *Bot Status:* {status}\n"
            f"🔗 *Link-only:* {'✅ ON' if link_only_this else '❌ OFF'}\n"
            f"🔁 *Repeating:* {'✅ ON' if repeat_on else '❌ OFF'}\n"
            f"👋 *Join Reply:* {'✅ ON' if join_reply_on else '❌ OFF'}\n\n"
            f"Select an action:"
        )

        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(types.InlineKeyboardButton("📨 Send Message", callback_data=f"send_to_group:{chat_id}"))
        markup.add(types.InlineKeyboardButton("🔁 Timer / Repeat / Auto-Delete", callback_data=f"setup_repeat:{chat_id}"))
        markup.add(types.InlineKeyboardButton("👋 Join Reply", callback_data=f"group_join_reply:{chat_id}"))
        markup.add(types.InlineKeyboardButton("🗑 Delete ALL My Sent Msgs", callback_data=f"purge:{chat_id}"))
        markup.add(types.InlineKeyboardButton("🗑 Delete Last Bot Message", callback_data=f"delete_last:{chat_id}"))
        last_id = r.get(f'last_sent:{chat_id}')
        if last_id and is_admin:
            markup.add(types.InlineKeyboardButton("📌 Pin Last Message", callback_data=f"pin_last:{chat_id}"))
        markup.add(types.InlineKeyboardButton("💬 Set /start@ Reply", callback_data=f"set_group_start_reply:{chat_id}"))
        markup.add(types.InlineKeyboardButton("➕ Add Account to Group", callback_data=f"add_to_group:{chat_id}:choose"))
        markup.add(types.InlineKeyboardButton(
            f"{'🔴 Disable' if link_only_this else '🟢 Enable'} Link-Only",
            callback_data=f"toggle_group:{chat_id}"
        ))
        markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data="my_groups"))

        edit(text, markup, parse_mode='Markdown')
        answer()

    # ── SETUP REPEAT (per group) ──────────────────────────────────────────────
    elif data.startswith("setup_repeat:"):
        _, chat_id_str = data.split(":", 1)
        chat_id = int(chat_id_str)
        repeat_on = r.get(f'repeat_task:{chat_id}') == 'True'
        interval = r.get(f'repeat_interval:{chat_id}') or "3600"
        autodelete = r.get(f'repeat_autodelete:{chat_id}') == 'True'
        self_del = r.get(f'repeat_self_delete:{chat_id}')
        current_text = r.get(f'repeat_text:{chat_id}') or "Not set"

        text = (
            f"⚙️ *Repeat Setup*\n\n"
            f"Status: {'✅ ON' if repeat_on else '❌ OFF'}\n"
            f"Interval: {interval}s\n"
            f"Auto-delete previous: {'✅' if autodelete else '❌'}\n"
            f"Self-delete after: {self_del + 's' if self_del else '❌ OFF'}\n"
            f"Message: _{current_text[:80]}_"
        )

        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.row(
            types.InlineKeyboardButton("✅ ON", callback_data=f"repeat_on:{chat_id}"),
            types.InlineKeyboardButton("❌ OFF", callback_data=f"repeat_off:{chat_id}")
        )
        markup.add(types.InlineKeyboardButton("✏️ Set / Edit Message", callback_data=f"set_repeat_text:{chat_id}"))
        markup.row(
            types.InlineKeyboardButton("⏱ Interval (sec)", callback_data=f"set_interval_sec:{chat_id}"),
            types.InlineKeyboardButton("⏱ Interval (min)", callback_data=f"set_interval_min:{chat_id}")
        )
        markup.add(types.InlineKeyboardButton(
            f"🗑 Auto-del prev: {'✅ ON' if autodelete else '❌ OFF'}",
            callback_data=f"toggle_autodelete:{chat_id}"
        ))
        markup.add(types.InlineKeyboardButton(
            f"💣 Self-delete: {'✅ ' + self_del + 's' if self_del else '❌ OFF'}",
            callback_data=f"set_self_delete:{chat_id}"
        ))
        if self_del:
            markup.add(types.InlineKeyboardButton("❌ Remove Self-Delete", callback_data=f"remove_self_delete:{chat_id}"))
        markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data=f"group_menu:{chat_id}"))

        edit(text, markup, parse_mode='Markdown')
        answer()

    elif data.startswith("set_repeat_text:"):
        _, chat_id_str = data.split(":", 1)
        chat_id = int(chat_id_str)
        edit("✏️ Send the message you want to repeat:")
        bot.register_next_step_handler(call.message, lambda m: process_set_repeat_text(m, chat_id))
        answer()

    elif data.startswith("set_self_delete:"):
        _, chat_id_str = data.split(":", 1)
        chat_id = int(chat_id_str)
        edit("💣 Send self-delete delay in seconds (e.g. 30):")
        bot.register_next_step_handler(call.message, lambda m: process_self_delete(m, chat_id))
        answer()

    elif data.startswith("remove_self_delete:"):
        _, chat_id_str = data.split(":", 1)
        chat_id = int(chat_id_str)
        r.delete(f'repeat_self_delete:{chat_id}')
        answer("✅ Self-delete removed.")
        callback(types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, data=f"setup_repeat:{chat_id}"))

    elif data.startswith("repeat_on:"):
        _, chat_id_str = data.split(":", 1)
        chat_id = int(chat_id_str)
        if not r.get(f'repeat_text:{chat_id}'):
            answer("⚠️ Set a repeat message first!", alert=True)
            return
        r.set(f'repeat_task:{chat_id}', 'True')
        start_repeat_thread(chat_id)
        answer("✅ Repeating ON")
        callback(types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, data=f"setup_repeat:{chat_id}"))

    elif data.startswith("repeat_off:"):
        _, chat_id_str = data.split(":", 1)
        chat_id = int(chat_id_str)
        r.set(f'repeat_task:{chat_id}', 'False')
        answer("✅ Repeating OFF")
        callback(types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, data=f"setup_repeat:{chat_id}"))

    elif data.startswith("set_interval_sec:") or data.startswith("set_interval_min:"):
        _, chat_id_str = data.split(":", 1)
        chat_id = int(chat_id_str)
        unit = "sec" if "sec" in data else "min"
        edit(f"⏱ Send interval in {unit} (number only):")
        bot.register_next_step_handler(call.message, lambda m: process_interval(m, chat_id, unit))
        answer()

    elif data.startswith("toggle_autodelete:"):
        _, chat_id_str = data.split(":", 1)
        chat_id = int(chat_id_str)
        current = r.get(f'repeat_autodelete:{chat_id}') == 'True'
        r.set(f'repeat_autodelete:{chat_id}', 'False' if current else 'True')
        answer(f"🗑 Auto-delete prev now {'❌ OFF' if current else '✅ ON'}")
        callback(types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, data=f"setup_repeat:{chat_id}"))

    # ── DELETE ALL MY SENT MESSAGES IN GROUP ──────────────────────────────────
    elif data.startswith("purge:"):
        _, chat_id_str = data.split(":", 1)
        chat_id = int(chat_id_str)
        msg_ids = get_sent_messages(chat_id)
        if not msg_ids:
            answer("❌ No tracked messages to delete.", alert=True)
            return
        deleted = 0
        failed = 0
        for m_id in msg_ids:
            try:
                bot.delete_message(chat_id, m_id)
                deleted += 1
            except Exception:
                failed += 1
        clear_sent_messages(chat_id)
        answer(f"✅ Deleted {deleted} msgs. {failed} failed (too old or already gone).", alert=True)
        callback(types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, data=f"group_menu:{chat_id}"))

    # ── DELETE LAST BOT MESSAGE IN GROUP ──────────────────────────────────────
    elif data.startswith("delete_last:"):
        _, chat_id_str = data.split(":", 1)
        chat_id = int(chat_id_str)
        last_id = r.get(f'last_sent:{chat_id}')
        if not last_id:
            answer("❌ No last message tracked.", alert=True)
            return
        try:
            bot.delete_message(chat_id, int(last_id))
            r.delete(f'last_sent:{chat_id}')
            answer("✅ Last message deleted!", alert=True)
        except Exception as e:
            answer(f"❌ Failed: {str(e)}", alert=True)
        callback(types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, data=f"group_menu:{chat_id}"))

    # ── DELETE ALL PRIVATE SENT MESSAGES ─────────────────────────────────────
    elif data == "delete_all_private":
        users = get_all_users()
        deleted = 0
        failed = 0
        for uid in users:
            for m_id in get_private_sent(uid):
                try:
                    bot.delete_message(uid, m_id)
                    deleted += 1
                except Exception:
                    failed += 1
            clear_private_sent(uid)
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data="back"))
        edit(f"🗑 Done!\n✅ Deleted: {deleted}\n❌ Failed: {failed}", markup)
        answer()

    # ── SEND TO GROUP ─────────────────────────────────────────────────────────
    elif data.startswith("send_to_group:"):
        _, chat_id_str = data.split(":", 1)
        chat_id = int(chat_id_str)
        edit("📨 Send your message now:")
        bot.register_next_step_handler(call.message, lambda m: process_single_message(m, chat_id))
        answer()

    # ── PIN LAST ──────────────────────────────────────────────────────────────
    elif data.startswith("pin_last:"):
        _, chat_id_str = data.split(":", 1)
        chat_id = int(chat_id_str)
        msg_id = r.get(f'last_sent:{chat_id}')
        if not msg_id:
            answer("❌ No last message tracked.", alert=True)
            return
        if not bot_can_pin(chat_id):
            answer("❌ Bot does not have permission to pin in this group.", alert=True)
            return
        try:
            bot.pin_chat_message(chat_id, int(msg_id))
            answer("✅ Message pinned!", alert=True)
        except Exception as e:
            answer(f"❌ Pin failed: {str(e)}", alert=True)

    # ── TOGGLE GROUP LINK-ONLY ────────────────────────────────────────────────
    elif data.startswith("toggle_group:"):
        _, chat_id_str = data.split(":", 1)
        chat_id = int(chat_id_str)
        current = is_link_only(chat_id)
        set_link_only(chat_id, not current)
        answer(f"🔗 Link-only now {'❌ OFF' if current else '✅ ON'}")
        callback(types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, data=f"group_menu:{chat_id}"))

    # ── GROUP /start@ REPLY ───────────────────────────────────────────────────
    elif data.startswith("set_group_start_reply:"):
        _, chat_id_str = data.split(":", 1)
        chat_id = int(chat_id_str)
        current = r.get(f'group_start_reply:{chat_id}') or "Not set"
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(
            types.InlineKeyboardButton("✏️ Set / Edit Reply", callback_data=f"do_set_group_start_reply:{chat_id}"),
            types.InlineKeyboardButton("🗑 Remove Reply", callback_data=f"reset_group_start_reply:{chat_id}"),
            types.InlineKeyboardButton("🔙 Go Back", callback_data=f"group_menu:{chat_id}"),
        )
        edit(f"💬 */start@ Reply for this group*\n\nCurrent:\n_{current}_", markup, parse_mode='Markdown')
        answer()

    elif data.startswith("do_set_group_start_reply:"):
        _, chat_id_str = data.split(":", 1)
        chat_id = int(chat_id_str)
        edit("✏️ Send the new /start@ reply for this group:")
        bot.register_next_step_handler(call.message, lambda m: process_group_start_reply(m, chat_id))
        answer()

    elif data.startswith("reset_group_start_reply:"):
        _, chat_id_str = data.split(":", 1)
        chat_id = int(chat_id_str)
        r.delete(f'group_start_reply:{chat_id}')
        answer("✅ Group /start@ reply removed.", alert=True)
        # FIX #3: return to the group start reply menu, not main menu
        callback(types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, data=f"set_group_start_reply:{chat_id}"))

    # ── GLOBAL JOIN REPLY ─────────────────────────────────────────────────────
    elif data == "global_join_reply_menu":
        enabled = r.get('global_join_reply_enabled') == 'True'
        current_text = r.get('global_join_reply_text') or "Welcome!"
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.row(
            types.InlineKeyboardButton("✅ ON", callback_data="global_join_reply_on"),
            types.InlineKeyboardButton("❌ OFF", callback_data="global_join_reply_off")
        )
        markup.add(types.InlineKeyboardButton("✏️ Set / Edit Reply", callback_data="set_global_join_reply"))
        markup.add(types.InlineKeyboardButton("🗑 Reset to Default", callback_data="reset_global_join_reply"))
        markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data="back"))
        edit(
            f"👋 *Global Join Reply*\n\nStatus: {'✅ ON' if enabled else '❌ OFF'}\nMessage:\n_{current_text}_",
            markup, parse_mode='Markdown'
        )
        answer()

    elif data == "global_join_reply_on":
        r.set('global_join_reply_enabled', 'True')
        answer("✅ Global join reply ON")
        callback(types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, data="global_join_reply_menu"))

    elif data == "global_join_reply_off":
        r.set('global_join_reply_enabled', 'False')
        answer("✅ Global join reply OFF")
        callback(types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, data="global_join_reply_menu"))

    elif data == "set_global_join_reply":
        edit("✏️ Send the new global join reply message:")
        bot.register_next_step_handler(call.message, process_global_join_reply)
        answer()

    elif data == "reset_global_join_reply":
        r.set('global_join_reply_text', 'Welcome!')
        answer("✅ Reset to default: 'Welcome!'", alert=True)
        callback(types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, data="global_join_reply_menu"))

    # ── GROUP JOIN REPLY ──────────────────────────────────────────────────────
    elif data.startswith("group_join_reply:"):
        _, chat_id_str = data.split(":", 1)
        chat_id = int(chat_id_str)
        enabled = r.get(f'join_reply_enabled:{chat_id}') == 'True'
        current_text = r.get(f'join_reply_text:{chat_id}') or "Not set (uses global)"
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.row(
            types.InlineKeyboardButton("✅ ON", callback_data=f"group_join_on:{chat_id}"),
            types.InlineKeyboardButton("❌ OFF", callback_data=f"group_join_off:{chat_id}")
        )
        markup.add(types.InlineKeyboardButton("✏️ Set / Edit Reply", callback_data=f"set_group_join_reply:{chat_id}"))
        markup.add(types.InlineKeyboardButton("🗑 Reset Reply", callback_data=f"reset_group_join_reply:{chat_id}"))
        markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data=f"group_menu:{chat_id}"))
        edit(
            f"👋 *Group Join Reply*\n\nStatus: {'✅ ON' if enabled else '❌ OFF'}\nMessage:\n_{current_text}_",
            markup, parse_mode='Markdown'
        )
        answer()

    elif data.startswith("group_join_on:"):
        _, chat_id_str = data.split(":", 1)
        chat_id = int(chat_id_str)
        r.set(f'join_reply_enabled:{chat_id}', 'True')
        answer("✅ Group join reply ON")
        callback(types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, data=f"group_join_reply:{chat_id}"))

    elif data.startswith("group_join_off:"):
        _, chat_id_str = data.split(":", 1)
        chat_id = int(chat_id_str)
        r.set(f'join_reply_enabled:{chat_id}', 'False')
        answer("✅ Group join reply OFF")
        callback(types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, data=f"group_join_reply:{chat_id}"))

    elif data.startswith("set_group_join_reply:"):
        _, chat_id_str = data.split(":", 1)
        chat_id = int(chat_id_str)
        edit("✏️ Send the join reply message for this group:")
        bot.register_next_step_handler(call.message, lambda m: process_group_join_reply(m, chat_id))
        answer()

    elif data.startswith("reset_group_join_reply:"):
        _, chat_id_str = data.split(":", 1)
        chat_id = int(chat_id_str)
        r.delete(f'join_reply_text:{chat_id}')
        answer("✅ Group join reply text reset (will use global).", alert=True)
        callback(types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, data=f"group_join_reply:{chat_id}"))

    # ── ADD ACCOUNT TO GROUP (main menu) ──────────────────────────────────────
    elif data == "add_account_menu":
        groups = get_groups()
        eligible = []
        for g in groups:
            can_add, can_promote = bot_can_add_members(g)
            if can_add or can_promote:
                eligible.append((g, can_add, can_promote))

        if not eligible:
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data="back"))
            edit(
                "❌ No groups where bot has the necessary permissions.\n\n"
                "ℹ️ Bot must be admin with 'Add Members' or 'Promote Members' rights.",
                markup
            )
            answer()
            return

        markup = types.InlineKeyboardMarkup(row_width=1)
        for g, can_add, can_promote in eligible:
            title, _ = get_group_info(g)
            perms = []
            if can_promote:
                perms.append("Can Promote")
            if can_add:
                perms.append("Can Invite")
            markup.add(types.InlineKeyboardButton(
                f"{title} ({', '.join(perms)})",
                callback_data=f"add_to_group:{g}:choose"
            ))
        markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data="back"))
        edit("➕ *Select a group:*", markup, parse_mode='Markdown')
        answer()

    # ── ADD TO GROUP: CHOOSE ROLE ─────────────────────────────────────────────
    elif data.startswith("add_to_group:") and data.endswith(":choose"):
        parts = data.split(":")
        chat_id = int(parts[1])
        can_add, can_promote = bot_can_add_members(chat_id)
        title, _ = get_group_info(chat_id)
        markup = types.InlineKeyboardMarkup(row_width=1)
        if can_promote:
            markup.add(types.InlineKeyboardButton(
                "👑 Promote to Admin (user must already be in group)",
                callback_data=f"add_to_group:{chat_id}:admin"
            ))
        if can_add:
            markup.add(types.InlineKeyboardButton(
                "🔗 Generate Invite Link (to add as member)",
                callback_data=f"add_to_group:{chat_id}:invite"
            ))
        markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data="add_account_menu"))
        edit(
            f"➕ *Add to:* {title}\n\n"
            f"⚠️ *How it works:*\n"
            f"• *Promote to Admin* — user must already be in the group.\n"
            f"• *Invite Link* — generates a link anyone can use to join.\n\n"
            f"Choose an option:",
            markup, parse_mode='Markdown'
        )
        answer()

    # ── ADD TO GROUP: PROMOTE TO ADMIN ────────────────────────────────────────
    # FIX #2: Telegram cannot add arbitrary users to a group by user ID.
    # promote_chat_member only works on users already in the group.
    # This is now clearly stated and the result is always honestly reported.
    elif data.startswith("add_to_group:") and data.endswith(":admin"):
        parts = data.split(":")
        chat_id = int(parts[1])
        title, _ = get_group_info(chat_id)
        edit(
            f"👑 *Promote to Admin in:* {title}\n\n"
            f"Send the user ID(s) of people *already in the group*.\n"
            f"You can send multiple IDs separated by spaces or commas.\n\n"
            f"⚠️ Only works if the user is already a member of this group.",
            markup=None
        )
        bot.register_next_step_handler(call.message, lambda m: process_promote_to_admin(m, chat_id))
        answer()

    # ── ADD TO GROUP: GENERATE INVITE LINK ────────────────────────────────────
    elif data.startswith("add_to_group:") and data.endswith(":invite"):
        parts = data.split(":")
        chat_id = int(parts[1])
        title, _ = get_group_info(chat_id)
        try:
            link = bot.create_chat_invite_link(chat_id)
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data=f"add_to_group:{chat_id}:choose"))
            edit(
                f"🔗 *Invite link for:* {title}\n\n"
                f"`{link.invite_link}`\n\n"
                f"Share this link with anyone you want to add to the group.",
                markup, parse_mode='Markdown'
            )
        except Exception as e:
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data=f"add_to_group:{chat_id}:choose"))
            edit(f"❌ Could not generate invite link:\n{str(e)}", markup)
        answer()

    # ── GLOBAL REPEAT MENU ────────────────────────────────────────────────────
    elif data == "global_repeat_menu":
        repeat_on = r.get('global_repeat_task') == 'True'
        interval = r.get('global_repeat_interval') or "3600"
        autodelete = r.get('global_repeat_autodelete') == 'True'
        self_del = r.get('global_repeat_self_delete')
        current_text = r.get('global_repeat_text') or "Not set"

        text = (
            f"🔁 *Global Broadcast Repeat*\n\n"
            f"Status: {'✅ ON' if repeat_on else '❌ OFF'}\n"
            f"Interval: {interval}s\n"
            f"Auto-delete previous: {'✅' if autodelete else '❌'}\n"
            f"Self-delete after: {self_del + 's' if self_del else '❌ OFF'}\n"
            f"Message: _{current_text[:80]}_"
        )

        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(types.InlineKeyboardButton("⏸ Pause All Group Repeats", callback_data="pause_all_group_repeats"))
        markup.row(
            types.InlineKeyboardButton("✅ ON", callback_data="global_repeat_on"),
            types.InlineKeyboardButton("❌ OFF", callback_data="global_repeat_off")
        )
        markup.add(types.InlineKeyboardButton("✏️ Set / Edit Message", callback_data="set_global_repeat_text"))
        markup.row(
            types.InlineKeyboardButton("⏱ Interval (sec)", callback_data="set_global_interval_sec"),
            types.InlineKeyboardButton("⏱ Interval (min)", callback_data="set_global_interval_min")
        )
        markup.add(types.InlineKeyboardButton(
            f"🗑 Auto-del prev: {'✅ ON' if autodelete else '❌ OFF'}",
            callback_data="toggle_global_autodelete"
        ))
        markup.add(types.InlineKeyboardButton(
            f"💣 Self-delete: {'✅ ' + self_del + 's' if self_del else '❌ OFF'}",
            callback_data="set_global_self_delete"
        ))
        if self_del:
            markup.add(types.InlineKeyboardButton("❌ Remove Self-Delete", callback_data="remove_global_self_delete"))
        markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data="back"))

        edit(text, markup, parse_mode='Markdown')
        answer()

    elif data == "pause_all_group_repeats":
        for g in get_groups():
            r.set(f'repeat_task:{g}', 'False')
        answer("⏸ All individual group repeats paused.", alert=True)
        callback(types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, data="global_repeat_menu"))

    elif data == "global_repeat_on":
        if not r.get('global_repeat_text'):
            answer("⚠️ Set a repeat message first!", alert=True)
            return
        r.set('global_repeat_task', 'True')
        start_global_repeat_thread()
        answer("✅ Global repeat ON")
        callback(types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, data="global_repeat_menu"))

    elif data == "global_repeat_off":
        r.set('global_repeat_task', 'False')
        answer("✅ Global repeat OFF")
        callback(types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, data="global_repeat_menu"))

    elif data == "set_global_repeat_text":
        edit("✏️ Send the message for global repeat broadcast:")
        bot.register_next_step_handler(call.message, process_global_repeat_text)
        answer()

    elif data in ["set_global_interval_sec", "set_global_interval_min"]:
        unit = "sec" if "sec" in data else "min"
        edit(f"⏱ Send global repeat interval in {unit} (number only):")
        bot.register_next_step_handler(call.message, lambda m: process_global_interval(m, unit))
        answer()

    elif data == "toggle_global_autodelete":
        current = r.get('global_repeat_autodelete') == 'True'
        r.set('global_repeat_autodelete', 'False' if current else 'True')
        answer(f"🗑 Global auto-delete prev now {'❌ OFF' if current else '✅ ON'}")
        callback(types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, data="global_repeat_menu"))

    elif data == "set_global_self_delete":
        edit("💣 Send self-delete delay in seconds for global repeat (e.g. 30):")
        bot.register_next_step_handler(call.message, process_global_self_delete)
        answer()

    elif data == "remove_global_self_delete":
        r.delete('global_repeat_self_delete')
        answer("✅ Global self-delete removed.")
        callback(types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, data="global_repeat_menu"))

    # ── BROADCAST TO BOT USERS ────────────────────────────────────────────────
    elif data == "broadcast_users":
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(
            types.InlineKeyboardButton("📣 Send Broadcast", callback_data="do_broadcast_users"),
            types.InlineKeyboardButton("📌 Send & Pin", callback_data="do_broadcast_users_pin"),
            types.InlineKeyboardButton("🔙 Go Back", callback_data="back"),
        )
        edit("📣 *Broadcast to Bot Users*\n\nSend a message to everyone who has ever started the bot.", markup, parse_mode='Markdown')
        answer()

    elif data in ["do_broadcast_users", "do_broadcast_users_pin"]:
        pin = data == "do_broadcast_users_pin"
        edit(f"✏️ Send the message to broadcast to all bot users{' (will also be pinned)' if pin else ''}:")
        bot.register_next_step_handler(call.message, lambda m: process_broadcast_users(m, pin))
        answer()


# ─────────────────────────────────────────────────────────────────────────────
#  PROCESS FUNCTIONS
# All process functions now always include a Go Back button pointing to the
# correct previous screen — fixes FIX #3 (no back button after text input).
# ─────────────────────────────────────────────────────────────────────────────

def process_global_start_reply(message):
    if message.from_user.id != OWNER_ID:
        return
    text = message.text.strip()
    if text.lower() == 'reset':
        r.delete('global_start_reply')
        bot.send_message(message.chat.id, "✅ Global /start reply removed.",
                         reply_markup=_back_markup("back"))
    else:
        r.set('global_start_reply', text)
        bot.send_message(message.chat.id, "✅ Global /start reply set.",
                         reply_markup=_back_markup("back"))

def process_global_group_start_reply(message):
    if message.from_user.id != OWNER_ID:
        return
    r.set('global_group_start_reply', message.text.strip())
    bot.send_message(message.chat.id, "✅ Global group /start@ reply set.",
                     reply_markup=_back_markup("set_global_group_start_reply"))

def process_group_start_reply(message, chat_id):
    if message.from_user.id != OWNER_ID:
        return
    text = message.text.strip()
    key = f'group_start_reply:{chat_id}'
    if text.lower() == 'reset':
        r.delete(key)
        bot.send_message(message.chat.id, "✅ Group /start@ reply removed.",
                         reply_markup=_back_markup(f"set_group_start_reply:{chat_id}"))
    else:
        r.set(key, text)
        bot.send_message(message.chat.id, "✅ Group /start@ reply set.",
                         reply_markup=_back_markup(f"set_group_start_reply:{chat_id}"))

def process_global_join_reply(message):
    if message.from_user.id != OWNER_ID:
        return
    r.set('global_join_reply_text', message.text.strip())
    bot.send_message(message.chat.id, "✅ Global join reply message set.",
                     reply_markup=_back_markup("global_join_reply_menu"))

def process_group_join_reply(message, chat_id):
    if message.from_user.id != OWNER_ID:
        return
    r.set(f'join_reply_text:{chat_id}', message.text.strip())
    bot.send_message(message.chat.id, "✅ Join reply for this group set.",
                     reply_markup=_back_markup(f"group_join_reply:{chat_id}"))

def process_interval(message, chat_id, unit):
    if message.from_user.id != OWNER_ID:
        return
    try:
        val = int(message.text.strip())
        if val <= 0:
            raise ValueError
        seconds = val if unit == "sec" else val * 60
        r.set(f'repeat_interval:{chat_id}', str(seconds))
        bot.send_message(message.chat.id, f"✅ Interval set to {seconds} seconds.",
                         reply_markup=_back_markup(f"setup_repeat:{chat_id}"))
    except Exception:
        bot.send_message(message.chat.id, "❌ Please send a positive number.",
                         reply_markup=_back_markup(f"setup_repeat:{chat_id}"))

def process_set_repeat_text(message, chat_id):
    if message.from_user.id != OWNER_ID:
        return
    r.set(f'repeat_text:{chat_id}', message.text)
    bot.send_message(message.chat.id, "✅ Repeat message set.",
                     reply_markup=_back_markup(f"setup_repeat:{chat_id}"))

def process_self_delete(message, chat_id):
    if message.from_user.id != OWNER_ID:
        return
    try:
        val = int(message.text.strip())
        if val <= 0:
            raise ValueError
        r.set(f'repeat_self_delete:{chat_id}', str(val))
        bot.send_message(message.chat.id, f"✅ Self-delete set to {val} seconds.",
                         reply_markup=_back_markup(f"setup_repeat:{chat_id}"))
    except Exception:
        bot.send_message(message.chat.id, "❌ Please send a positive number.",
                         reply_markup=_back_markup(f"setup_repeat:{chat_id}"))

def process_global_repeat_text(message):
    if message.from_user.id != OWNER_ID:
        return
    r.set('global_repeat_text', message.text)
    bot.send_message(message.chat.id, "✅ Global repeat message set.",
                     reply_markup=_back_markup("global_repeat_menu"))

def process_global_interval(message, unit):
    if message.from_user.id != OWNER_ID:
        return
    try:
        val = int(message.text.strip())
        if val <= 0:
            raise ValueError
        seconds = val if unit == "sec" else val * 60
        r.set('global_repeat_interval', str(seconds))
        bot.send_message(message.chat.id, f"✅ Global repeat interval set to {seconds} seconds.",
                         reply_markup=_back_markup("global_repeat_menu"))
    except Exception:
        bot.send_message(message.chat.id, "❌ Please send a positive number.",
                         reply_markup=_back_markup("global_repeat_menu"))

def process_global_self_delete(message):
    if message.from_user.id != OWNER_ID:
        return
    try:
        val = int(message.text.strip())
        if val <= 0:
            raise ValueError
        r.set('global_repeat_self_delete', str(val))
        bot.send_message(message.chat.id, f"✅ Global self-delete set to {val} seconds.",
                         reply_markup=_back_markup("global_repeat_menu"))
    except Exception:
        bot.send_message(message.chat.id, "❌ Please send a positive number.",
                         reply_markup=_back_markup("global_repeat_menu"))

def process_broadcast_all(message):
    if message.from_user.id != OWNER_ID:
        return
    text = message.text
    groups = get_groups()
    if not groups:
        bot.send_message(message.chat.id, "❌ No groups.", reply_markup=_back_markup("back"))
        return
    sent_count = 0
    failed_count = 0
    for group in groups:
        try:
            sent = bot.send_message(group, text)
            r.set(f'last_sent:{group}', sent.message_id)
            save_last_sent(group, sent.message_id)
            sent_count += 1
        except Exception:
            failed_count += 1
    bot.send_message(
        message.chat.id,
        f"✅ Broadcast sent!\n👥 Sent to: {sent_count} groups\n❌ Failed: {failed_count}",
        reply_markup=_back_markup("back")
    )

def process_single_message(message, group_id):
    if message.from_user.id != OWNER_ID:
        return
    text = message.text
    try:
        sent = bot.send_message(group_id, text)
        r.set(f'last_sent:{group_id}', sent.message_id)
        save_last_sent(group_id, sent.message_id)
        markup = types.InlineKeyboardMarkup(row_width=1)
        if bot_can_pin(group_id):
            markup.add(types.InlineKeyboardButton("📌 Pin Message", callback_data=f"pin_last:{group_id}"))
        markup.add(types.InlineKeyboardButton("🔙 Group Menu", callback_data=f"group_menu:{group_id}"))
        bot.send_message(message.chat.id, "✅ Message sent!", reply_markup=markup)
        if r.get(f'repeat_task:{group_id}') == 'True' and not r.get(f'repeat_text:{group_id}'):
            r.set(f'repeat_text:{group_id}', text)
            bot.send_message(message.chat.id, "ℹ️ This message is now the repeating message (no prior set).",
                             reply_markup=_back_markup(f"group_menu:{group_id}"))
    except Exception as e:
        bot.send_message(message.chat.id, f"❌ Error: {str(e)}",
                         reply_markup=_back_markup(f"group_menu:{group_id}"))

# FIX #2: Honest promote-to-admin with real Telegram API behaviour.
# promote_chat_member ONLY works on users already in the group.
# Every result is honestly reported — success or exact failure reason.
def process_promote_to_admin(message, chat_id):
    if message.from_user.id != OWNER_ID:
        return
    raw = message.text.strip()
    id_strings = [x.strip() for x in re.split(r'[\s,]+', raw) if x.strip()]
    results = []
    for id_str in id_strings:
        try:
            user_id = int(id_str)
        except ValueError:
            results.append(f"❌ `{id_str}` — not a valid numeric ID")
            continue
        try:
            bot.promote_chat_member(
                chat_id, user_id,
                can_manage_chat=True,
                can_change_info=True,
                can_delete_messages=True,
                can_restrict_members=True,
                can_invite_users=True,
                can_pin_messages=True
            )
            results.append(f"✅ `{user_id}` — promoted to Admin successfully")
        except telebot.apihelper.ApiTelegramException as e:
            err = str(e)
            if "USER_NOT_PARTICIPANT" in err:
                results.append(
                    f"❌ `{user_id}` — Not in the group yet.\n"
                    f"   ℹ️ Use the Invite Link option to add them first."
                )
            elif "CHAT_ADMIN_REQUIRED" in err:
                results.append(f"❌ `{user_id}` — Bot lacks admin rights in this group.")
            elif "USER_PRIVACY_RESTRICTED" in err:
                results.append(f"❌ `{user_id}` — Blocked by user's privacy settings.")
            elif "PEER_ID_INVALID" in err or "user not found" in err.lower():
                results.append(f"❌ `{user_id}` — User ID not found or invalid.")
            elif "can't demote chat creator" in err.lower():
                results.append(f"❌ `{user_id}` — Cannot modify the group creator.")
            else:
                results.append(f"❌ `{user_id}` — {err}")
        except Exception as e:
            results.append(f"❌ `{user_id}` — Unexpected error: {str(e)}")

    title, _ = get_group_info(chat_id)
    summary = f"👑 *Promotion Results for {title}:*\n\n" + "\n".join(results)
    try:
        bot.send_message(message.chat.id, summary, parse_mode='Markdown',
                         reply_markup=_back_markup(f"add_to_group:{chat_id}:choose"))
    except Exception:
        bot.send_message(message.chat.id, summary.replace('`', '').replace('*', ''),
                         reply_markup=_back_markup(f"add_to_group:{chat_id}:choose"))

def process_broadcast_users(message, pin=False):
    if message.from_user.id != OWNER_ID:
        return
    text = message.text
    users = get_all_users()
    sent_count = 0
    failed_count = 0
    pin_count = 0
    pin_failed = 0

    for uid in users:
        try:
            sent = bot.send_message(uid, text)
            save_private_sent(uid, sent.message_id)
            sent_count += 1
            if pin:
                try:
                    bot.pin_chat_message(uid, sent.message_id)
                    pin_count += 1
                except Exception:
                    pin_failed += 1
        except Exception:
            failed_count += 1

    result = (
        f"📣 Broadcast to users done!\n"
        f"✅ Sent: {sent_count}\n"
        f"❌ Failed: {failed_count}"
    )
    if pin:
        result += f"\n📌 Pinned: {pin_count}\n❌ Pin failed: {pin_failed}"

    bot.send_message(message.chat.id, result, reply_markup=_back_markup("back"))


# ─────────────────────────────────────────────────────────────────────────────
#  GROUP MESSAGE FILTER (link-only)
# ─────────────────────────────────────────────────────────────────────────────

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
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
#  FLASK WEBHOOK
# ─────────────────────────────────────────────────────────────────────────────

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
            print(f"Update error: {str(e)}")
        return ''
    abort(403)


# ─────────────────────────────────────────────────────────────────────────────
#  STARTUP
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    bot.remove_webhook()
    time.sleep(1)
    bot.set_webhook(WEBHOOK_URL)

    # Restart any active per-group repeat tasks (preserves existing 19+ groups)
    for g in get_groups():
        start_repeat_thread(g)

    # Restart global repeat if it was running before redeploy
    start_global_repeat_thread()

    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
