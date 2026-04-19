import os
import io
import json
import re
import time
import heapq
import logging
import psutil
import datetime
from collections import deque
from flask import Flask, request, abort
import telebot
from telebot import types
import redis
import threading

# ─── Structured Logging ──────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ─── Monitoring Counters ──────────────────────────────────────────────────────
_flood_wait_counter = 0
_flood_wait_lock = threading.Lock()

# Per-group runtime error log: {chat_id: [error_str, ...]}
_runtime_errors = {}
_runtime_errors_lock = threading.Lock()

def _increment_flood_counter(seconds=0):
    global _flood_wait_counter
    with _flood_wait_lock:
        _flood_wait_counter += 1
    logger.warning(f"FloodWait hit — retry_after={seconds}s (total hits: {_flood_wait_counter})")

def _log_runtime_error(chat_id, context, error_str):
    """Store a runtime error per group and log it."""
    msg = f"[{context}] {error_str}"
    with _runtime_errors_lock:
        if chat_id not in _runtime_errors:
            _runtime_errors[chat_id] = []
        _runtime_errors[chat_id].append(msg)
        # Keep last 20 errors per group
        if len(_runtime_errors[chat_id]) > 20:
            _runtime_errors[chat_id] = _runtime_errors[chat_id][-20:]
    logger.error(f"Group {chat_id} {msg}")

# Environment variables
TOKEN = os.environ['TOKEN']
OWNER_ID = int(os.environ['OWNER_ID'])
WEBHOOK_URL = os.environ['WEBHOOK_URL']
REDIS_URL = os.environ['REDIS_URL']

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)
r   = redis.Redis.from_url(REDIS_URL, decode_responses=True)
_BOT_ID = bot.get_me().id   # cached once at startup — used by bot detection system

# ─── Defaults ────────────────────────────────────────────────────────────────
if r.get('link_only_global') is None:
    r.set('link_only_global', 'False')
if r.get('global_start_reply') is None:
    r.set('global_start_reply', 'Hello! This bot is managed by its owner. Use groups for features.')
if r.get('global_join_reply_enabled') is None:
    r.set('global_join_reply_enabled', 'False')
if r.get('global_join_reply_text') is None:
    r.set('global_join_reply_text', 'Welcome to the group!')
if r.get('global_group_start_reply_enabled') is None:
    r.set('global_group_start_reply_enabled', 'False')
if r.get('added_to_group_msg_enabled') is None:
    r.set('added_to_group_msg_enabled', 'True')
if r.get('added_to_group_msg') is None:
    r.set('added_to_group_msg', 'https://t.me/AllMusicShazamandlyrics_bot?startgroup=true&admin=change_info+delete_messages+restrict_members+invite_users+pin_messages+manage_video_chats+anonymous+manage_chat+post_stories+edit_stories+delete_stories')

# ─── Group helpers ────────────────────────────────────────────────────────────
def get_groups():
    return [int(g) for g in r.smembers('groups')]

def add_group(chat_id):
    r.sadd('groups', str(chat_id))
    _invalidate_groups_cache()

def remove_group(chat_id):
    r.srem('groups', str(chat_id))
    r.sadd('recently_removed_groups', str(chat_id))
    r.expire('recently_removed_groups', 86400)
    r.delete(f'last_sent:{chat_id}')
    r.delete(f'link_only:{chat_id}')
    r.delete(f'repeat_task:{chat_id}')
    r.delete(f'repeat_interval:{chat_id}')
    r.delete(f'repeat_text:{chat_id}')
    r.delete(f'repeat_autodelete:{chat_id}')
    r.delete(f'group_start_reply:{chat_id}')
    r.delete(f'join_reply_enabled:{chat_id}')
    r.delete(f'join_reply_text:{chat_id}')
    r.delete(f'join_reply_autodelete:{chat_id}')
    r.delete(f'join_reply_last_msg:{chat_id}')
    r.delete(f'bot_kick_enabled:{chat_id}')
    r.delete(f'bot_kick_count:{chat_id}')
    r.delete(f'bot_kick_no_perm:{chat_id}')
    r.delete(f'bot_kick_notif_sent:{chat_id}')
    r.delete(f'global_repeat_task:{chat_id}')
    r.delete(f'group_start_reply_independent:{chat_id}')
    r.delete(f'cache_group_title:{chat_id}')
    r.delete(f'cache_group_status:{chat_id}')
    r.delete(f'global_last_sent:{chat_id}')
    r.delete(f'group_error:{chat_id}')
    r.delete(f'gr_next_send:{chat_id}')
    r.srem('groups_with_errors', str(chat_id))
    _invalidate_groups_cache()
    _invalidate_group_config_cache(chat_id)

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
    r.expire(sent_list_key, 604800)

def get_sent_messages(chat_id):
    key = f'sent_messages:{chat_id}'
    msgs = r.lrange(key, 0, -1)
    return [int(m) for m in msgs if m.isdigit()]

def clear_sent_messages(chat_id):
    r.delete(f'sent_messages:{chat_id}')

def get_group_info(chat_id, force_refresh=False):
    title_key = f'cache_group_title:{chat_id}'
    status_key = f'cache_group_status:{chat_id}'
    if not force_refresh:
        cached_title = r.get(title_key)
        cached_status = r.get(status_key)
        if cached_title and cached_status:
            return cached_title, cached_status
    try:
        chat = bot.get_chat(chat_id)
        title = chat.title or f"Group {chat_id}"
        member = bot.get_chat_member(chat_id, bot.get_me().id)
        status = "Admin" if member.status in ['administrator', 'creator'] else (
            "Member" if member.status == 'member' else "Other"
        )
        r.set(title_key, title, ex=600)
        r.set(status_key, status, ex=600)
        return title, status
    except Exception:
        return f"Group {chat_id}", "Error"

def bot_can_add_members(chat_id):
    try:
        me = bot.get_me()
        member = bot.get_chat_member(chat_id, me.id)
        if member.status == 'creator':
            return True, True
        if member.status == 'administrator':
            can_invite = getattr(member, 'can_invite_users', False)
            can_promote = getattr(member, 'can_promote_members', False)
            return can_invite, can_promote
        return False, False
    except Exception:
        return False, False

def get_bot_admin_permissions(chat_id):
    """
    Returns a dict of the bot's own admin permissions in the group.
    A bot can only grant permissions it itself holds — so we read our
    own permissions first and only attempt to grant those.
    """
    try:
        me = bot.get_me()
        member = bot.get_chat_member(chat_id, me.id)
        if member.status == 'creator':
            return {
                'can_manage_chat': True,
                'can_change_info': True,
                'can_delete_messages': True,
                'can_restrict_members': True,
                'can_invite_users': True,
                'can_pin_messages': True,
                'can_manage_video_chats': True,
                'can_promote_members': True,
                'can_post_stories': True,
                'can_edit_stories': True,
                'can_delete_stories': True,
            }
        if member.status == 'administrator':
            return {
                'can_manage_chat': getattr(member, 'can_manage_chat', False),
                'can_change_info': getattr(member, 'can_change_info', False),
                'can_delete_messages': getattr(member, 'can_delete_messages', False),
                'can_restrict_members': getattr(member, 'can_restrict_members', False),
                'can_invite_users': getattr(member, 'can_invite_users', False),
                'can_pin_messages': getattr(member, 'can_pin_messages', False),
                'can_manage_video_chats': getattr(member, 'can_manage_video_chats', False),
                'can_promote_members': getattr(member, 'can_promote_members', False),
                'can_post_stories': getattr(member, 'can_post_stories', False),
                'can_edit_stories': getattr(member, 'can_edit_stories', False),
                'can_delete_stories': getattr(member, 'can_delete_stories', False),
            }
        return {}
    except Exception:
        return {}

def bot_can_pin(chat_id):
    try:
        me = bot.get_me()
        member = bot.get_chat_member(chat_id, me.id)
        if member.status == 'creator':
            return True
        if member.status == 'administrator':
            return getattr(member, 'can_pin_messages', False)
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


# ─────────────────────────────────────────────────────────────────────────────
#  PLAN 1: REDIS CACHE LAYER
#  Reduces Redis reads by 95%+ through in-memory caching.
#  Reads from Redis only periodically; writes only on changes.
# ─────────────────────────────────────────────────────────────────────────────

_global_config_cache = {}          # key → (value, fetched_at)
_global_config_cache_lock = threading.Lock()
_CONFIG_CACHE_TTL = 60             # seconds — refresh global config every 60s

_groups_cache = []                 # list of group ids
_groups_cache_lock = threading.Lock()
_groups_cache_fetched_at = 0.0
_GROUPS_CACHE_TTL = 300            # 5 minutes

# Per-group repeat config cache: chat_id → {key: value, '_fetched': ts}
_group_repeat_cache = {}
_group_repeat_cache_lock = threading.Lock()
_GROUP_CONFIG_CACHE_TTL = 300      # 5 minutes

def _get_cached_global(key):
    """Return cached value for a global Redis key, refreshing if stale."""
    with _global_config_cache_lock:
        entry = _global_config_cache.get(key)
        if entry and (time.time() - entry[1]) < _CONFIG_CACHE_TTL:
            return entry[0]
    val = r.get(key)
    with _global_config_cache_lock:
        _global_config_cache[key] = (val, time.time())
    return val

def _invalidate_global_cache(*keys):
    """Called after an admin changes a setting — forces immediate re-read."""
    with _global_config_cache_lock:
        for k in keys:
            _global_config_cache.pop(k, None)

def _get_cached_groups():
    """Return cached group list, refreshing every 5 minutes."""
    global _groups_cache, _groups_cache_fetched_at
    with _groups_cache_lock:
        if time.time() - _groups_cache_fetched_at < _GROUPS_CACHE_TTL:
            return list(_groups_cache)
    groups = [int(g) for g in r.smembers('groups')]
    with _groups_cache_lock:
        _groups_cache = groups
        _groups_cache_fetched_at = time.time()
    return list(groups)

def _invalidate_groups_cache():
    global _groups_cache_fetched_at
    with _groups_cache_lock:
        _groups_cache_fetched_at = 0.0

def _get_cached_group_config(chat_id):
    """Return cached per-group repeat config, refreshing every 5 minutes."""
    with _group_repeat_cache_lock:
        cfg = _group_repeat_cache.get(chat_id)
        if cfg and (time.time() - cfg.get('_fetched', 0)) < _GROUP_CONFIG_CACHE_TTL:
            return cfg
    # Pipeline to read all keys in one round-trip
    pipe = r.pipeline()
    pipe.get(f'repeat_task:{chat_id}')
    pipe.get(f'repeat_text:{chat_id}')
    pipe.get(f'repeat_interval:{chat_id}')
    pipe.get(f'repeat_autodelete:{chat_id}')
    pipe.get(f'repeat_self_delete:{chat_id}')
    results = pipe.execute()
    cfg = {
        'repeat_task':      results[0],
        'repeat_text':      results[1],
        'repeat_interval':  results[2],
        'repeat_autodelete':results[3],
        'repeat_self_delete':results[4],
        '_fetched':         time.time(),
    }
    with _group_repeat_cache_lock:
        _group_repeat_cache[chat_id] = cfg
    return cfg

def _invalidate_group_config_cache(chat_id):
    with _group_repeat_cache_lock:
        _group_repeat_cache.pop(chat_id, None)


# ─────────────────────────────────────────────────────────────────────────────
#  PLAN 2: PER-GROUP RATE LIMITING + PRIORITY QUEUE
#  Replaces global flood-wait with per-group cooldowns.
#  No single group's 429 blocks other groups.
# ─────────────────────────────────────────────────────────────────────────────

# Priority levels: 1=join replies, 2=start@ replies, 3=manual broadcasts, 4=global repeat

_send_queue = []                   # heapq of (priority, seq, chat_id, text, result_holder)
_send_queue_lock = threading.Lock()
_send_queue_event = threading.Event()
_send_queue_seq = 0                # tie-breaker for same priority
_INTER_MSG_DELAY = 0.4            # 0.4s between any two sends globally (≤2.5/sec)
_MAX_PER_GROUP_PER_MIN = 18       # stay under Telegram's ~20/min per-chat limit

# Per-group rate state (in memory — no Redis needed for this)
_group_rate_lock = threading.Lock()
_group_msg_timestamps = {}        # chat_id → deque of send timestamps
_group_cooldown_until = {}        # chat_id → float timestamp (from 429)

def _group_is_allowed(chat_id):
    """Check per-group rate limit. Returns True if OK to send."""
    now = time.time()
    with _group_rate_lock:
        # Check 429 cooldown first
        cooldown = _group_cooldown_until.get(chat_id, 0)
        if now < cooldown:
            return False
        # Check message rate in last 60s
        timestamps = _group_msg_timestamps.setdefault(chat_id, deque())
        cutoff = now - 60
        while timestamps and timestamps[0] < cutoff:
            timestamps.popleft()
        return len(timestamps) < _MAX_PER_GROUP_PER_MIN

def _group_record_send(chat_id):
    """Record a successful send for rate tracking."""
    with _group_rate_lock:
        timestamps = _group_msg_timestamps.setdefault(chat_id, deque())
        timestamps.append(time.time())

def _group_set_cooldown(chat_id, retry_after):
    """Set per-group 429 cooldown. Does NOT affect other groups."""
    with _group_rate_lock:
        _group_cooldown_until[chat_id] = time.time() + retry_after
    _increment_flood_counter(retry_after)
    logger.warning(f"[FLOOD] Group {chat_id} cooldown {retry_after}s")

def _do_send(chat_id, text, _flood_callback=None, reply_markup=None):
    """Execute send. 429 is per-group only — never blocks other groups."""
    for attempt in range(3):
        try:
            sent = bot.send_message(
                chat_id, text,
                disable_web_page_preview=True,
                reply_markup=reply_markup
            )
            r.srem('groups_with_errors', str(chat_id))
            r.delete(f'group_error:{chat_id}')
            _group_record_send(chat_id)
            return sent
        except telebot.apihelper.ApiTelegramException as e:
            err = str(e)
            if '429' in err:
                try:
                    retry_after = int(re.search(r'retry after (\d+)', err, re.IGNORECASE).group(1))
                except Exception:
                    retry_after = 30
                _group_set_cooldown(chat_id, retry_after)
                if _flood_callback:
                    _flood_callback(retry_after)
                    return None
                else:
                    return None
            else:
                r.sadd('groups_with_errors', str(chat_id))
                r.set(f'group_error:{chat_id}', err[:200])
                _log_runtime_error(chat_id, 'send', err[:200])
                logger.error(f"[ERR] send to {chat_id}: {err[:100]}")
                return None
        except Exception as e:
            r.sadd('groups_with_errors', str(chat_id))
            r.set(f'group_error:{chat_id}', str(e)[:200])
            _log_runtime_error(chat_id, 'send', str(e)[:200])
            logger.error(f"[ERR] send to {chat_id}: {str(e)[:100]}")
            return None
    return None

def _send_queue_worker():
    """Priority queue worker. Per-group rate limit enforced before each send."""
    while True:
        _send_queue_event.wait()
        requeue_batch = []
        while True:
            with _send_queue_lock:
                if not _send_queue:
                    _send_queue_event.clear()
                    break
                priority, seq, chat_id, text, result_holder, reply_markup = heapq.heappop(_send_queue)

            if not _group_is_allowed(chat_id):
                requeue_batch.append((priority + 0.001, seq, chat_id, text, result_holder, reply_markup))
                time.sleep(0.05)
                continue

            sent = _do_send(chat_id, text, reply_markup=reply_markup)

            if sent is None and not _group_is_allowed(chat_id):
                requeue_batch.append((priority + 0.001, seq, chat_id, text, result_holder, reply_markup))
            else:
                if result_holder is not None:
                    result_holder.append(sent)

            time.sleep(_INTER_MSG_DELAY)

        if requeue_batch:
            with _send_queue_lock:
                for item in requeue_batch:
                    heapq.heappush(_send_queue, item)
                _send_queue_event.set()
            time.sleep(1)

_send_worker_thread = threading.Thread(target=_send_queue_worker, daemon=True)
_send_worker_thread.start()

def _enqueue(chat_id, text, result_holder, priority=3, reply_markup=None):
    global _send_queue_seq
    with _send_queue_lock:
        _send_queue_seq += 1
        heapq.heappush(_send_queue, (priority, _send_queue_seq, chat_id, text, result_holder, reply_markup))
        _send_queue_event.set()

def safe_send(chat_id, text, priority=3, reply_markup=None):
    """Queue a message, block until sent. Returns Message or None."""
    result_holder = []
    _enqueue(chat_id, text, result_holder, priority, reply_markup=reply_markup)
    deadline = time.time() + 180
    while time.time() < deadline:
        if result_holder:
            return result_holder[0]
        time.sleep(0.05)
    return None

def safe_send_nowait(chat_id, text, priority=2, reply_markup=None):
    """Queue a message without blocking. Fire and forget."""
    _enqueue(chat_id, text, None, priority, reply_markup=reply_markup)

def safe_delete(chat_id, message_id):
    """Delete a message, logging any errors."""
    try:
        bot.delete_message(chat_id, message_id)
    except Exception as e:
        _log_runtime_error(chat_id, 'delete', str(e)[:200])
        logger.error(f"[DELETE ERROR] chat_id={chat_id} message_id={message_id} error={e}")


# ─────────────────────────────────────────────────────────────────────────────
#  PER-GROUP REPEATING MESSAGE
#
#  Uses blocking safe_send() so the message_id comes back immediately.
#  This is the ONLY correct way to make delete-previous and self-delete work.
#  Each group has its own independent thread — they don't block each other.
# ─────────────────────────────────────────────────────────────────────────────

active_repeat_threads = {}
_repeat_thread_lock = threading.Lock()

def repeat_message_task(chat_id):
    """Per-group repeat thread. Uses in-memory cache — reads Redis only every 5 min."""
    _last_cfg_refresh = 0.0
    _cfg = {}

    while True:
        now = time.time()
        # Refresh config from Redis every 5 minutes (or on first run)
        if now - _last_cfg_refresh >= _GROUP_CONFIG_CACHE_TTL:
            _cfg = _get_cached_group_config(chat_id)
            _last_cfg_refresh = now

        if _cfg.get('repeat_task') != 'True':
            break

        text = _cfg.get('repeat_text')
        if not text:
            break

        cycle_start = time.time()
        autodelete_prev = _cfg.get('repeat_autodelete') == 'True'
        self_delete_after = _cfg.get('repeat_self_delete')

        if autodelete_prev:
            prev_id = r.get(f'last_sent:{chat_id}')
            if prev_id:
                safe_delete(chat_id, int(prev_id))

        sent = safe_send(chat_id, text, priority=3)
        if sent:
            r.set(f'last_sent:{chat_id}', str(sent.message_id))
            save_last_sent(chat_id, sent.message_id)

            if self_delete_after:
                secs = int(self_delete_after)
                msg_id_snap = sent.message_id
                def _self_delete(cid=chat_id, mid=msg_id_snap, delay=secs):
                    time.sleep(delay)
                    safe_delete(cid, mid)
                threading.Thread(target=_self_delete, daemon=True).start()

        interval = int(_cfg.get('repeat_interval') or 3600)
        elapsed = time.time() - cycle_start
        sleep_time = max(interval - elapsed, 1)

        # Sleep in chunks so we can pick up stop signals within 10s
        slept = 0
        while slept < sleep_time:
            chunk = min(10, sleep_time - slept)
            time.sleep(chunk)
            slept += chunk
            # Quick in-memory check: re-read from Redis only if cache is stale
            now2 = time.time()
            if now2 - _last_cfg_refresh >= _GROUP_CONFIG_CACHE_TTL:
                _cfg = _get_cached_group_config(chat_id)
                _last_cfg_refresh = now2
            if _cfg.get('repeat_task') != 'True':
                return

def start_repeat_thread(chat_id):
    key = f'repeat_task:{chat_id}'
    if r.get(key) != 'True':
        return
    with _repeat_thread_lock:
        existing = active_repeat_threads.get(chat_id)
        if existing and existing.is_alive():
            return
        thread = threading.Thread(target=repeat_message_task, args=(chat_id,), daemon=True)
        thread.start()
        active_repeat_threads[chat_id] = thread



# GLOBAL REPEAT - OPTIMIZED WITH CACHING

_global_repeat_worker_thread = None
_global_repeat_lock = threading.Lock()
_global_repeat_running = False

def _global_repeat_worker():
    global _global_repeat_running
    print("[GLOBAL REPEAT] Worker started")

    # In-memory cache for global config
    _cfg_cache = {}
    _cfg_fetched_at = 0.0
    # In-memory next_send times (also persisted to Redis for restart survival)
    _next_send = {}   # chat_id → float

    while True:
        now = time.time()

        # Refresh global config every 60 seconds
        if now - _cfg_fetched_at >= 60:
            pipe = r.pipeline()
            pipe.get('global_repeat_task')
            pipe.get('global_repeat_text')
            pipe.get('global_repeat_interval')
            pipe.get('global_repeat_self_delete')
            pipe.get('global_repeat_autodelete')
            results = pipe.execute()
            _cfg_cache = {
                'task':        results[0],
                'text':        results[1],
                'interval':    results[2],
                'self_delete': results[3],
                'autodelete':  results[4],
            }
            _cfg_fetched_at = now

        if _cfg_cache.get('task') != 'True':
            print("[GLOBAL REPEAT] Stopped (flag off)")
            _global_repeat_running = False
            return

        text = _cfg_cache.get('text')
        if not text:
            print("[GLOBAL REPEAT] No text set, stopping")
            r.set('global_repeat_task', 'False')
            _global_repeat_running = False
            return

        interval = int(_cfg_cache.get('interval') or 3600)
        self_delete_after_raw = _cfg_cache.get('self_delete')
        self_delete_secs = int(self_delete_after_raw) if self_delete_after_raw else None
        autodelete_prev = _cfg_cache.get('autodelete') == 'True'

        now = time.time()
        # Refresh group list every 5 minutes (handled by _get_cached_groups)
        groups = _get_cached_groups()
        sent_this_tick = False

        for chat_id in groups:
            # Re-check task flag from cache (no Redis read per group)
            if _cfg_cache.get('task') != 'True':
                break

            # Initialize next_send from Redis if first time seeing this group
            if chat_id not in _next_send:
                raw = r.get(f'gr_next_send:{chat_id}')
                _next_send[chat_id] = float(raw) if raw else 0.0

            if now < _next_send[chat_id]:
                continue  # Not due yet

            # Check per-group rate limit before sending
            if not _group_is_allowed(chat_id):
                # Bump next_send slightly so we don't hammer this check
                _next_send[chat_id] = now + 5
                continue

            # Schedule next send BEFORE sending (prevent double-send)
            if _next_send[chat_id] == 0.0:
                new_next = now + interval
            else:
                new_next = _next_send[chat_id] + interval
            _next_send[chat_id] = new_next
            # Write to Redis asynchronously (persistence for restarts)
            r.set(f'gr_next_send:{chat_id}', str(new_next), ex=interval * 3 + 60)

            if autodelete_prev:
                prev_id = r.get(f'global_last_sent:{chat_id}')
                if prev_id:
                    safe_delete(chat_id, int(prev_id))

            def _on_flood(retry_after, _cid=chat_id, _key=f'gr_next_send:{chat_id}', _interval=interval):
                _group_set_cooldown(_cid, retry_after)
                new_t = time.time() + retry_after
                _next_send[_cid] = new_t
                r.set(_key, str(new_t), ex=_interval * 3 + 60)

            # Direct send (not via shared queue) — needed for immediate message_id
            sent = _do_send(chat_id, text, _flood_callback=_on_flood)

            if sent:
                r.set(f'global_last_sent:{chat_id}', str(sent.message_id))
                if self_delete_secs is not None:
                    def _self_del(cid=chat_id, mid=sent.message_id, delay=self_delete_secs):
                        time.sleep(delay)
                        safe_delete(cid, mid)
                    threading.Thread(target=_self_del, daemon=True).start()

            sent_this_tick = True
            time.sleep(_INTER_MSG_DELAY)

        if not sent_this_tick:
            time.sleep(10)  # No group was due — wait 10s (was 0.5s) before next scan

def start_global_repeat_thread():
    global _global_repeat_worker_thread, _global_repeat_running
    if r.get('global_repeat_task') != 'True':
        return
    with _global_repeat_lock:
        if _global_repeat_running and _global_repeat_worker_thread and _global_repeat_worker_thread.is_alive():
            return
        _global_repeat_running = True
        _global_repeat_worker_thread = threading.Thread(
            target=_global_repeat_worker, daemon=True
        )
        _global_repeat_worker_thread.start()

def stop_global_repeat():
    """Stop the global repeat worker cleanly."""
    global _global_repeat_running
    r.set('global_repeat_task', 'False')
    _global_repeat_running = False

def reset_global_repeat_schedule():
    """Clear next-send schedules so all groups fire immediately on next start."""
    for g in get_groups():
        r.delete(f'gr_next_send:{g}')


# ─── Heartbeat Task ───────────────────────────────────────────────────────────
def _heartbeat_worker():
    """Runs every 60 seconds and logs key system metrics."""
    while True:
        time.sleep(60)
        try:
            with _repeat_thread_lock:
                active_repeats = sum(1 for t in active_repeat_threads.values() if t.is_alive())
            total_groups = r.scard('groups')
            try:
                r.ping()
                redis_status = "OK"
            except Exception:
                redis_status = "ERROR"
            try:
                proc = psutil.Process()
                mem_mb = proc.memory_info().rss / 1024 / 1024
                mem_str = f"{mem_mb:.1f} MB"
            except Exception:
                mem_str = "N/A"
            with _flood_wait_lock:
                flood_count = _flood_wait_counter
            logger.info(
                f"HEARTBEAT | active_repeats={active_repeats} | groups={total_groups} "
                f"| redis={redis_status} | memory={mem_str} | flood_waits={flood_count}"
            )
        except Exception as e:
            logger.error(f"Heartbeat error: {e}")

_heartbeat_thread = threading.Thread(target=_heartbeat_worker, daemon=True)
_heartbeat_thread.start()


# ─────────────────────────────────────────────────────────────────────────────
#  BACKUP / RESTORE SYSTEM
# ─────────────────────────────────────────────────────────────────────────────

# Module-level state — never stored in Redis so restore itself can't break it
_last_backup_data   = None      # io.BytesIO of the last successful backup JSON
_last_backup_time   = 0.0       # epoch of last successful backup collection
_last_backup_sent   = 0.0       # epoch of last auto-send to owner
_backup_state_lock  = threading.Lock()

# Restore mode: {user_id: True}  (in-memory only, intentionally not in Redis)
_restore_mode       = {}
_restore_mode_lock  = threading.Lock()

# ── Key patterns this bot uses ────────────────────────────────────────────────
_BACKUP_SCALAR_KEYS = [
    'link_only_global', 'global_start_reply',
    'global_join_reply_enabled', 'global_join_reply_text',
    'global_join_reply_autodelete',
    'global_group_start_reply_enabled', 'global_group_start_reply',
    'added_to_group_msg_enabled', 'added_to_group_msg',
    'aawm_enabled', 'aawm_text', 'aawm_buttons_global',
    'global_repeat_task', 'global_repeat_text',
    'global_repeat_interval', 'global_repeat_self_delete',
    'global_repeat_autodelete',
    'bot_kick_enabled', 'bot_kick_count',
]

_BACKUP_SET_KEYS = [
    'groups', 'bot_users', 'recently_removed_groups', 'groups_with_errors',
    'bot_kick_whitelist',
]

_BACKUP_HASH_KEYS = [
    'user_info', 'user_first_seen',
]

_BACKUP_PATTERN_KEYS = [
    'repeat_task:*', 'repeat_text:*', 'repeat_interval:*',
    'repeat_autodelete:*', 'repeat_self_delete:*',
    'group_start_reply:*', 'group_start_reply_independent:*',
    'join_reply_enabled:*', 'join_reply_text:*',
    'join_reply_autodelete:*', 'join_reply_last_msg:*',
    'bot_kick_enabled:*', 'bot_kick_count:*', 'bot_kick_no_perm:*',
    'link_only:*', 'last_sent:*', 'global_last_sent:*',
    'gr_next_send:*', 'group_error:*',
    'sent_messages:*', 'private_sent:*',
    'aawm_enabled:*', 'aawm_text:*', 'aawm_buttons:*',
    'cache_group_title:*', 'cache_group_status:*',
    'inline_btns:*',
    'embedded_draft:*', 'post_draft:*',
    'btn_broadcast_text:*', 'btn_broadcast_btn_text:*', 'btn_broadcast_btn_url:*',
]


def _collect_all_redis_data():
    """
    Collect every Redis key this bot uses into a plain dict.
    Uses pipelines where possible for efficiency.
    Returns (data_dict, total_key_count).
    """
    data = {
        'meta': {
            'bot': 'GroupManagementBot',
            'timestamp': datetime.datetime.utcnow().isoformat() + 'Z',
            'version': 1,
        },
        'scalars': {},
        'sets': {},
        'hashes': {},
        'lists': {},
        'pattern_scalars': {},
    }

    # ── Scalar keys ───────────────────────────────────────────────────────────
    try:
        pipe = r.pipeline()
        for k in _BACKUP_SCALAR_KEYS:
            pipe.get(k)
        results = pipe.execute()
        for k, v in zip(_BACKUP_SCALAR_KEYS, results):
            data['scalars'][k] = v
    except Exception as e:
        logger.error(f"[BACKUP] Scalar read error: {e}")

    # ── Set keys ──────────────────────────────────────────────────────────────
    try:
        pipe = r.pipeline()
        for k in _BACKUP_SET_KEYS:
            pipe.smembers(k)
        results = pipe.execute()
        for k, v in zip(_BACKUP_SET_KEYS, results):
            data['sets'][k] = list(v) if v else []
    except Exception as e:
        logger.error(f"[BACKUP] Set read error: {e}")

    # ── Hash keys ─────────────────────────────────────────────────────────────
    try:
        pipe = r.pipeline()
        for k in _BACKUP_HASH_KEYS:
            pipe.hgetall(k)
        results = pipe.execute()
        for k, v in zip(_BACKUP_HASH_KEYS, results):
            data['hashes'][k] = v if v else {}
    except Exception as e:
        logger.error(f"[BACKUP] Hash read error: {e}")

    # ── Pattern keys (scan + pipeline read) ───────────────────────────────────
    try:
        all_pattern_keys = []
        for pattern in _BACKUP_PATTERN_KEYS:
            for key in r.scan_iter(pattern):
                all_pattern_keys.append(key)

        # Determine type per key and read in one pipeline
        if all_pattern_keys:
            pipe = r.pipeline()
            for key in all_pattern_keys:
                pipe.type(key)
            types_result = pipe.execute()

            pipe2 = r.pipeline()
            for key, ktype in zip(all_pattern_keys, types_result):
                if ktype == 'string':
                    pipe2.get(key)
                elif ktype == 'list':
                    pipe2.lrange(key, 0, -1)
                elif ktype == 'set':
                    pipe2.smembers(key)
                elif ktype == 'hash':
                    pipe2.hgetall(key)
                else:
                    pipe2.get(key)
            values_result = pipe2.execute()

            for key, ktype, val in zip(all_pattern_keys, types_result, values_result):
                if ktype == 'list':
                    data['lists'][key] = list(val) if val else []
                elif ktype == 'set':
                    data['sets'][key] = list(val) if val else []
                elif ktype == 'hash':
                    data['hashes'][key] = val if val else {}
                else:
                    data['pattern_scalars'][key] = val
    except Exception as e:
        logger.error(f"[BACKUP] Pattern scan error: {e}")

    total = (
        len(data['scalars']) +
        len(data['sets']) +
        len(data['hashes']) +
        len(data['lists']) +
        len(data['pattern_scalars'])
    )
    return data, total


def _build_backup_bytes(data):
    """Serialize backup dict to UTF-8 JSON bytes wrapped in io.BytesIO."""
    raw = json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8')
    buf = io.BytesIO(raw)
    buf.name = f"backup_{data['meta']['timestamp'][:10]}.json"
    return buf


def _do_send_backup_to_owner(buf, caption=None):
    """Send the backup file to the owner's private chat (Saved Messages)."""
    buf.seek(0)
    cap = caption or f"🗄 Auto-backup — {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC"
    bot.send_document(OWNER_ID, buf, caption=cap, visible_file_name=buf.name)


# ── Auto-backup background thread ─────────────────────────────────────────────
def _auto_backup_worker():
    """
    Runs every 30 minutes.
    Sends the backup file to the owner at most once per hour.
    Never blocks the bot or raises to the caller.
    """
    global _last_backup_data, _last_backup_time, _last_backup_sent
    # Stagger first run by 5 minutes so startup noise settles
    time.sleep(300)
    while True:
        try:
            data, total = _collect_all_redis_data()
            buf = _build_backup_bytes(data)
            buf_size = buf.getbuffer().nbytes
            now = time.time()

            # ── Health check: warn if backup is suspiciously small ─────────────
            # A healthy backup with even minimal data is well over 5 KB.
            # Anything under 5 KB means Redis is empty or wiped.
            _HEALTHY_BACKUP_MIN_BYTES = 5120  # 5 KB
            if buf_size < _HEALTHY_BACKUP_MIN_BYTES:
                logger.warning(f"[BACKUP] Suspiciously small backup: {buf_size} bytes — Redis may be empty/wiped. NOT sending file.")
                try:
                    bot.send_message(
                        OWNER_ID,
                        f"⚠️ *Backup Warning!*\n\n"
                        f"Redis appears empty or wiped!\n"
                        f"Backup size is only *{buf_size} bytes* — this is not a real backup.\n\n"
                        f"❌ File NOT sent to protect your last good backup.\n\n"
                        f"🔴 Check your Redis immediately.\n"
                        f"✅ If you have a previous good backup file, use /restore now.",
                        parse_mode='Markdown'
                    )
                except Exception as warn_e:
                    logger.error(f"[BACKUP] Failed to send wipe warning: {warn_e}")
                # Still update in-memory state so /backup command shows the warning too
                with _backup_state_lock:
                    _last_backup_data = buf
                    _last_backup_time = now
                time.sleep(1800)
                continue

            with _backup_state_lock:
                _last_backup_data = buf
                _last_backup_time = now
            logger.info(f"[BACKUP] Auto-backup collected {total} keys ({buf_size} bytes).")

            # Send to owner at most once per hour
            with _backup_state_lock:
                last_sent = _last_backup_sent
            if now - last_sent >= 3600:
                _do_send_backup_to_owner(buf)
                with _backup_state_lock:
                    _last_backup_sent = now
                logger.info("[BACKUP] Auto-backup sent to owner.")
        except Exception as e:
            logger.error(f"[BACKUP] Auto-backup worker error: {e}")

        time.sleep(1800)  # 30 minutes


def start_backup_thread():
    t = threading.Thread(target=_auto_backup_worker, daemon=True)
    t.start()
    logger.info("[BACKUP] Auto-backup thread started.")


# ─────────────────────────────────────────────────────────────────────────────
#  GROUP EVENT HANDLERS
# ─────────────────────────────────────────────────────────────────────────────

@bot.message_handler(content_types=['new_chat_members'])
def handle_new_chat_members(message):
    chat_id = message.chat.id
    bot_id = bot.get_me().id

    bot_joined = any(m.id == bot_id for m in message.new_chat_members)

    # ── Handle bot joining ──────────────────────────────────────────────────
    if bot_joined:
        add_group(chat_id)

        # ── Suspicious group check ──────────────────────────────────────────
        try:
            member_count_check = bot.get_chat_member_count(chat_id)
        except Exception:
            member_count_check = 999  # if we can't fetch, don't flag
        adder_id = message.from_user.id if message.from_user else None
        if member_count_check < 5 and adder_id != OWNER_ID:
            try:
                chat_check = bot.get_chat(chat_id)
                sus_title = chat_check.title or f"Group {chat_id}"
            except Exception:
                sus_title = f"Group {chat_id}"
            adder_name = message.from_user.full_name if message.from_user else "Unknown"
            sus_markup = types.InlineKeyboardMarkup(row_width=2)
            sus_markup.row(
                types.InlineKeyboardButton("✅ Confirm Leave", callback_data=f"sus_leave:{chat_id}"),
                types.InlineKeyboardButton("🟢 Stay", callback_data=f"sus_stay:{chat_id}"),
            )
            logger.warning(f"Suspicious group detected: {chat_id} ({sus_title}), members={member_count_check}, adder={adder_id}")
            try:
                bot.send_message(
                    OWNER_ID,
                    f"⚠️ *Suspicious Group Detected!*\n\n"
                    f"📌 *Group:* {sus_title}\n"
                    f"🆔 *ID:* `{chat_id}`\n"
                    f"👥 *Members:* {member_count_check}\n"
                    f"👤 *Added by:* {adder_name} (`{adder_id}`)\n\n"
                    f"This group has fewer than 5 members and was not added by you.\n"
                    f"Do you want the bot to leave?",
                    parse_mode='Markdown',
                    reply_markup=sus_markup
                )
            except Exception:
                pass
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
                bot_member = bot.get_chat_member(chat_id, bot_id)
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
            bot.send_message(OWNER_ID, notification.replace('*', '').replace('`', ''), reply_markup=markup)

        invite_link = "https://t.me/AllMusicShazamandlyrics_bot?startgroup=true&admin=change_info+delete_messages+restrict_members+invite_users+pin_messages+manage_video_chats+anonymous+manage_chat+post_stories+edit_stories+delete_stories"
        added_msg_enabled = r.get('added_to_group_msg_enabled') == 'True'
        added_msg = r.get('added_to_group_msg') or invite_link
        if added_msg_enabled:
            try:
                bot.reply_to(message, added_msg)
            except telebot.apihelper.ApiTelegramException as e:
                if '403' in str(e):
                    kicked = True

        if kicked:
            remove_group(chat_id)
        else:
            # ── Scan group for existing bots on join ──────────────────────────
            if _botdet_is_enabled_group(chat_id):
                def _scan_on_join(cid=chat_id):
                    time.sleep(2)  # brief delay to let Telegram settle after bot joins
                    k, wl, np, err, found, err_detail = _botdet_scan_group(cid)
                    group_title = _botdet_get_title(cid)
                    err_line = f"\n⚠️ Error detail: `{err_detail}`" if err_detail else ""
                    try:
                        bot.send_message(
                            OWNER_ID,
                            f"🤖 *Bot Scan on Join — {group_title}*\n\n"
                            f"🔍 Admin bots found: {found}\n"
                            f"✅ Kicked: {k}\n"
                            f"⚪ Whitelisted (skipped): {wl}\n"
                            f"⚠️ No permission (skipped): {np}\n"
                            f"❌ Errors: {err}{err_line}\n\n"
                            f"ℹ️ Only admin bots are visible on join scan.\n"
                            f"Member bots will be caught when they next rejoin.",
                            parse_mode='Markdown'
                        )
                    except Exception:
                        pass
                threading.Thread(target=_scan_on_join, daemon=True).start()
        return  # Done handling bot join — don't fall through to user join logic

    # ── Handle regular user joining → join reply ────────────────────────────
    global_enabled    = r.get('global_join_reply_enabled') == 'True'
    global_autodelete = r.get('global_join_reply_autodelete') == 'True'

    for member in message.new_chat_members:
        if member.id == bot_id:
            continue

        # ── Bot detection: kick other bots silently ───────────────────────────
        if getattr(member, 'is_bot', False):
            threading.Thread(
                target=_botdet_handle_new_bot,
                args=(chat_id, member),
                daemon=True
            ).start()
            continue  # don't send join reply to bots

        group_enabled = r.get(f'join_reply_enabled:{chat_id}')
        # Per-group autodelete overrides global if explicitly set, else falls back to global
        group_autodelete_raw = r.get(f'join_reply_autodelete:{chat_id}')
        if group_autodelete_raw is not None:
            autodelete = group_autodelete_raw == 'True'
        else:
            autodelete = global_autodelete

        reply_text = None
        if group_enabled == 'True':
            reply_text = r.get(f'join_reply_text:{chat_id}') or r.get('global_join_reply_text') or "Welcome!"
        elif group_enabled != 'False' and global_enabled:
            reply_text = r.get('global_join_reply_text') or "Welcome!"
        # group_enabled == 'False': this group explicitly opted out — do nothing

        if reply_text:
            # Delete previous join reply atomically — getdel ensures only one
            # concurrent request wins the previous message ID, preventing orphans
            if autodelete:
                prev_id = r.getdel(f'join_reply_last_msg:{chat_id}')
                if prev_id:
                    try:
                        bot.delete_message(chat_id, int(prev_id))
                    except Exception:
                        pass

            # Send and track the new join reply message id
            def _send_and_track(cid=chat_id, text=reply_text, track=autodelete):
                sent = safe_send(cid, text)
                if sent and track:
                    r.set(f'join_reply_last_msg:{cid}', str(sent.message_id), ex=604800)
            threading.Thread(target=_send_and_track, daemon=True).start()


@bot.message_handler(content_types=['left_chat_member'])
def handle_left_chat_member(message):
    if message.left_chat_member.id == bot.get_me().id:
        remove_group(message.chat.id)


# ─────────────────────────────────────────────────────────────────────────────
#  AAWM — AUTO APPROVE + WELCOME MESSAGE
# ─────────────────────────────────────────────────────────────────────────────

# Redis defaults for AAWM
if r.get('aawm_enabled') is None:
    r.set('aawm_enabled', 'False')
if r.get('aawm_text') is None:
    r.set('aawm_text', 'Welcome to the group! Please read the rules.')

@bot.chat_join_request_handler()
def handle_join_request(update):
    """Auto-approve join requests and send private welcome message."""
    chat_id = update.chat.id
    user = update.from_user
    user_id = user.id

    # Only handle groups where AAWM is enabled
    aawm_enabled = r.get(f'aawm_enabled:{chat_id}') or r.get('aawm_enabled')
    if aawm_enabled != 'True':
        return

    # Send private welcome message first, then approve
    aawm_text = r.get(f'aawm_text:{chat_id}') or r.get('aawm_text') or 'Welcome!'
    aawm_btns_raw = r.get(f'aawm_buttons:{chat_id}') or r.get('aawm_buttons_global')
    reply_markup = None
    if aawm_btns_raw:
        try:
            btn_list = json.loads(aawm_btns_raw)
            flat = [(b['text'], b['url']) for b in btn_list]
            rows = _auto_layout_buttons(flat)
            reply_markup = build_inline_keyboard(rows)
        except Exception:
            pass
    try:
        bot.send_message(
            user_id, aawm_text,
            reply_markup=reply_markup,
            disable_web_page_preview=True
        )
    except Exception:
        pass
    try:
        bot.approve_chat_join_request(chat_id, user_id)
    except Exception as e:
        logger.error(f"AAWM: Failed to approve {user_id} in {chat_id}: {e}")


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
        bot.reply_to(message, reply, disable_web_page_preview=True)

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

# ─────────────────────────────────────────────────────────────────────────────
#  /backup COMMAND
# ─────────────────────────────────────────────────────────────────────────────

@bot.message_handler(commands=['backup'], chat_types=['private'])
def backup_command(message):
    global _last_backup_data, _last_backup_time, _last_backup_sent
    if message.from_user.id != OWNER_ID:
        return
    try:
        # If a fresh backup exists (< 5 min old), just send it immediately
        with _backup_state_lock:
            age = time.time() - _last_backup_time
            cached = _last_backup_data
        if cached and age < 300:
            cached.seek(0)
            bot.send_message(message.chat.id, "📦 Using latest cached backup (< 5 min old)...")
            _do_send_backup_to_owner(
                cached,
                caption=f"📦 Manual /backup — {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC (cached)"
            )
            bot.send_message(message.chat.id, "✅ Backup sent to your Saved Messages!")
            return

        # Live backup with step-by-step progress
        status = bot.send_message(message.chat.id, "⏳ Starting backup...")

        def _edit(text):
            try:
                bot.edit_message_text(text, message.chat.id, status.message_id)
            except Exception:
                pass

        _edit("🔍 Scanning Redis keys...\n[██░░░░░░░░] 20%")
        time.sleep(0.3)

        try:
            data, total = _collect_all_redis_data()
        except Exception as e:
            _edit(f"❌ Failed to collect Redis data:\n{e}")
            logger.error(f"[BACKUP] Manual backup collect failed: {e}")
            return

        _edit(f"📦 Building JSON... ({total} keys)\n[██████░░░░] 60%")
        time.sleep(0.3)

        try:
            buf = _build_backup_bytes(data)
        except Exception as e:
            _edit(f"❌ Failed to build JSON:\n{e}")
            logger.error(f"[BACKUP] Manual backup JSON build failed: {e}")
            return

        with _backup_state_lock:
            _last_backup_data = buf
            _last_backup_time = time.time()

        _edit("📤 Sending to Saved Messages...\n[████████░░] 80%")
        time.sleep(0.3)

        # Health check before sending
        buf_size = buf.getbuffer().nbytes
        if buf_size < 5120:
            _edit(
                f"⚠️ *Backup Warning!*\n\n"
                f"Redis appears empty or wiped!\n"
                f"Backup is only {buf_size} bytes — not a real backup.\n\n"
                f"❌ File NOT sent to protect your last good backup.\n"
                f"🔴 Use /restore with a previous good backup file."
            )
            logger.warning(f"[BACKUP] Manual backup aborted — only {buf_size} bytes.")
            return

        try:
            _do_send_backup_to_owner(
                buf,
                caption=f"📦 Manual /backup — {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC"
            )
            with _backup_state_lock:
                _last_backup_sent = time.time()
        except Exception as e:
            _edit(f"❌ Failed to send backup file:\n{e}")
            logger.error(f"[BACKUP] Manual backup send failed: {e}")
            return

        _edit(
            f"✅ Backup sent!\n"
            f"[██████████] 100%\n\n"
            f"📊 {total} keys backed up\n"
            f"📁 File sent to your Saved Messages"
        )
        logger.info(f"[BACKUP] Manual backup completed: {total} keys.")

    except Exception as e:
        logger.error(f"[BACKUP] backup_command top-level error: {e}")
        try:
            bot.send_message(message.chat.id, f"❌ Backup failed unexpectedly:\n{e}")
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
#  /restore COMMAND
# ─────────────────────────────────────────────────────────────────────────────

@bot.message_handler(commands=['restore'], chat_types=['private'])
def restore_command(message):
    if message.from_user.id != OWNER_ID:
        return
    try:
        with _restore_mode_lock:
            _restore_mode[message.from_user.id] = True
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("❌ Cancel", callback_data="restore_cancel"))
        bot.send_message(
            message.chat.id,
            "🔄 *Restore mode activated.*\n\n"
            "Forward or upload the backup `.json` file now.\n"
            "Type /cancel to abort at any time.",
            parse_mode='Markdown',
            reply_markup=markup
        )
        bot.register_next_step_handler(message, _process_restore_file)
    except Exception as e:
        logger.error(f"[RESTORE] restore_command error: {e}")
        try:
            bot.send_message(message.chat.id, f"❌ Restore initiation failed:\n{e}")
        except Exception:
            pass


@bot.message_handler(commands=['cancel'], chat_types=['private'])
def cancel_command(message):
    if message.from_user.id != OWNER_ID:
        return
    try:
        with _restore_mode_lock:
            was_active = _restore_mode.pop(message.from_user.id, False)
        if was_active:
            bot.send_message(message.chat.id, "❌ Restore cancelled. Bot running normally.")
        else:
            bot.send_message(message.chat.id, "ℹ️ Nothing to cancel.")
    except Exception as e:
        logger.error(f"[RESTORE] cancel_command error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
#  /recover COMMAND — force-register groups back after a data wipe
# ─────────────────────────────────────────────────────────────────────────────

@bot.message_handler(commands=['recover'], chat_types=['private'])
def recover_command(message):
    if message.from_user.id != OWNER_ID:
        return
    try:
        bot.send_message(
            message.chat.id,
            "♻️ *Group Recovery Mode*\n\n"
            "Paste the group ID(s) you want to force-register.\n"
            "Separate multiple IDs with spaces or commas.\n\n"
            "Example:\n"
            "`-1001234567890 -1009876543210`\n\n"
            "These groups will be added back to the `groups` set in Redis "
            "and all their existing configs (if any) will be reactivated.\n\n"
            "Type /cancel to abort.",
            parse_mode='Markdown'
        )
        bot.register_next_step_handler(message, _process_recover_ids)
    except Exception as e:
        logger.error(f"[RECOVER] recover_command error: {e}")
        try:
            bot.send_message(message.chat.id, f"❌ Recovery initiation failed:\n{e}")
        except Exception:
            pass


def _process_recover_ids(message):
    if message.from_user.id != OWNER_ID:
        return
    try:
        if message.text and message.text.strip().lower() in ['/cancel', 'cancel']:
            bot.send_message(message.chat.id, "❌ Recovery cancelled.")
            return

        if not message.text:
            bot.send_message(message.chat.id, "⚠️ Please send group IDs as text.")
            return

        raw = message.text.strip()
        # Accept space or comma separated IDs
        parts = [x.strip() for x in re.split(r'[\s,]+', raw) if x.strip()]

        if not parts:
            bot.send_message(message.chat.id, "⚠️ No IDs found. Try again.")
            return

        status = bot.send_message(
            message.chat.id,
            f"♻️ Processing {len(parts)} ID(s)..."
        )

        results = []
        registered = 0
        skipped = 0
        failed = 0

        for part in parts:
            try:
                group_id = int(part)
            except ValueError:
                results.append(f"❌ `{part}` — not a valid integer ID")
                failed += 1
                continue

            try:
                # Check if already registered
                already = r.sismember('groups', str(group_id))

                # Force-add to groups set
                r.sadd('groups', str(group_id))
                _invalidate_groups_cache()

                # Restart repeat thread if repeat was configured for this group
                try:
                    if r.get(f'repeat_task:{group_id}') == 'True':
                        start_repeat_thread(group_id)
                        repeat_note = " 🔁 repeat task restarted"
                    else:
                        repeat_note = ""
                except Exception:
                    repeat_note = ""

                # Try to fetch group title for confirmation
                try:
                    chat = bot.get_chat(group_id)
                    title = chat.title or f"Group {group_id}"
                    r.set(f'cache_group_title:{group_id}', title, ex=600)
                except Exception:
                    title = r.get(f'cache_group_title:{group_id}') or f"Group {group_id}"

                if already:
                    results.append(
                        f"✅ `{group_id}` — already registered, refreshed\n"
                        f"   📌 {title}{repeat_note}"
                    )
                    skipped += 1
                else:
                    results.append(
                        f"✅ `{group_id}` — registered successfully\n"
                        f"   📌 {title}{repeat_note}"
                    )
                    registered += 1

            except Exception as e:
                results.append(f"❌ `{part}` — error: {str(e)[:80]}")
                failed += 1
                logger.error(f"[RECOVER] Failed to register group {part}: {e}")

        summary = (
            f"♻️ *Recovery Complete*\n\n"
            f"✅ Newly registered: {registered}\n"
            f"🔄 Already existed (refreshed): {skipped}\n"
            f"❌ Failed: {failed}\n\n"
        )
        summary += "\n".join(results)

        try:
            bot.edit_message_text(
                summary,
                message.chat.id,
                status.message_id,
                parse_mode='Markdown'
            )
        except Exception:
            # If too long, send as new message
            try:
                bot.send_message(message.chat.id, summary, parse_mode='Markdown')
            except Exception:
                bot.send_message(
                    message.chat.id,
                    summary.replace('`', '').replace('*', '')
                )

        logger.info(
            f"[RECOVER] Recovery done: {registered} new, {skipped} refreshed, {failed} failed."
        )

    except Exception as e:
        logger.error(f"[RECOVER] _process_recover_ids top-level error: {e}")
        try:
            bot.send_message(message.chat.id, f"❌ Recovery failed unexpectedly:\n{e}")
        except Exception:
            pass


def _process_restore_file(message):
    """Step handler: receives the JSON file from the owner for restore."""
    uid = message.from_user.id if message.from_user else None
    if uid != OWNER_ID:
        return

    # Check if user cancelled via /cancel (already cleared from restore_mode)
    with _restore_mode_lock:
        still_active = _restore_mode.get(uid, False)
    if not still_active:
        return

    # Handle /cancel typed as text instead of command
    if message.text and message.text.strip().lower() in ['/cancel', 'cancel']:
        with _restore_mode_lock:
            _restore_mode.pop(uid, None)
        bot.send_message(message.chat.id, "❌ Restore cancelled. Bot running normally.")
        return

    if not message.document:
        bot.send_message(
            message.chat.id,
            "⚠️ That's not a file. Please send the backup `.json` file or type /cancel to abort."
        )
        bot.register_next_step_handler(message, _process_restore_file)
        return

    status = bot.send_message(message.chat.id, "📥 File received → parsing JSON...")

    def _edit(text):
        try:
            bot.edit_message_text(text, message.chat.id, status.message_id)
        except Exception:
            pass

    try:
        file_info = bot.get_file(message.document.file_id)
        file_bytes = bot.download_file(file_info.file_path)
    except Exception as e:
        _edit(f"❌ Failed to download file:\n{e}")
        logger.error(f"[RESTORE] File download failed: {e}")
        with _restore_mode_lock:
            _restore_mode.pop(uid, None)
        return

    try:
        data = json.loads(file_bytes.decode('utf-8'))
    except Exception as e:
        _edit(f"❌ Invalid JSON file:\n{e}\n\nPlease check the file and try again.")
        logger.error(f"[RESTORE] JSON parse failed: {e}")
        with _restore_mode_lock:
            _restore_mode.pop(uid, None)
        return

    # Validate structure
    if 'meta' not in data or 'scalars' not in data:
        _edit("❌ This file doesn't look like a valid bot backup. Restore aborted.")
        with _restore_mode_lock:
            _restore_mode.pop(uid, None)
        return

    _edit("🔧 Restoring Redis keys...\n[████░░░░░░] 40%")
    time.sleep(0.3)

    restored = 0
    errors = 0

    try:
        # ── Restore scalars ───────────────────────────────────────────────────
        scalars = data.get('scalars', {})
        if scalars:
            pipe = r.pipeline()
            for k, v in scalars.items():
                if v is not None:
                    pipe.set(k, v)
            pipe.execute()
            restored += len([v for v in scalars.values() if v is not None])

        _edit(f"🔧 Restoring sets... ({restored} done)\n[██████░░░░] 60%")
        time.sleep(0.2)

        # ── Restore sets ──────────────────────────────────────────────────────
        sets = data.get('sets', {})
        for k, members in sets.items():
            try:
                pipe = r.pipeline()
                pipe.delete(k)
                if members:
                    pipe.sadd(k, *members)
                pipe.execute()
                restored += 1
            except Exception as e:
                logger.error(f"[RESTORE] Set restore failed for {k}: {e}")
                errors += 1

        _edit(f"🔧 Restoring hashes... ({restored} done)\n[███████░░░] 70%")
        time.sleep(0.2)

        # ── Restore hashes ────────────────────────────────────────────────────
        hashes = data.get('hashes', {})
        for k, mapping in hashes.items():
            try:
                if mapping:
                    pipe = r.pipeline()
                    pipe.delete(k)
                    pipe.hset(k, mapping=mapping)
                    pipe.execute()
                    restored += 1
            except Exception as e:
                logger.error(f"[RESTORE] Hash restore failed for {k}: {e}")
                errors += 1

        _edit(f"🔧 Restoring lists & pattern keys... ({restored} done)\n[████████░░] 80%")
        time.sleep(0.2)

        # ── Restore lists ─────────────────────────────────────────────────────
        lists = data.get('lists', {})
        for k, items in lists.items():
            try:
                pipe = r.pipeline()
                pipe.delete(k)
                if items:
                    pipe.rpush(k, *items)
                pipe.execute()
                restored += 1
            except Exception as e:
                logger.error(f"[RESTORE] List restore failed for {k}: {e}")
                errors += 1

        # ── Restore pattern scalars ───────────────────────────────────────────
        pattern_scalars = data.get('pattern_scalars', {})
        if pattern_scalars:
            pipe = r.pipeline()
            for k, v in pattern_scalars.items():
                if v is not None:
                    pipe.set(k, v)
            pipe.execute()
            restored += len([v for v in pattern_scalars.values() if v is not None])

    except Exception as e:
        _edit(f"❌ Restore failed during key write:\n{e}")
        logger.error(f"[RESTORE] Key write error: {e}")
        with _restore_mode_lock:
            _restore_mode.pop(uid, None)
        return

    # ── Record restore metadata so Group Health menu can show it ─────────────
    try:
        restore_ts = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')
        backup_ts = data.get('meta', {}).get('timestamp', 'unknown')[:16].replace('T', ' ') + ' UTC'
        r.set('last_restore_time', restore_ts)
        r.set('last_restore_backup_ts', backup_ts)
    except Exception as e:
        logger.error(f"[RESTORE] Failed to record restore metadata: {e}")

    # Invalidate all in-memory caches so the restored data takes effect
    try:
        _invalidate_groups_cache()
        with _global_config_cache_lock:
            _global_config_cache.clear()
        with _group_repeat_cache_lock:
            _group_repeat_cache.clear()
    except Exception as e:
        logger.error(f"[RESTORE] Cache invalidation error: {e}")

    with _restore_mode_lock:
        _restore_mode.pop(uid, None)

    _edit(
        f"✅ Restore complete!\n"
        f"[██████████] 100%\n\n"
        f"📦 Backup date: {backup_ts}\n"
        f"🔑 Keys restored: {restored}\n"
        f"{'⚠️ Errors: ' + str(errors) if errors else '✅ No errors'}\n\n"
        f"✅ Backup successfully uploaded to Redis.\n"
        f"🤖 Bot continuing and running normally."
    )
    logger.info(f"[RESTORE] Restore complete: {restored} keys restored, {errors} errors.")


@bot.callback_query_handler(func=lambda call: call.data == 'restore_cancel')
def restore_cancel_callback(call):
    if call.from_user.id != OWNER_ID:
        return
    try:
        with _restore_mode_lock:
            _restore_mode.pop(call.from_user.id, None)
        bot.answer_callback_query(call.id, "❌ Restore cancelled.")
        try:
            bot.edit_message_text(
                "❌ Restore cancelled. Bot running normally.",
                call.message.chat.id, call.message.message_id
            )
        except Exception:
            pass
    except Exception as e:
        logger.error(f"[RESTORE] restore_cancel_callback error: {e}")


@bot.message_handler(
    func=lambda m: (
        m.chat.type in ['group', 'supergroup'] and
        m.text is not None and
        re.match(r'^/start(@\w+)?(\s|$)', m.text.strip(), re.IGNORECASE)
    )
)
def group_start_command(message):
    chat_id = message.chat.id
    global_enabled = r.get('global_group_start_reply_enabled') == 'True'
    global_reply = r.get('global_group_start_reply')
    independent = r.get(f'group_start_reply_independent:{chat_id}') == 'True'
    group_reply = r.get(f'group_start_reply:{chat_id}')

    if independent and group_reply:
        safe_send_nowait(chat_id, group_reply)
        return
    if global_enabled and global_reply:
        safe_send_nowait(chat_id, global_reply)
        return
    if group_reply:
        safe_send_nowait(chat_id, group_reply)


# ─────────────────────────────────────────────────────────────────────────────
#  BOT DETECTION & AUTO-KICK SYSTEM
# ─────────────────────────────────────────────────────────────────────────────

def _botdet_get_title(chat_id):
    """Group title for menus — cache first, live API fallback on miss."""
    cached = r.get(f'cache_group_title:{chat_id}')
    if cached:
        return cached
    try:
        chat  = bot.get_chat(chat_id)
        title = chat.title or f'Group {chat_id}'
        r.set(f'cache_group_title:{chat_id}', title, ex=3600)
        return title
    except Exception:
        return f'Group {chat_id}'

def _botdet_is_enabled_global():
    return r.get('bot_kick_enabled') == 'True'

def _botdet_is_enabled_group(chat_id):
    val = r.get(f'bot_kick_enabled:{chat_id}')
    if val is not None:
        return val == 'True'
    return _botdet_is_enabled_global()

def _botdet_is_whitelisted(bot_id_or_username):
    wl = r.smembers('bot_kick_whitelist')
    return str(bot_id_or_username).lower() in {w.lower() for w in wl}

def _botdet_log_kick(chat_id, target_id, target_username, reason='auto'):
    try:
        entry = json.dumps({
            'ts':        int(time.time()),
            'chat_id':   chat_id,
            'target_id': target_id,
            'username':  target_username or str(target_id),
            'reason':    reason,
            'group':     r.get(f'cache_group_title:{chat_id}') or str(chat_id),
        })
        pipe = r.pipeline()
        pipe.rpush('bot_kick_log', entry)
        pipe.ltrim('bot_kick_log', -100, -1)
        pipe.incr('bot_kick_count')
        pipe.incr(f'bot_kick_count:{chat_id}')
        pipe.execute()
    except Exception as e:
        logger.error(f'[BOTDET] Log kick failed: {e}')

def _botdet_check_permission(chat_id):
    """
    Returns ('kick', None), ('no_admin', reason), ('no_perm', reason), or ('error', reason).
    Uses cached _BOT_ID — no get_me() API call.

    Key fix: in basic groups (type='group'), can_restrict_members may be absent
    from the API response even though the bot has full admin rights. We default
    to True when the field is missing so we attempt the kick rather than silently
    refusing. If the actual kick fails, we surface the real error to the owner.
    """
    try:
        member = bot.get_chat_member(chat_id, _BOT_ID)
        if member.status == 'creator':
            return ('kick', None)
        if member.status == 'administrator':
            # getattr returns None when the field is absent (basic groups),
            # not False — so check explicitly for False only.
            can_restrict = getattr(member, 'can_restrict_members', None)
            if can_restrict is False:
                return ('no_perm', 'Bot is admin but "Restrict Members" permission is OFF.')
            # True OR None (absent/basic group) → attempt the kick
            return ('kick', None)
        return ('no_admin', 'Bot is not an admin in this group.')
    except Exception as e:
        return ('error', str(e)[:200])

def _botdet_kick_bot(chat_id, target_id, target_username):
    """
    Silently kick a bot: ban + immediate unban.
    Tries ban_chat_member first (Telegram API 5.3+), falls back to
    kick_chat_member (older pyTelegramBotAPI) for full version compatibility.
    Returns (True, None) on success, (False, error_str) on failure.
    """
    try:
        try:
            bot.ban_chat_member(chat_id, target_id)
        except AttributeError:
            bot.kick_chat_member(chat_id, target_id)
        bot.unban_chat_member(chat_id, target_id)
        _botdet_log_kick(chat_id, target_id, target_username)
        logger.info(f'[BOTDET] Kicked @{target_username} ({target_id}) from {chat_id}')
        return True, None
    except telebot.apihelper.ApiTelegramException as e:
        err = str(e)
        logger.error(f'[BOTDET] Kick failed for {target_id} in {chat_id}: {err}')
        return False, err[:200]
    except Exception as e:
        err = str(e)
        logger.error(f'[BOTDET] Kick error for {target_id} in {chat_id}: {err}')
        return False, err[:200]

def _botdet_handle_new_bot(chat_id, target_user):
    """
    Called when a bot is detected joining a group.
    Runs in a background thread — never blocks webhook.
    Notification policy:
      - Kicked successfully    → always notify owner
      - Kick failed            → always notify owner (no throttle — each failure is actionable)
      - No permission          → notify once per group per 6 hours
      - Whitelisted / disabled → silent
    """
    try:
        target_id       = target_user.id
        target_username = target_user.username or str(target_id)

        if target_id == _BOT_ID:
            return

        # Whitelisted — silent skip
        if _botdet_is_whitelisted(target_id) or _botdet_is_whitelisted(f'@{target_username}'):
            logger.info(f'[BOTDET] @{target_username} is whitelisted — skipping.')
            return

        # Disabled for this group — silent skip
        if not _botdet_is_enabled_group(chat_id):
            logger.info(f'[BOTDET] Auto-kick disabled for {chat_id} — skipping @{target_username}.')
            return

        perm, reason = _botdet_check_permission(chat_id)
        group_title   = _botdet_get_title(chat_id)

        if perm == 'kick':
            success, err = _botdet_kick_bot(chat_id, target_id, target_username)
            if success:
                try:
                    bot.send_message(
                        OWNER_ID,
                        f'✅ *Bot Kicked!*\n\n'
                        f'👾 @{target_username} (`{target_id}`)\n'
                        f'📌 {group_title} (`{chat_id}`)\n'
                        f'🔇 Silently removed.',
                        parse_mode='Markdown'
                    )
                except Exception:
                    pass
            else:
                # Always notify on kick failure — every failure is actionable
                r.set(f'bot_kick_no_perm:{chat_id}', 'kick_failed', ex=86400)
                try:
                    bot.send_message(
                        OWNER_ID,
                        f'❌ *Bot Kick Failed!*\n\n'
                        f'👾 @{target_username} (`{target_id}`)\n'
                        f'📌 {group_title} (`{chat_id}`)\n'
                        f'⚠️ Telegram error: `{err}`\n\n'
                        f'_Check bot admin permissions in this group._',
                        parse_mode='Markdown'
                    )
                except Exception:
                    pass
        else:
            # no_admin, no_perm, or error — all need a permission fix
            r.set(f'bot_kick_no_perm:{chat_id}', perm, ex=86400)
            notif_key = f'bot_kick_notif_sent:{chat_id}'
            if not r.exists(notif_key):
                r.set(notif_key, '1', ex=21600)
                if perm == 'no_admin':
                    action = '➡️ Promote bot to admin with *Restrict Members* permission.'
                elif perm == 'no_perm':
                    action = '➡️ Grant bot the *Restrict Members* permission in group admin settings.'
                else:
                    action = f'➡️ Permission check error: `{reason}`'
                try:
                    bot.send_message(
                        OWNER_ID,
                        f'⚠️ *Bot Detected — Cannot Kick*\n\n'
                        f'👾 @{target_username} (`{target_id}`)\n'
                        f'📌 {group_title} (`{chat_id}`)\n\n'
                        f'❌ {reason}\n'
                        f'{action}\n\n'
                        f'_(Won\'t repeat for 6 hours per group)_',
                        parse_mode='Markdown'
                    )
                except Exception:
                    pass
    except Exception as e:
        logger.error(f'[BOTDET] _botdet_handle_new_bot error: {e}')


def _botdet_scan_group(chat_id):
    """
    Scan all admins of a group for bots and kick them.
    NOTE: Only admin bots are visible via the Bot API.
    Returns (kicked, skipped_whitelist, skipped_noperm, errors, admin_bots_found, error_detail).
    """
    kicked = skipped_wl = skipped_np = errors = admin_bots_found = 0
    error_detail = None
    try:
        perm, perm_reason = _botdet_check_permission(chat_id)
        try:
            admins = bot.get_chat_administrators(chat_id)
        except Exception as e:
            error_detail = str(e)[:200]
            logger.error(f'[BOTDET] Scan get_admins failed {chat_id}: {e}')
            return 0, 0, 0, 1, 0, error_detail
        for admin in admins:
            user = admin.user
            if not user.is_bot:
                continue
            if user.id == _BOT_ID:
                continue
            admin_bots_found += 1
            uname = user.username or str(user.id)
            if _botdet_is_whitelisted(user.id) or _botdet_is_whitelisted(f'@{uname}'):
                skipped_wl += 1
                continue
            if perm != 'kick':
                skipped_np += 1
                continue
            ok, kerr = _botdet_kick_bot(chat_id, user.id, uname)
            if ok:
                kicked += 1
            else:
                errors += 1
                error_detail = kerr
            time.sleep(0.3)
    except Exception as e:
        error_detail = str(e)[:200]
        logger.error(f'[BOTDET] Scan error {chat_id}: {e}')
        errors += 1
    return kicked, skipped_wl, skipped_np, errors, admin_bots_found, error_detail

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
        types.InlineKeyboardButton("📩 'Added to Group' Message", callback_data="added_to_group_menu"),
        types.InlineKeyboardButton("📡 Updates & Group Health", callback_data="updates_menu"),
        types.InlineKeyboardButton("🔗 Global Broadcast Embedded Links", callback_data="broadcast_embedded_links"),
        types.InlineKeyboardButton("🤖 Auto Approve Welcome (AAWM)", callback_data="aawm_menu"),
        types.InlineKeyboardButton("📝 Create Post", callback_data="create_post_menu"),
        types.InlineKeyboardButton("🚫 Ban User", callback_data="ban_user_menu"),
        types.InlineKeyboardButton("🤖 Bot Detection & Auto-Kick", callback_data="bot_detection_menu"),
    )
    if message_id:
        try:
            bot.edit_message_text(text, chat_id, message_id, reply_markup=markup)
        except Exception:
            bot.send_message(chat_id, text, reply_markup=markup)
    else:
        bot.send_message(chat_id, text, reply_markup=markup)


def _back_markup(callback_data):
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data=callback_data))
    return markup


# ─── Universal Inline Button Builder ─────────────────────────────────────────
def build_inline_keyboard(buttons):
    """
    buttons: list of rows, each row is a list of (text, url) tuples.
    e.g. [[("Rules", "https://..."), ("Site", "https://...")], [("Community", "https://...")]]
    Supports up to 5 buttons total.
    """
    keyboard = types.InlineKeyboardMarkup()
    for row in buttons:
        keyboard.row(*[types.InlineKeyboardButton(text, url=url) for text, url in row])
    return keyboard

def _auto_layout_buttons(flat_buttons):
    """
    Given a flat list of (text, url) tuples (max 5),
    returns rows laid out 2-per-row with last row holding the remainder.
    """
    rows = []
    for i in range(0, len(flat_buttons), 2):
        rows.append(flat_buttons[i:i+2])
    return rows

# ─── Inline Button Collection Flow ───────────────────────────────────────────
# Temporary storage key: inline_btns:{user_id}  → JSON list of {text, url}
# Used by: broadcast, single message, AAWM

def _get_pending_buttons(user_id):
    raw = r.get(f'inline_btns:{user_id}')
    if not raw:
        return []
    return json.loads(raw)

def _set_pending_buttons(user_id, btns):
    r.set(f'inline_btns:{user_id}', json.dumps(btns), ex=600)

def _clear_pending_buttons(user_id):
    r.delete(f'inline_btns:{user_id}')

def _build_keyboard_from_pending(user_id):
    """Build InlineKeyboardMarkup from stored pending buttons for a user."""
    btns = _get_pending_buttons(user_id)
    if not btns:
        return None
    flat = [(b['text'], b['url']) for b in btns]
    rows = _auto_layout_buttons(flat)
    return build_inline_keyboard(rows)

def _ask_add_buttons(message, on_yes_cb_data, on_no_cb_data, context_label=""):
    """Send the 'Add inline buttons?' prompt after a message is composed."""
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(
        types.InlineKeyboardButton("✅ Yes", callback_data=on_yes_cb_data),
        types.InlineKeyboardButton("❌ No", callback_data=on_no_cb_data),
    )
    bot.send_message(
        message.chat.id,
        f"🔘 Do you want to add inline buttons?{' (' + context_label + ')' if context_label else ''}\n"
        f"Up to 5 buttons supported.",
        reply_markup=markup,
        disable_web_page_preview=True
    )


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
        except telebot.apihelper.ApiTelegramException as e:
            if 'message is not modified' in str(e).lower():
                pass  # same content — not an error
            else:
                try:
                    bot.send_message(cid, text, reply_markup=markup, parse_mode=parse_mode,
                                     disable_web_page_preview=True)
                except Exception:
                    pass
        except Exception:
            try:
                bot.send_message(cid, text, reply_markup=markup, parse_mode=parse_mode,
                                 disable_web_page_preview=True)
            except Exception:
                pass

    def answer(text="", alert=False):
        try:
            bot.answer_callback_query(call.id, text, show_alert=alert)
        except Exception:
            pass

    def _reload(new_data):
        """Re-render a menu in-place by faking a callback with new data."""
        fake = types.CallbackQuery(
            id=call.id, from_user=call.from_user,
            message=call.message, data=new_data,
            chat_instance=getattr(call, 'chat_instance', '')
        )
        callback(fake)

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
        enabled = r.get('global_group_start_reply_enabled') == 'True'
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.row(
            types.InlineKeyboardButton("✅ ON (Override All)", callback_data="global_group_start_reply_on"),
            types.InlineKeyboardButton("❌ OFF", callback_data="global_group_start_reply_off"),
        )
        markup.add(
            types.InlineKeyboardButton("✏️ Set / Edit Reply", callback_data="do_set_global_group_start_reply"),
            types.InlineKeyboardButton("🗑 Remove Reply", callback_data="reset_global_group_start_reply"),
            types.InlineKeyboardButton("🔙 Go Back", callback_data="back"),
        )
        status_text = "✅ ON (overrides all groups)" if enabled else "❌ OFF"
        menu_text = (
            f"📝 Global /start@ Group Reply\n\n"
            f"Status: {status_text}\n\n"
            f"Current Reply:\n{current}\n\n"
            f"ℹ️ When ON, this overrides all group-specific /start@ replies.\n"
            f"Groups you set individually will run independently.\n"
            f"Turning OFF then ON again resets all groups back under global control."
        )
        try:
            bot.edit_message_text(menu_text, cid, mid, reply_markup=markup)
        except Exception:
            try:
                bot.send_message(cid, menu_text, reply_markup=markup)
            except Exception:
                pass
        answer()

    elif data == "global_group_start_reply_on":
        for g in get_groups():
            r.delete(f'group_start_reply_independent:{g}')
        r.set('global_group_start_reply_enabled', 'True')
        current = r.get('global_group_start_reply') or "Not set"
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.row(
            types.InlineKeyboardButton("✅ ON (Override All)", callback_data="global_group_start_reply_on"),
            types.InlineKeyboardButton("❌ OFF", callback_data="global_group_start_reply_off"),
        )
        markup.add(
            types.InlineKeyboardButton("✏️ Set / Edit Reply", callback_data="do_set_global_group_start_reply"),
            types.InlineKeyboardButton("🗑 Remove Reply", callback_data="reset_global_group_start_reply"),
            types.InlineKeyboardButton("🔙 Go Back", callback_data="back"),
        )
        menu_text = (
            f"📝 Global /start@ Group Reply\n\n"
            f"Status: ✅ ON (overrides all groups)\n\n"
            f"Current Reply:\n{current}\n\n"
            f"ℹ️ When ON, this overrides all group-specific /start@ replies.\n"
            f"Groups you set individually will run independently.\n"
            f"Turning OFF then ON again resets all groups back under global control."
        )
        try:
            bot.edit_message_text(menu_text, cid, mid, reply_markup=markup)
        except Exception:
            bot.send_message(cid, menu_text, reply_markup=markup)
        answer("✅ Global /start@ reply is ON — overrides all groups.")

    elif data == "global_group_start_reply_off":
        r.set('global_group_start_reply_enabled', 'False')
        current = r.get('global_group_start_reply') or "Not set"
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.row(
            types.InlineKeyboardButton("✅ ON (Override All)", callback_data="global_group_start_reply_on"),
            types.InlineKeyboardButton("❌ OFF", callback_data="global_group_start_reply_off"),
        )
        markup.add(
            types.InlineKeyboardButton("✏️ Set / Edit Reply", callback_data="do_set_global_group_start_reply"),
            types.InlineKeyboardButton("🗑 Remove Reply", callback_data="reset_global_group_start_reply"),
            types.InlineKeyboardButton("🔙 Go Back", callback_data="back"),
        )
        menu_text = (
            f"📝 Global /start@ Group Reply\n\n"
            f"Status: ❌ OFF\n\n"
            f"Current Reply:\n{current}\n\n"
            f"ℹ️ When ON, this overrides all group-specific /start@ replies.\n"
            f"Groups you set individually will run independently.\n"
            f"Turning OFF then ON again resets all groups back under global control."
        )
        try:
            bot.edit_message_text(menu_text, cid, mid, reply_markup=markup)
        except Exception:
            bot.send_message(cid, menu_text, reply_markup=markup)
        answer("✅ Global /start@ reply is OFF.")

    elif data == "do_set_global_group_start_reply":
        edit("✏️ Send the new global reply for /start@AllMusicShazamandlyrics_bot in groups:")
        bot.register_next_step_handler(call.message, process_global_group_start_reply)
        answer()

    elif data == "reset_global_group_start_reply":
        r.delete('global_group_start_reply')
        r.set('global_group_start_reply_enabled', 'False')
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.row(
            types.InlineKeyboardButton("✅ ON (Override All)", callback_data="global_group_start_reply_on"),
            types.InlineKeyboardButton("❌ OFF", callback_data="global_group_start_reply_off"),
        )
        markup.add(
            types.InlineKeyboardButton("✏️ Set / Edit Reply", callback_data="do_set_global_group_start_reply"),
            types.InlineKeyboardButton("🗑 Remove Reply", callback_data="reset_global_group_start_reply"),
            types.InlineKeyboardButton("🔙 Go Back", callback_data="back"),
        )
        menu_text = (
            f"📝 Global /start@ Group Reply\n\n"
            f"Status: ❌ OFF\n\n"
            f"Current Reply:\nNot set\n\n"
            f"ℹ️ When ON, this overrides all group-specific /start@ replies.\n"
            f"Groups you set individually will run independently.\n"
            f"Turning OFF then ON again resets all groups back under global control."
        )
        try:
            bot.edit_message_text(menu_text, cid, mid, reply_markup=markup)
        except Exception:
            bot.send_message(cid, menu_text, reply_markup=markup)
        answer("✅ Global group /start@ reply removed and turned OFF.")

    # ── BROADCAST TO ALL GROUPS ───────────────────────────────────────────────
    elif data == "broadcast_all":
        _clear_pending_buttons(OWNER_ID)
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.row(
            types.InlineKeyboardButton("✅ Yes — Add Buttons", callback_data="ibtn_add:broadcast_all"),
            types.InlineKeyboardButton("❌ No Buttons", callback_data="ibtn_skip:broadcast_all"),
        )
        edit("📢 Do you want to add inline buttons to this broadcast?", markup)
        answer()

    # ── TOGGLE GLOBAL LINK-ONLY ───────────────────────────────────────────────
    elif data == "toggle_global":
        current = r.get('link_only_global') == 'True'
        r.set('link_only_global', 'False' if current else 'True')
        _invalidate_global_cache('link_only_global')
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
                get_group_info(g, force_refresh=True)
            except telebot.apihelper.ApiTelegramException as e:
                if "chat not found" in str(e).lower() or "forbidden" in str(e).lower():
                    remove_group(g)
                    removed += 1
        answer(f"✅ Refreshed. Removed {removed} invalid groups.")
        _reload("my_groups")

    # ── GROUP MENU ────────────────────────────────────────────────────────────
    elif data.startswith("group_menu:"):
        _, chat_id_str = data.split(":", 1)
        chat_id = int(chat_id_str)
        title, status = get_group_info(chat_id)
        is_admin = status == "Admin"
        link_only_this = is_link_only(chat_id)
        repeat_on = r.get(f'repeat_task:{chat_id}') == 'True'
        join_reply_on = r.get(f'join_reply_enabled:{chat_id}') == 'True'

        # Live permission fetch (no cache)
        PERM_LABELS = [
            ('can_manage_chat',       'Manage Chat'),
            ('can_change_info',       'Change Info'),
            ('can_delete_messages',   'Delete Messages'),
            ('can_restrict_members',  'Ban Users'),
            ('can_invite_users',      'Invite Users'),
            ('can_pin_messages',      'Pin Messages'),
            ('can_manage_video_chats','Manage Voice Chats'),
            ('can_promote_members',   'Add Admins'),
            ('can_post_stories',      'Post Stories'),
            ('can_edit_stories',      'Edit Stories'),
            ('can_delete_stories',    'Delete Stories'),
        ]
        try:
            me = bot.get_chat_member(chat_id, bot.get_me().id)
            if me.status == 'creator':
                perms_lines = [f"🟢 {label}" for _, label in PERM_LABELS]
                perms_block = "\n".join(perms_lines)
            elif me.status == 'administrator':
                perms_lines = [
                    f"{'🟢' if getattr(me, k, False) else '❌'} {label}"
                    for k, label in PERM_LABELS
                ]
                perms_block = "\n".join(perms_lines)
            else:
                perms_block = "❌ Bot is not an administrator in this group."
        except Exception:
            perms_block = "⚠️ Could not fetch permissions."

        text = (
            f"👥 *Group:* {title}\n"
            f"🤖 *Bot Status:* {status}\n"
            f"🔗 *Link-only:* {'✅ ON' if link_only_this else '❌ OFF'}\n"
            f"🔁 *Repeating:* {'✅ ON' if repeat_on else '❌ OFF'}\n"
            f"👋 *Join Reply:* {'✅ ON' if join_reply_on else '❌ OFF'}\n\n"
            f"🔐 *Bot Permissions:*\n{perms_block}\n\n"
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
        markup.add(types.InlineKeyboardButton("ℹ️ Group Info", callback_data=f"group_info:{chat_id}"))
        markup.add(types.InlineKeyboardButton("🚫 Ban User", callback_data=f"ban_user_in_group:{chat_id}"))
        markup.add(types.InlineKeyboardButton("🚪 Leave Group", callback_data=f"leave_group_confirm:{chat_id}"))
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
        _invalidate_group_config_cache(chat_id)
        answer("✅ Self-delete removed.")
        _reload(f"setup_repeat:{chat_id}")

    elif data.startswith("repeat_on:"):
        _, chat_id_str = data.split(":", 1)
        chat_id = int(chat_id_str)
        if not r.get(f'repeat_text:{chat_id}'):
            answer("⚠️ Set a repeat message first!", alert=True)
            return
        r.set(f'repeat_task:{chat_id}', 'True')
        _invalidate_group_config_cache(chat_id)
        start_repeat_thread(chat_id)
        answer("✅ Repeating ON")
        _reload(f"setup_repeat:{chat_id}")

    elif data.startswith("repeat_off:"):
        _, chat_id_str = data.split(":", 1)
        chat_id = int(chat_id_str)
        r.set(f'repeat_task:{chat_id}', 'False')
        _invalidate_group_config_cache(chat_id)
        # Remove from active threads dict so thread exits on next config check
        with _repeat_thread_lock:
            active_repeat_threads.pop(chat_id, None)
        answer("✅ Repeating OFF")
        _reload(f"setup_repeat:{chat_id}")

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
        _invalidate_group_config_cache(chat_id)
        answer(f"🗑 Auto-delete prev now {'❌ OFF' if current else '✅ ON'}")
        _reload(f"setup_repeat:{chat_id}")

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
        _reload(f"group_menu:{chat_id}")

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
        _reload(f"group_menu:{chat_id}")

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
        _clear_pending_buttons(OWNER_ID)
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.row(
            types.InlineKeyboardButton("✅ Yes — Add Buttons", callback_data=f"ibtn_add:group:{chat_id}"),
            types.InlineKeyboardButton("❌ No Buttons", callback_data=f"ibtn_skip:group:{chat_id}"),
        )
        edit("📨 Do you want to add inline buttons to this message?", markup)
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
        _reload(f"group_menu:{chat_id}")

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
        _reload(f"set_group_start_reply:{chat_id}")

    # ── GLOBAL JOIN REPLY ─────────────────────────────────────────────────────
    elif data == "global_join_reply_menu":
        enabled     = r.get('global_join_reply_enabled') == 'True'
        autodelete  = r.get('global_join_reply_autodelete') == 'True'
        current_text = r.get('global_join_reply_text') or "Welcome!"
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.row(
            types.InlineKeyboardButton("✅ ON", callback_data="global_join_reply_on"),
            types.InlineKeyboardButton("❌ OFF", callback_data="global_join_reply_off")
        )
        markup.add(types.InlineKeyboardButton("✏️ Set / Edit Reply", callback_data="set_global_join_reply"))
        markup.add(types.InlineKeyboardButton("🗑 Reset to Default", callback_data="reset_global_join_reply"))
        markup.add(types.InlineKeyboardButton(
            f"🗑 Delete Previous: {'✅ ON' if autodelete else '❌ OFF'}",
            callback_data="toggle_global_join_autodelete"
        ))
        markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data="back"))
        edit(
            f"👋 *Global Join Reply*\n\n"
            f"Status: {'✅ ON' if enabled else '❌ OFF'}\n"
            f"Delete previous reply: {'✅ ON' if autodelete else '❌ OFF'}\n"
            f"Message:\n_{current_text}_",
            markup, parse_mode='Markdown'
        )
        answer()

    elif data == "global_join_reply_on":
        r.set('global_join_reply_enabled', 'True')
        answer("✅ Global join reply ON")
        _reload("global_join_reply_menu")

    elif data == "global_join_reply_off":
        r.set('global_join_reply_enabled', 'False')
        answer("✅ Global join reply OFF")
        _reload("global_join_reply_menu")

    elif data == "set_global_join_reply":
        edit("✏️ Send the new global join reply message:")
        bot.register_next_step_handler(call.message, process_global_join_reply)
        answer()

    elif data == "reset_global_join_reply":
        r.set('global_join_reply_text', 'Welcome!')
        answer("✅ Reset to default: 'Welcome!'", alert=True)
        _reload("global_join_reply_menu")

    elif data == "toggle_global_join_autodelete":
        current = r.get('global_join_reply_autodelete') == 'True'
        r.set('global_join_reply_autodelete', 'False' if current else 'True')
        answer(f"🗑 Delete previous join reply: {'❌ OFF' if current else '✅ ON'}")
        _reload("global_join_reply_menu")

    # ── GROUP JOIN REPLY ──────────────────────────────────────────────────────
    elif data.startswith("group_join_reply:"):
        _, chat_id_str = data.split(":", 1)
        chat_id = int(chat_id_str)
        enabled      = r.get(f'join_reply_enabled:{chat_id}') == 'True'
        current_text = r.get(f'join_reply_text:{chat_id}') or "Not set (uses global)"
        # Per-group autodelete: if not set, show global fallback state
        group_ad_raw = r.get(f'join_reply_autodelete:{chat_id}')
        global_ad    = r.get('global_join_reply_autodelete') == 'True'
        if group_ad_raw is not None:
            autodelete     = group_ad_raw == 'True'
            autodelete_src = ""
        else:
            autodelete     = global_ad
            autodelete_src = " (global)"
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.row(
            types.InlineKeyboardButton("✅ ON", callback_data=f"group_join_on:{chat_id}"),
            types.InlineKeyboardButton("❌ OFF", callback_data=f"group_join_off:{chat_id}")
        )
        markup.add(types.InlineKeyboardButton("✏️ Set / Edit Reply", callback_data=f"set_group_join_reply:{chat_id}"))
        markup.add(types.InlineKeyboardButton("🗑 Reset Reply", callback_data=f"reset_group_join_reply:{chat_id}"))
        markup.add(types.InlineKeyboardButton(
            f"🗑 Delete Previous: {'✅ ON' if autodelete else '❌ OFF'}{autodelete_src}",
            callback_data=f"toggle_group_join_autodelete:{chat_id}"
        ))
        markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data=f"group_menu:{chat_id}"))
        edit(
            f"👋 *Group Join Reply*\n\n"
            f"Status: {'✅ ON' if enabled else '❌ OFF'}\n"
            f"Delete previous reply: {'✅ ON' if autodelete else '❌ OFF'}{autodelete_src}\n"
            f"Message:\n_{current_text}_",
            markup, parse_mode='Markdown'
        )
        answer()

    elif data.startswith("group_join_on:"):
        _, chat_id_str = data.split(":", 1)
        chat_id = int(chat_id_str)
        r.set(f'join_reply_enabled:{chat_id}', 'True')
        answer("✅ Group join reply ON")
        _reload(f"group_join_reply:{chat_id}")

    elif data.startswith("group_join_off:"):
        _, chat_id_str = data.split(":", 1)
        chat_id = int(chat_id_str)
        r.set(f'join_reply_enabled:{chat_id}', 'False')
        answer("✅ Group join reply OFF")
        _reload(f"group_join_reply:{chat_id}")

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
        _reload(f"group_join_reply:{chat_id}")

    elif data.startswith("toggle_group_join_autodelete:"):
        _, chat_id_str = data.split(":", 1)
        chat_id = int(chat_id_str)
        current_raw = r.get(f'join_reply_autodelete:{chat_id}')
        # If not set yet, first toggle sets it explicitly (opposite of current effective state)
        if current_raw is None:
            global_ad = r.get('global_join_reply_autodelete') == 'True'
            new_val = 'False' if global_ad else 'True'
        else:
            new_val = 'False' if current_raw == 'True' else 'True'
        r.set(f'join_reply_autodelete:{chat_id}', new_val)
        answer(f"🗑 Delete previous join reply: {'✅ ON' if new_val == 'True' else '❌ OFF'}")
        _reload(f"group_join_reply:{chat_id}")

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
    elif data.startswith("add_to_group:") and data.endswith(":admin"):
        parts = data.split(":")
        chat_id = int(parts[1])
        title, _ = get_group_info(chat_id)
        bot_perms = get_bot_admin_permissions(chat_id)
        perm_map = {
            'can_manage_chat': 'Manage Chat',
            'can_change_info': 'Change Info',
            'can_delete_messages': 'Delete Messages',
            'can_restrict_members': 'Restrict Members',
            'can_invite_users': 'Invite Users',
            'can_pin_messages': 'Pin Messages',
            'can_manage_video_chats': 'Manage Video Chats',
            'can_promote_members': 'Add New Admins',
            'can_post_stories': 'Post Stories',
            'can_edit_stories': 'Edit Stories',
            'can_delete_stories': 'Delete Stories',
        }
        perm_lines = [f"  ✅ {label}" for key, label in perm_map.items() if bot_perms.get(key)]
        perms_info = "\n".join(perm_lines) if perm_lines else "  ⚠️ Bot has no grantable permissions"
        edit(
            f"👑 *Promote to Admin in:* {title}\n\n"
            f"Send the user ID(s) of people *already in the group*.\n"
            f"Separate multiple IDs with spaces or commas.\n\n"
            f"📋 *Permissions the bot can grant:*\n{perms_info}\n\n"
            f"⚠️ Only works if the user is already a member.",
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
        _reload("global_repeat_menu")

    elif data == "global_repeat_on":
        if not r.get('global_repeat_text'):
            answer("⚠️ Set a repeat message first!", alert=True)
            return
        r.set('global_repeat_task', 'True')
        _invalidate_global_cache('global_repeat_task')
        reset_global_repeat_schedule()  # Clear schedules so all groups fire immediately
        start_global_repeat_thread()
        answer("✅ Global repeat ON")
        _reload("global_repeat_menu")

    elif data == "global_repeat_off":
        stop_global_repeat()
        _invalidate_global_cache('global_repeat_task')
        answer("✅ Global repeat OFF")
        _reload("global_repeat_menu")

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
        _invalidate_global_cache('global_repeat_autodelete')
        answer(f"🗑 Global auto-delete prev now {'❌ OFF' if current else '✅ ON'}")
        _reload("global_repeat_menu")

    elif data == "set_global_self_delete":
        edit("💣 Send self-delete delay in seconds for global repeat (e.g. 30):")
        bot.register_next_step_handler(call.message, process_global_self_delete)
        answer()

    elif data == "remove_global_self_delete":
        r.delete('global_repeat_self_delete')
        _invalidate_global_cache('global_repeat_self_delete')
        answer("✅ Global self-delete removed.")
        _reload("global_repeat_menu")

    # ── LEAVE GROUP ───────────────────────────────────────────────────────────
    elif data.startswith("leave_group_confirm:"):
        lv_chat_id = int(data.split(":", 1)[1])
        lv_title, _ = get_group_info(lv_chat_id)
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.row(
            types.InlineKeyboardButton("✅ Confirm Leave", callback_data=f"leave_group_do:{lv_chat_id}"),
            types.InlineKeyboardButton("❌ Cancel", callback_data=f"group_menu:{lv_chat_id}"),
        )
        edit(f"⚠️ Are you sure you want the bot to leave *{lv_title}*?\n\nThis will stop all repeat tasks and remove the group from all records.", markup, parse_mode='Markdown')
        answer()

    elif data.startswith("leave_group_do:"):
        lv_chat_id = int(data.split(":", 1)[1])
        r.set(f'repeat_task:{lv_chat_id}', 'False')
        _invalidate_group_config_cache(lv_chat_id)
        with _repeat_thread_lock:
            active_repeat_threads.pop(lv_chat_id, None)
        remove_group(lv_chat_id)
        try:
            bot.leave_chat(lv_chat_id)
        except Exception as e:
            logger.error(f"Failed to leave group {lv_chat_id}: {e}")
        edit(f"✅ Bot has left the group and it has been removed from all records.", _back_markup("my_groups"))
        answer("✅ Left group.")

    # ── GROUP INFO ────────────────────────────────────────────────────────────
    elif data.startswith("group_info:"):
        gi_chat_id = int(data.split(":", 1)[1])
        try:
            gi_chat = bot.get_chat(gi_chat_id)
            gi_title = gi_chat.title or f"Group {gi_chat_id}"
            gi_link = gi_chat.invite_link or "N/A"
            try:
                gi_count = bot.get_chat_member_count(gi_chat_id)
            except Exception:
                gi_count = "N/A"
            try:
                gi_me = bot.get_chat_member(gi_chat_id, bot.get_me().id)
                gi_bot_role = "Admin" if gi_me.status in ['administrator', 'creator'] else "Member"
            except Exception:
                gi_bot_role = "Unknown"
            # Fetch admins live (no caching)
            try:
                gi_admins = bot.get_chat_administrators(gi_chat_id)
                gi_admin_lines = []
                gi_owner = "Unknown"
                for a in gi_admins:
                    u = a.user
                    name = u.full_name
                    username = f" @{u.username}" if u.username else ""
                    if a.status == 'creator':
                        gi_owner = f"{name}{username}"
                        gi_admin_lines.append(f"👑 {name}{username} (Owner)")
                    else:
                        gi_admin_lines.append(f"🛡 {name}{username}")
                admins_str = "\n".join(gi_admin_lines) if gi_admin_lines else "None visible"
            except Exception as e:
                gi_owner = "N/A"
                admins_str = f"Could not fetch ({e})"
            info_text = (
                f"ℹ️ <b>Group Info</b>\n\n"
                f"📌 <b>Name:</b> {gi_title}\n"
                f"🆔 <b>ID:</b> <code>{gi_chat_id}</code>\n"
                f"🔗 <b>Link:</b> {gi_link}\n"
                f"👑 <b>Owner:</b> {gi_owner}\n"
                f"👥 <b>Members:</b> {gi_count}\n"
                f"🤖 <b>Bot Role:</b> {gi_bot_role}\n\n"
                f"<b>Admins:</b>\n{admins_str}"
            )
        except Exception as e:
            info_text = f"❌ Could not fetch group info:\n{e}"
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data=f"group_menu:{gi_chat_id}"))
        try:
            bot.edit_message_text(info_text, cid, mid, reply_markup=markup, parse_mode='HTML')
        except Exception:
            bot.send_message(cid, info_text, reply_markup=markup, parse_mode='HTML')
        answer()

    # ── SUSPICIOUS GROUP: LEAVE / STAY ───────────────────────────────────────
    elif data.startswith("sus_leave:"):
        sus_chat_id = int(data.split(":", 1)[1])
        r.set(f'repeat_task:{sus_chat_id}', 'False')
        _invalidate_group_config_cache(sus_chat_id)
        with _repeat_thread_lock:
            active_repeat_threads.pop(sus_chat_id, None)
        remove_group(sus_chat_id)
        try:
            bot.leave_chat(sus_chat_id)
        except Exception as e:
            logger.error(f"Failed to leave suspicious group {sus_chat_id}: {e}")
        edit(f"✅ Bot has left the suspicious group (`{sus_chat_id}`) and it has been removed from all records.", _back_markup("back"), parse_mode='Markdown')
        answer("✅ Left suspicious group.")

    elif data.startswith("sus_stay:"):
        sus_chat_id = int(data.split(":", 1)[1])
        edit(f"🟢 Staying in group `{sus_chat_id}`. It remains registered normally.", _back_markup("back"), parse_mode='Markdown')
        answer("🟢 Staying in group.")

    # ── UPDATES & GROUP HEALTH ────────────────────────────────────────────────
    elif data in ["updates_menu", "updates_refresh"]:
        groups = get_groups()
        total = len(groups)
        working = []
        api_errors = []
        perm_errors = []
        recently_removed = list(r.smembers('recently_removed_groups'))

        for g in groups:
            err = r.get(f'group_error:{g}')
            if err:
                err_low = err.lower()
                if 'forbidden' in err_low or 'kicked' in err_low or 'not a member' in err_low or '403' in err_low:
                    perm_errors.append((g, err))
                else:
                    api_errors.append((g, err))
            else:
                working.append(g)

        error_set = r.smembers('groups_with_errors')
        for g_str in error_set:
            try:
                g = int(g_str)
            except Exception:
                continue
            if g not in [x[0] for x in api_errors] and g not in [x[0] for x in perm_errors]:
                err = r.get(f'group_error:{g}') or 'Unknown error'
                api_errors.append((g, err))
                if g in working:
                    working.remove(g)

        # Monitoring stats
        with _repeat_thread_lock:
            active_repeats = sum(1 for t in active_repeat_threads.values() if t.is_alive())
        with _flood_wait_lock:
            flood_count = _flood_wait_counter
        try:
            proc = psutil.Process()
            mem_mb = proc.memory_info().rss / 1024 / 1024
            mem_str = f"{mem_mb:.1f} MB"
        except Exception:
            mem_str = "N/A"
        try:
            r.ping()
            redis_ok = "✅ Connected"
        except Exception:
            redis_ok = "❌ ERROR"

        lines = ["📡 <b>Group Health &amp; Monitor Report</b>\n"]
        lines.append(f"📊 Total groups: {total}")
        lines.append(f"✅ Working normally: {len(working)}")
        lines.append(f"❌ API / send errors: {len(api_errors)}")
        lines.append(f"🚫 Permission / access errors: {len(perm_errors)}")
        lines.append(f"🗑 Recently removed: {len(recently_removed)}")

        with _send_queue_lock:
            q_depth = len(_send_queue)
        lines.append(f"📬 Send queue depth: {q_depth} pending")

        lines.append(f"\n🖥 <b>Monitor Stats</b>")
        lines.append(f"🔁 Active repeat tasks: {active_repeats}")
        lines.append(f"⚡ FloodWait hits (session): {flood_count}")
        lines.append(f"💾 Memory usage: {mem_str}")
        lines.append(f"🗄 Redis: {redis_ok}")

        # ── Restore status banner ─────────────────────────────────────────────
        try:
            last_restore = r.get('last_restore_time')
            last_restore_bk = r.get('last_restore_backup_ts')
            if last_restore:
                lines.append(
                    f"\n🔁 <b>Last Restore:</b> {last_restore}\n"
                    f"   📦 Backup used: {last_restore_bk or 'unknown'}\n"
                    f"   ✅ Bot is running on restored backup data"
                )
        except Exception:
            pass

        # Show per-group cooldowns
        with _group_rate_lock:
            now_ts = time.time()
            cooling_groups = [(cid, int(until - now_ts)) for cid, until in _group_cooldown_until.items() if until > now_ts]
        if cooling_groups:
            lines.append(f"\n🌡 Groups in 429 cooldown: {len(cooling_groups)}")
            for cid, secs in cooling_groups[:5]:
                title = r.get(f'cache_group_title:{cid}') or f"Group {cid}"
                lines.append(f"  • {title}: {secs}s remaining")
        else:
            lines.append("✅ No groups in rate-limit cooldown")

        # --- Advanced per-group error display with expandable blockquote ---
        all_error_groups = list(dict.fromkeys([g for g, _ in api_errors] + [g for g, _ in perm_errors]))
        if all_error_groups:
            lines.append("\n⚠️ <b>Per-Group Errors:</b>")
            for g in all_error_groups[:10]:
                g_title = r.get(f'cache_group_title:{g}') or f"Group {g}"
                # Collect errors: redis error + runtime errors
                error_lines = []
                redis_err = r.get(f'group_error:{g}')
                if redis_err:
                    error_lines.append(redis_err[:100])
                with _runtime_errors_lock:
                    rt_errs = _runtime_errors.get(g, [])
                for e_line in rt_errs[-5:]:
                    if e_line[:100] not in error_lines:
                        error_lines.append(e_line[:100])
                errors_text = "\n".join(error_lines) if error_lines else "Unknown error"
                lines.append(f"\n<b>Group: {g_title}</b>")
                lines.append(f"<blockquote expandable>{errors_text}</blockquote>")
            if len(all_error_groups) > 10:
                lines.append(f"...and {len(all_error_groups) - 10} more groups with errors")

        if recently_removed:
            lines.append("\n🗑 Recently removed groups:")
            for g_str in list(recently_removed)[:5]:
                title = r.get(f'cache_group_title:{g_str}') or f"Group {g_str}"
                lines.append(f"  • {title}")
            if len(recently_removed) > 5:
                lines.append(f"  ...and {len(recently_removed) - 5} more")

        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(
            types.InlineKeyboardButton("🔄 Refresh", callback_data="updates_refresh"),
            types.InlineKeyboardButton("🧹 Clear Error Log", callback_data="updates_clear_errors"),
            types.InlineKeyboardButton("🔙 Go Back", callback_data="back"),
        )
        report_text = "\n".join(lines)
        # Always edit in place — never send a new message (prevents duplication on refresh)
        try:
            bot.edit_message_text(report_text, cid, mid, reply_markup=markup, parse_mode='HTML')
        except telebot.apihelper.ApiTelegramException as e:
            if 'message is not modified' in str(e).lower():
                pass  # content unchanged — not an error
            else:
                # If message is too long, truncate and edit
                try:
                    bot.edit_message_text(report_text[:4000] + "\n...truncated", cid, mid, reply_markup=markup, parse_mode='HTML')
                except Exception:
                    pass
        except Exception:
            pass
        answer("🔄 Refreshed" if data == "updates_refresh" else "")

    elif data == "updates_clear_errors":
        error_groups = list(r.smembers('groups_with_errors'))
        for g_str in error_groups:
            r.delete(f'group_error:{g_str}')
        r.delete('groups_with_errors')
        r.delete('recently_removed_groups')
        answer("✅ Error log cleared.", alert=True)
        try:
            bot.edit_message_text(
                "📡 Group Health Report\n\nError log cleared. Press Refresh to re-scan.",
                cid, mid,
                reply_markup=types.InlineKeyboardMarkup().add(
                    types.InlineKeyboardButton("🔄 Refresh", callback_data="updates_refresh"),
                    types.InlineKeyboardButton("🔙 Go Back", callback_data="back")
                )
            )
        except Exception:
            pass

    # ── ADDED TO GROUP MESSAGE ────────────────────────────────────────────────
    elif data == "added_to_group_menu":
        enabled = r.get('added_to_group_msg_enabled') == 'True'
        current_msg = r.get('added_to_group_msg') or "Not set"
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.row(
            types.InlineKeyboardButton("✅ ON", callback_data="added_to_group_on"),
            types.InlineKeyboardButton("❌ OFF", callback_data="added_to_group_off"),
        )
        markup.add(
            types.InlineKeyboardButton("✏️ Set / Edit Message", callback_data="set_added_to_group_msg"),
            types.InlineKeyboardButton("🔄 Reset to Default", callback_data="reset_added_to_group_msg"),
            types.InlineKeyboardButton("🔙 Go Back", callback_data="back"),
        )
        preview = current_msg[:200] + ("..." if len(current_msg) > 200 else "")
        edit(
            f"📩 *'Added to Group' Reply Message*\n\n"
            f"Status: {'✅ ON' if enabled else '❌ OFF'}\n\n"
            f"Current Message:\n`{preview}`\n\n"
            f"ℹ️ This is what the bot sends in the group chat when it is added.\n"
            f"Different from your private notification.",
            markup, parse_mode='Markdown'
        )
        answer()

    elif data == "added_to_group_on":
        r.set('added_to_group_msg_enabled', 'True')
        answer("✅ 'Added to Group' message is now ON.", alert=True)
        _reload("added_to_group_menu")

    elif data == "added_to_group_off":
        r.set('added_to_group_msg_enabled', 'False')
        answer("✅ 'Added to Group' message is now OFF.", alert=True)
        _reload("added_to_group_menu")

    elif data == "set_added_to_group_msg":
        edit(
            "✏️ Send the new message for when the bot is added to a group.\n\n"
            "You can use plain text or links.\n"
            "For embedded links use Markdown like: [Click here](https://example.com)"
        )
        bot.register_next_step_handler(call.message, process_added_to_group_msg)
        answer()

    elif data == "reset_added_to_group_msg":
        default = ('https://t.me/AllMusicShazamandlyrics_bot?startgroup=true&admin='
                   'change_info+delete_messages+restrict_members+invite_users+'
                   'pin_messages+manage_video_chats+anonymous+manage_chat+'
                   'post_stories+edit_stories+delete_stories')
        r.set('added_to_group_msg', default)
        answer("✅ Reset to default invite link.", alert=True)
        _reload("added_to_group_menu")

    # ── GLOBAL BROADCAST EMBEDDED LINKS ──────────────────────────────────────
    elif data == "broadcast_embedded_links":
        edit(
            "🔗 *Global Broadcast Embedded Links*\n\n"
            "Send the message text you want to broadcast.\n"
            "After that you'll be able to embed links into specific words.",
            _back_markup("back"), parse_mode='Markdown'
        )
        bot.register_next_step_handler(call.message, process_embedded_links_text)
        answer()

    elif data.startswith("embedded_confirm_broadcast:"):
        key = data.split(":", 1)[1]
        html_text = r.get(f'embedded_draft:{key}')
        if not html_text:
            answer("❌ Session expired. Please start over.", alert=True)
            return
        r.delete(f'embedded_draft:{key}')
        groups = get_groups()
        if not groups:
            edit("❌ No groups to broadcast to.", _back_markup("back"))
            answer()
            return
        bot.send_message(
            cid,
            f"📢 Broadcasting embedded links message to {len(groups)} groups...\n"
            f"⏱ Estimated time: ~{int(len(groups) * _INTER_MSG_DELAY)}s\n"
            f"📡 You'll get a report when done.",
            reply_markup=_back_markup("back")
        )
        def _do_embedded_broadcast(text=html_text, grps=groups):
            sent_count = 0
            failed_count = 0
            for group in grps:
                try:
                    sent = bot.send_message(group, text, parse_mode='HTML')
                    if sent:
                        r.set(f'last_sent:{group}', str(sent.message_id))
                        save_last_sent(group, sent.message_id)
                        sent_count += 1
                    else:
                        failed_count += 1
                except Exception as e:
                    _log_runtime_error(group, 'embedded_broadcast', str(e)[:200])
                    failed_count += 1
                time.sleep(_INTER_MSG_DELAY)
            try:
                bot.send_message(
                    OWNER_ID,
                    f"✅ Embedded broadcast complete!\n"
                    f"👥 Sent to: {sent_count} groups\n"
                    f"❌ Failed: {failed_count}",
                    reply_markup=_back_markup("back")
                )
            except Exception:
                pass
        threading.Thread(target=_do_embedded_broadcast, daemon=True).start()
        answer()

    elif data.startswith("embedded_cancel:"):
        key = data.split(":", 1)[1]
        r.delete(f'embedded_draft:{key}')
        edit("❌ Broadcast cancelled.", _back_markup("back"))
        answer("Cancelled.")

    # ── BROADCAST TO BOT USERS ────────────────────────────────────────────────
    elif data == "broadcast_users":
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(
            types.InlineKeyboardButton("📣 Send Broadcast", callback_data="do_broadcast_users"),
            types.InlineKeyboardButton("📌 Send & Pin", callback_data="do_broadcast_users_pin"),
            types.InlineKeyboardButton("🔘 Send Message With Button", callback_data="do_broadcast_users_with_button"),
            types.InlineKeyboardButton("🔙 Go Back", callback_data="back"),
        )
        edit("📣 *Broadcast to Bot Users*\n\nSend a message to everyone who has ever started the bot.", markup, parse_mode='Markdown')
        answer()

    elif data in ["do_broadcast_users", "do_broadcast_users_pin"]:
        pin = data == "do_broadcast_users_pin"
        edit(f"✏️ Send the message to broadcast to all bot users{' (will also be pinned)' if pin else ''}:")
        bot.register_next_step_handler(call.message, lambda m: process_broadcast_users(m, pin))
        answer()

    elif data == "do_broadcast_users_with_button":
        edit("✏️ Send the broadcast message text:")
        bot.register_next_step_handler(call.message, process_broadcast_users_btn_text)
        answer()


    elif data.startswith("add_inline_btn:"):
        btn_key = data.split(":", 1)[1]
        edit("✏️ Send the Button Text (label):")
        bot.register_next_step_handler(call.message, lambda m: process_broadcast_users_btn_label(m, btn_key))
        answer()

    elif data.startswith("no_inline_btn:"):
        btn_key = data.split(":", 1)[1]
        btext = r.get(f'btn_broadcast_text:{btn_key}')
        r.delete(f'btn_broadcast_text:{btn_key}')
        if not btext:
            answer("❌ Session expired.", alert=True)
            return
        users = get_all_users()
        bot.send_message(cid, f"📣 Broadcasting to {len(users)} users (no button)...", reply_markup=_back_markup("back"))
        def _do_no_btn(text=btext, ulist=users):
            sent_count = 0
            failed_count = 0
            for uid in ulist:
                sent = safe_send(uid, text)
                if sent:
                    save_private_sent(uid, sent.message_id)
                    sent_count += 1
                else:
                    failed_count += 1
            try:
                bot.send_message(OWNER_ID, f"✅ Broadcast done!\n✅ Sent: {sent_count}\n❌ Failed: {failed_count}", reply_markup=_back_markup("back"))
            except Exception:
                pass
        threading.Thread(target=_do_no_btn, daemon=True).start()
        answer()

    elif data.startswith("confirm_broadcast_btn:"):
        btn_key = data.split(":", 1)[1]
        btext = r.get(f'btn_broadcast_text:{btn_key}')
        bbtn_text = r.get(f'btn_broadcast_btn_text:{btn_key}')
        bbtn_url = r.get(f'btn_broadcast_btn_url:{btn_key}')
        if not btext or not bbtn_text or not bbtn_url:
            answer("❌ Session expired. Please start over.", alert=True)
            return
        r.delete(f'btn_broadcast_text:{btn_key}')
        r.delete(f'btn_broadcast_btn_text:{btn_key}')
        r.delete(f'btn_broadcast_btn_url:{btn_key}')
        users = get_all_users()
        bot.send_message(cid, f"📣 Broadcasting to {len(users)} users...", reply_markup=_back_markup("back"))
        def _do_btn_broadcast(text=btext, btn_t=bbtn_text, btn_u=bbtn_url, ulist=users):
            sent_count = 0
            failed_count = 0
            for uid in ulist:
                try:
                    btn_markup = types.InlineKeyboardMarkup()
                    btn_markup.add(types.InlineKeyboardButton(btn_t, url=btn_u))
                    sent = safe_send(uid, text, reply_markup=btn_markup, priority=3)
                    if sent:
                        save_private_sent(uid, sent.message_id)
                        sent_count += 1
                    else:
                        failed_count += 1
                except Exception:
                    failed_count += 1
            try:
                bot.send_message(OWNER_ID, f"✅ Button broadcast done!\n✅ Sent: {sent_count}\n❌ Failed: {failed_count}", reply_markup=_back_markup("back"))
            except Exception:
                pass
        threading.Thread(target=_do_btn_broadcast, daemon=True).start()
        answer()

    elif data.startswith("cancel_broadcast_btn:"):
        btn_key = data.split(":", 1)[1]
        r.delete(f'btn_broadcast_text:{btn_key}')
        r.delete(f'btn_broadcast_btn_text:{btn_key}')
        r.delete(f'btn_broadcast_btn_url:{btn_key}')
        edit("❌ Broadcast with button cancelled.", _back_markup("back"))
        answer("Cancelled.")


    # ── AAWM MENU ─────────────────────────────────────────────────────────────
    elif data == "aawm_menu":
        enabled = r.get('aawm_enabled') == 'True'
        current_text = r.get('aawm_text') or "Welcome to the group! Please read the rules."
        btns_raw = r.get('aawm_buttons_global')
        btns_count = len(json.loads(btns_raw)) if btns_raw else 0
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.row(
            types.InlineKeyboardButton("✅ ON (Global)", callback_data="aawm_on"),
            types.InlineKeyboardButton("❌ OFF", callback_data="aawm_off"),
        )
        markup.add(
            types.InlineKeyboardButton("✏️ Set Welcome Text", callback_data="aawm_set_text"),
            types.InlineKeyboardButton(f"🔘 Manage Buttons ({btns_count}/5)", callback_data="aawm_manage_buttons"),
            types.InlineKeyboardButton("🔙 Go Back", callback_data="back"),
        )
        edit(
            f"🤖 <b>Auto Approve Welcome Message (AAWM)</b>\n\n"
            f"Status: {'✅ ON' if enabled else '❌ OFF'}\n\n"
            f"Welcome Text:\n{current_text[:200]}\n\n"
            f"Inline Buttons: {btns_count} configured\n\n"
            f"When ON, the bot auto-approves join requests and sends a private welcome message.",
            markup, parse_mode='HTML'
        )
        answer()

    elif data == "aawm_on":
        r.set('aawm_enabled', 'True')
        answer("✅ AAWM is ON")
        _reload("aawm_menu")

    elif data == "aawm_off":
        r.set('aawm_enabled', 'False')
        answer("❌ AAWM is OFF")
        _reload("aawm_menu")

    elif data == "aawm_set_text":
        edit("✏️ Send the welcome message text for AAWM:")
        bot.register_next_step_handler(call.message, process_aawm_text)
        answer()

    elif data == "aawm_manage_buttons":
        btns_raw = r.get('aawm_buttons_global')
        btns = json.loads(btns_raw) if btns_raw else []
        lines = [f"🔘 <b>AAWM Inline Buttons</b> ({len(btns)}/5)\n"]
        for i, b in enumerate(btns):
            lines.append(f"{i+1}. {b['text']} → {b['url']}")
        markup = types.InlineKeyboardMarkup(row_width=1)
        if len(btns) < 5:
            markup.add(types.InlineKeyboardButton("➕ Add Button", callback_data="aawm_add_button"))
        if btns:
            markup.add(types.InlineKeyboardButton("🗑 Clear All Buttons", callback_data="aawm_clear_buttons"))
        markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data="aawm_menu"))
        edit("\n".join(lines) if len(lines) > 1 else "🔘 No buttons set yet.", markup, parse_mode='HTML')
        answer()

    elif data == "aawm_add_button":
        edit("✏️ Send the button label text:")
        bot.register_next_step_handler(call.message, process_aawm_btn_text)
        answer()

    elif data == "aawm_clear_buttons":
        r.delete('aawm_buttons_global')
        answer("🗑 All AAWM buttons cleared.", alert=True)
        _reload("aawm_manage_buttons")

    # ── INLINE BUTTON COLLECTION FLOW (for broadcast / single send) ───────────
    elif data.startswith("ibtn_add:"):
        # ibtn_add:{context_key}  — user wants to add a button
        ctx = data.split(":", 1)[1]
        edit("✏️ Send the button label text:")
        bot.register_next_step_handler(call.message, lambda m: _process_ibtn_text(m, ctx))
        answer()

    elif data.startswith("ibtn_another:"):
        ctx = data.split(":", 1)[1]
        btns = _get_pending_buttons(OWNER_ID)
        if len(btns) >= 5:
            answer("⚠️ Maximum 5 buttons reached.", alert=True)
            _reload(f"ibtn_done:{ctx}")
            return
        edit("✏️ Send the next button label text:")
        bot.register_next_step_handler(call.message, lambda m: _process_ibtn_text(m, ctx))
        answer()

    elif data.startswith("ibtn_done:"):
        ctx = data.split(":", 1)[1]
        # Context tells us where to go next
        # ctx format: "broadcast_all" | "broadcast_users" | "group:{chat_id}"
        btns = _get_pending_buttons(OWNER_ID)
        if not btns:
            answer("No buttons — sending without buttons.", alert=True)
        # Route to the actual send handler
        if ctx == "broadcast_all":
            edit("📢 Send the message to broadcast to all groups:")
            bot.register_next_step_handler(call.message, process_broadcast_all)
        elif ctx == "broadcast_users":
            edit("📣 Send the message to broadcast to all users:")
            bot.register_next_step_handler(call.message, process_broadcast_users)
        else:
            try:
                grp_id = int(ctx.replace("group:", ""))
                edit("📨 Send your message now:")
                bot.register_next_step_handler(call.message, lambda m: process_single_message(m, grp_id))
            except Exception:
                edit("📨 Send your message now:")
        answer()

    elif data.startswith("ibtn_skip:"):
        ctx = data.split(":", 1)[1]
        _clear_pending_buttons(OWNER_ID)
        if ctx == "broadcast_all":
            edit("📢 Send the message to broadcast to all groups:")
            bot.register_next_step_handler(call.message, process_broadcast_all)
        elif ctx == "broadcast_users":
            edit("📣 Send the message to broadcast to all users:")
            bot.register_next_step_handler(call.message, process_broadcast_users)
        else:
            try:
                grp_id = int(ctx.replace("group:", ""))
                edit("📨 Send your message now:")
                bot.register_next_step_handler(call.message, lambda m: process_single_message(m, grp_id))
            except Exception:
                edit("📨 Send your message now:")
        answer()

    # ── CREATE POST ───────────────────────────────────────────────────────────
    elif data == "create_post_menu":
        _clear_pending_buttons(OWNER_ID)
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(
            types.InlineKeyboardButton("📝 Write Post", callback_data="create_post_write"),
            types.InlineKeyboardButton("🔙 Go Back", callback_data="back"),
        )
        edit("📝 *Create Post*\n\nWrite your post then choose where to send it.", markup, parse_mode='Markdown')
        answer()

    elif data == "create_post_write":
        _clear_pending_buttons(OWNER_ID)
        edit("📝 Send your post message now:")
        bot.register_next_step_handler(call.message, process_create_post_text)
        answer()

    elif data.startswith("post_add_buttons:"):
        post_key = data.split(":", 1)[1]
        edit("✏️ Send the button label text:")
        bot.register_next_step_handler(call.message, lambda m: process_post_btn_text(m, post_key))
        answer()

    elif data.startswith("post_skip_buttons:"):
        post_key = data.split(":", 1)[1]
        # Show destination selector with no buttons
        _show_post_destination(cid, mid, post_key, answer)

    elif data.startswith("post_another_btn:"):
        post_key = data.split(":", 1)[1]
        btns = _get_pending_buttons(OWNER_ID)
        if len(btns) >= 5:
            answer("⚠️ Maximum 5 buttons reached.", alert=True)
            _show_post_destination(cid, mid, post_key, answer)
            return
        edit("✏️ Send the next button label text:")
        bot.register_next_step_handler(call.message, lambda m: process_post_btn_text(m, post_key))
        answer()

    elif data.startswith("post_done_buttons:"):
        post_key = data.split(":", 1)[1]
        _show_post_destination(cid, mid, post_key, answer)

    elif data.startswith("post_to_all:"):
        post_key = data.split(":", 1)[1]
        post_text = r.get(f'post_draft:{post_key}')
        if not post_text:
            answer("❌ Session expired. Please start over.", alert=True)
            return
        r.delete(f'post_draft:{post_key}')
        groups = get_groups()
        if not groups:
            edit("❌ No groups to send to.", _back_markup("create_post_menu"))
            answer()
            return
        reply_markup = _build_keyboard_from_pending(OWNER_ID)
        _clear_pending_buttons(OWNER_ID)
        bot.send_message(
            cid,
            f"📝 Sending post to {len(groups)} groups...\n"
            f"⏱ Estimated time: ~{int(len(groups) * _INTER_MSG_DELAY)}s\n"
            f"📡 You'll get a report when done.",
            reply_markup=_back_markup("back"),
            disable_web_page_preview=True
        )
        def _do_post_all(text=post_text, grps=groups, kb=reply_markup):
            sent_count = 0
            failed_count = 0
            for group in grps:
                sent = safe_send(group, text, reply_markup=kb)
                if sent:
                    r.set(f'last_sent:{group}', str(sent.message_id))
                    save_last_sent(group, sent.message_id)
                    sent_count += 1
                else:
                    failed_count += 1
            try:
                bot.send_message(
                    OWNER_ID,
                    f"✅ Post sent!\n👥 Sent to: {sent_count} groups\n❌ Failed: {failed_count}",
                    reply_markup=_back_markup("back"),
                    disable_web_page_preview=True
                )
            except Exception:
                pass
        threading.Thread(target=_do_post_all, daemon=True).start()
        answer()

    elif data.startswith("post_select_groups:"):
        post_key = data.split(":", 1)[1]
        groups = get_groups()
        if not groups:
            edit("❌ No groups available.", _back_markup("create_post_menu"))
            answer()
            return
        markup = types.InlineKeyboardMarkup(row_width=1)
        for g in groups:
            title, _ = get_group_info(g)
            markup.add(types.InlineKeyboardButton(
                f"📤 {title}", callback_data=f"post_to_one:{post_key}:{g}"
            ))
        markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data="create_post_menu"))
        edit("📤 Select a group to send the post to:", markup)
        answer()

    elif data.startswith("post_to_one:"):
        parts = data.split(":")
        post_key = parts[1]
        group_id = int(parts[2])
        post_text = r.get(f'post_draft:{post_key}')
        if not post_text:
            answer("❌ Session expired. Please start over.", alert=True)
            return
        r.delete(f'post_draft:{post_key}')
        reply_markup = _build_keyboard_from_pending(OWNER_ID)
        _clear_pending_buttons(OWNER_ID)
        sent = safe_send(group_id, post_text, reply_markup=reply_markup)
        if sent:
            r.set(f'last_sent:{group_id}', str(sent.message_id))
            save_last_sent(group_id, sent.message_id)
            edit("✅ Post sent to group!", _back_markup("create_post_menu"))
        else:
            edit("❌ Failed to send post.", _back_markup("create_post_menu"))
        answer()

    # ── BOT DETECTION MENU ────────────────────────────────────────────────────
    elif data in ['bot_detection_menu', 'bot_detection_refresh']:
        global_on    = _botdet_is_enabled_global()
        total_kicked = int(r.get('bot_kick_count') or 0)
        log_size     = r.llen('bot_kick_log')
        wl_size      = r.scard('bot_kick_whitelist')
        groups       = get_groups()
        perm_ok = perm_no = perm_member = 0
        for g in groups:
            np = r.get(f'bot_kick_no_perm:{g}')
            if np == 'no_admin':
                perm_member += 1
            elif np in ('no_perm', 'kick_failed'):
                perm_no += 1
            else:
                perm_ok += 1

        status_icon = "✅" if global_on else "❌"
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.row(
            types.InlineKeyboardButton("✅ Enable Global", callback_data="botdet_global_on"),
            types.InlineKeyboardButton("❌ Disable Global", callback_data="botdet_global_off"),
        )
        markup.add(types.InlineKeyboardButton(f"⚪ Whitelist ({wl_size} bots)", callback_data="botdet_whitelist"))
        markup.add(types.InlineKeyboardButton(f"📋 Kick Log ({log_size} entries)", callback_data="botdet_log:0"))
        markup.add(types.InlineKeyboardButton("📊 Per-Group Status", callback_data="botdet_groups"))
        markup.add(types.InlineKeyboardButton("🔍 Scan a Group Now", callback_data="botdet_scan_pick"))
        markup.add(types.InlineKeyboardButton("🔄 Refresh", callback_data="botdet_groups_refresh" if data == 'bot_detection_refresh' else "bot_detection_refresh"))
        markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data="back"))
        edit(
            f"🤖 *Bot Detection & Auto-Kick*\n\n"
            f"Global Status: {status_icon} {'ON' if global_on else 'OFF'}\n"
            f"Total bots kicked (all time): *{total_kicked}*\n\n"
            f"📊 *Group Permission Status:*\n"
            f"✅ Can kick: {perm_ok}\n"
            f"⚠️ Admin but no restrict perm: {perm_no}\n"
            f"🔒 Bot is just a member: {perm_member}\n\n"
            f"⚠️ *Telegram API Limitation:*\n"
            f"Only bots that are *admins* can be detected in an existing scan.\n"
            f"Regular member bots already in the group are *invisible* to the Bot API — "
            f"they can only be kicked the moment they join or rejoin.\n\n"
            f"_Requires admin + Restrict Members permission to kick._",
            markup, parse_mode='Markdown'
        )
        answer()

    elif data == 'botdet_global_on':
        r.set('bot_kick_enabled', 'True')
        answer("✅ Bot auto-kick enabled globally.")
        _reload('bot_detection_menu')

    elif data == 'botdet_global_off':
        r.set('bot_kick_enabled', 'False')
        answer("❌ Bot auto-kick disabled globally.")
        _reload('bot_detection_menu')

    # ── WHITELIST ─────────────────────────────────────────────────────────────
    elif data == 'botdet_whitelist':
        wl = sorted(r.smembers('bot_kick_whitelist'))
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(types.InlineKeyboardButton("➕ Add Bot to Whitelist", callback_data="botdet_wl_add"))
        if wl:
            markup.add(types.InlineKeyboardButton("🗑 Clear Entire Whitelist", callback_data="botdet_wl_clear"))
        for entry in wl[:20]:
            markup.add(types.InlineKeyboardButton(f"❌ Remove: {entry}", callback_data=f"botdet_wl_remove:{entry}"))
        markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data="bot_detection_menu"))
        lines = ["⚪ *Bot Whitelist*\n", "Bots in this list will never be kicked.\n"]
        if wl:
            for e in wl:
                lines.append(f"• `{e}`")
        else:
            lines.append("_No bots whitelisted yet._")
        edit("\n".join(lines), markup, parse_mode='Markdown')
        answer()

    elif data == 'botdet_wl_add':
        edit("✏️ Send the bot's @username or numeric ID to whitelist:\n\nExamples: `@GroupAnonymousBot` or `123456789`")
        bot.register_next_step_handler(call.message, _process_botdet_wl_add)
        answer()

    elif data.startswith('botdet_wl_remove:'):
        entry = data.split(':', 1)[1]
        r.srem('bot_kick_whitelist', entry)
        answer(f"✅ Removed {entry} from whitelist.", alert=True)
        _reload('botdet_whitelist')

    elif data == 'botdet_wl_clear':
        r.delete('bot_kick_whitelist')
        answer("🗑 Whitelist cleared.", alert=True)
        _reload('botdet_whitelist')

    # ── KICK LOG ──────────────────────────────────────────────────────────────
    elif data.startswith('botdet_log:'):
        try:
            page = int(data.split(':', 1)[1])
        except Exception:
            page = 0
        per_page = 10
        all_entries = r.lrange('bot_kick_log', 0, -1)
        all_entries.reverse()   # newest first
        total = len(all_entries)
        start = page * per_page
        chunk = all_entries[start:start + per_page]

        lines = [f"📋 *Kick Log* ({total} total)\n"]
        if not chunk:
            lines.append("_No kicks recorded yet._")
        for raw in chunk:
            try:
                e = json.loads(raw)
                ts  = time.strftime('%m-%d %H:%M', time.gmtime(e.get('ts', 0)))
                grp = e.get('group', e.get('chat_id', '?'))
                usr = e.get('username', '?')
                lines.append(f"🕐 {ts} | 👾 @{usr} | 📌 {grp}")
            except Exception:
                pass

        markup = types.InlineKeyboardMarkup(row_width=2)
        nav_btns = []
        if page > 0:
            nav_btns.append(types.InlineKeyboardButton("◀️ Prev", callback_data=f"botdet_log:{page-1}"))
        if start + per_page < total:
            nav_btns.append(types.InlineKeyboardButton("Next ▶️", callback_data=f"botdet_log:{page+1}"))
        if nav_btns:
            markup.row(*nav_btns)
        if total > 0:
            markup.add(types.InlineKeyboardButton("🗑 Clear Log", callback_data="botdet_log_clear"))
        markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data="bot_detection_menu"))
        edit("\n".join(lines), markup, parse_mode='Markdown')
        answer()

    elif data == 'botdet_log_clear':
        r.delete('bot_kick_log')
        r.set('bot_kick_count', 0)
        answer("🗑 Kick log cleared.", alert=True)
        _reload('botdet_log:0')

    # ── PER-GROUP STATUS ──────────────────────────────────────────────────────
    elif data in ['botdet_groups', 'botdet_groups_refresh']:
        groups = get_groups()
        markup = types.InlineKeyboardMarkup(row_width=1)
        lines  = ["📊 *Per-Group Bot Detection Status*\n"]
        for g in groups[:30]:
            title      = _botdet_get_title(g)
            group_raw  = r.get(f'bot_kick_enabled:{g}')
            global_on  = _botdet_is_enabled_global()
            if group_raw is not None:
                status_icon = "✅" if group_raw == 'True' else "❌"
                status_src  = ""
            else:
                status_icon = "✅" if global_on else "❌"
                status_src  = " (global)"
            np = r.get(f'bot_kick_no_perm:{g}')
            if np == 'no_admin':
                perm_icon = "🔒"
            elif np in ('no_perm', 'kick_failed'):
                perm_icon = "⚠️"
            else:
                perm_icon = "✅"
            kicks = r.get(f'bot_kick_count:{g}') or '0'
            lines.append(f"{perm_icon} {status_icon} *{title[:30]}*{status_src} — {kicks} kicked")
            markup.add(types.InlineKeyboardButton(
                f"{'🔴 Disable' if (group_raw or ('True' if global_on else 'False')) == 'True' else '🟢 Enable'}: {title[:25]}",
                callback_data=f"botdet_toggle_group:{g}"
            ))
        if len(groups) > 30:
            lines.append(f"\n_...and {len(groups)-30} more groups_")
        lines.append(
            f"\n\n🔑 *Legend:*\n"
            f"✅ = can kick | ⚠️ = admin, no restrict perm | 🔒 = member only\n\n"
            f"⚠️ *Scan only detects admin bots.*\n"
            f"Member bots already present are invisible to the Bot API.\n"
            f"They will be caught automatically when they next join or rejoin."
        )
        markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data="bot_detection_menu"))
        try:
            bot.edit_message_text("\n".join(lines), cid, mid, reply_markup=markup, parse_mode='Markdown')
        except telebot.apihelper.ApiTelegramException as e:
            if 'message is not modified' not in str(e).lower():
                try:
                    bot.edit_message_text(("\n".join(lines))[:4000], cid, mid, reply_markup=markup, parse_mode='Markdown')
                except Exception:
                    pass
        except Exception:
            pass
        answer()

    elif data.startswith('botdet_toggle_group:'):
        g = int(data.split(':', 1)[1])
        current_raw = r.get(f'bot_kick_enabled:{g}')
        global_on   = _botdet_is_enabled_global()
        current_eff = current_raw == 'True' if current_raw is not None else global_on
        r.set(f'bot_kick_enabled:{g}', 'False' if current_eff else 'True')
        answer(f"{'❌ Disabled' if current_eff else '✅ Enabled'} for this group.")
        _reload('botdet_groups')

    # ── SCAN GROUP NOW ────────────────────────────────────────────────────────
    elif data == 'botdet_scan_pick':
        groups = get_groups()
        if not groups:
            answer("❌ No groups registered.", alert=True)
            return
        markup = types.InlineKeyboardMarkup(row_width=1)
        for g in groups:
            title = _botdet_get_title(g)
            markup.add(types.InlineKeyboardButton(f"🔍 {title[:35]}", callback_data=f"botdet_scan_do:{g}"))
        markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data="bot_detection_menu"))
        edit("🔍 *Scan a Group Now*\n\nSelect a group to scan and kick all non-whitelisted bots:\n\n⚠️ Only admin bots are detectable via scan.", markup, parse_mode='Markdown')
        answer()

    elif data.startswith('botdet_scan_do:'):
        g = int(data.split(':', 1)[1])
        title = _botdet_get_title(g)
        edit(f"🔍 Scanning *{title}* for bots…\n\nThis may take a few seconds.", parse_mode='Markdown')
        answer()

        def _do_scan(cid=g, t=title):
            k, wl, np, err, found, err_detail = _botdet_scan_group(cid)
            err_line = f"\n⚠️ Error detail: `{err_detail}`" if err_detail else ""
            result = (
                f"🔍 *Scan Complete: {t}*\n\n"
                f"🔎 Admin bots checked: {found}\n"
                f"✅ Bots kicked: {k}\n"
                f"⚪ Whitelisted (skipped): {wl}\n"
                f"⚠️ No permission (skipped): {np}\n"
                f"❌ Errors: {err}{err_line}\n\n"
                f"ℹ️ *Note:* Only admin bots are visible to this scan.\n"
                f"Member bots already present cannot be detected by the Telegram Bot API — "
                f"they will be kicked automatically the next time they join or rejoin this group."
            )
            try:
                bot.edit_message_text(result, cid_owner, mid_owner, parse_mode='Markdown',
                    reply_markup=_back_markup('bot_detection_menu'))
            except Exception:
                try:
                    bot.send_message(OWNER_ID, result, parse_mode='Markdown',
                        reply_markup=_back_markup('bot_detection_menu'))
                except Exception:
                    pass

        cid_owner = cid
        mid_owner = mid
        threading.Thread(target=_do_scan, daemon=True).start()

    # ── BAN USER ──────────────────────────────────────────────────────────────
    elif data == "ban_user_menu":
        groups = get_groups()
        if not groups:
            edit("❌ No groups available.", _back_markup("back"))
            answer()
            return
        markup = types.InlineKeyboardMarkup(row_width=1)
        for g in groups:
            title, status = get_group_info(g)
            markup.add(types.InlineKeyboardButton(
                f"🚫 {title} ({status})", callback_data=f"ban_select_group:{g}"
            ))
        markup.add(types.InlineKeyboardButton("🔙 Go Back", callback_data="back"))
        edit("🚫 *Ban User*\n\nSelect the group to ban from:", markup, parse_mode='Markdown')
        answer()

    elif data.startswith("ban_user_in_group:"):
        # Called from individual group menu — skip group selection
        group_id = int(data.split(":", 1)[1])
        _reload(f"ban_select_group:{group_id}")

    elif data.startswith("ban_select_group:"):
        group_id = int(data.split(":", 1)[1])
        title, _ = get_group_info(group_id)
        # Check bot permission
        try:
            me = bot.get_chat_member(group_id, bot.get_me().id)
            can_ban = (
                me.status == 'creator' or
                (me.status == 'administrator' and getattr(me, 'can_restrict_members', False))
            )
        except Exception:
            can_ban = False
        if not can_ban:
            edit(
                f"❌ Bot does not have *can_restrict_members* permission in *{title}*.\n\n"
                f"Please grant the bot ban rights first.",
                _back_markup("ban_user_menu"), parse_mode='Markdown'
            )
            answer()
            return
        r.setex(f'ban_target_group:{OWNER_ID}', 600, str(group_id))
        edit(
            f"🚫 *Ban User in {title}*\n\n"
            f"Send the user ID to ban.\n"
            f"You can also forward a message from that user.",
            _back_markup("ban_user_menu"), parse_mode='Markdown'
        )
        bot.register_next_step_handler(call.message, process_ban_user)
        answer()

# ─────────────────────────────────────────────────────────────────────────────
#  PROCESS FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def process_added_to_group_msg(message):
    if message.from_user.id != OWNER_ID:
        return
    r.set('added_to_group_msg', message.text.strip())
    bot.send_message(
        message.chat.id,
        "✅ 'Added to Group' message updated successfully!",
        reply_markup=_back_markup("added_to_group_menu")
    )

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
        r.delete(f'group_start_reply_independent:{chat_id}')
        bot.send_message(message.chat.id, "✅ Group /start@ reply removed. This group is now back under global control.",
                         reply_markup=_back_markup(f"set_group_start_reply:{chat_id}"))
    else:
        r.set(key, text)
        r.set(f'group_start_reply_independent:{chat_id}', 'True')
        bot.send_message(message.chat.id, "✅ Group /start@ reply set. This group will now reply independently from global settings.",
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

def _process_botdet_wl_add(message):
    """Step handler: add a bot to the whitelist."""
    if message.from_user.id != OWNER_ID:
        return
    try:
        raw = message.text.strip() if message.text else ''
        if not raw or raw.lower() in ['/cancel', 'cancel']:
            bot.send_message(message.chat.id, "❌ Cancelled.",
                             reply_markup=_back_markup('botdet_whitelist'))
            return
        # Normalise: strip leading @ for storage, re-add for display
        entry = raw.lstrip('@')
        if raw.startswith('@'):
            entry = f'@{entry}'
        r.sadd('bot_kick_whitelist', entry)
        bot.send_message(
            message.chat.id,
            f"✅ `{entry}` added to whitelist.\n\nThis bot will never be auto-kicked.",
            parse_mode='Markdown',
            reply_markup=_back_markup('botdet_whitelist')
        )
    except Exception as e:
        logger.error(f"[BOTDET] _process_botdet_wl_add error: {e}")
        try:
            bot.send_message(message.chat.id, f"❌ Failed: {e}",
                             reply_markup=_back_markup('botdet_whitelist'))
        except Exception:
            pass

def process_interval(message, chat_id, unit):
    if message.from_user.id != OWNER_ID:
        return
    try:
        val = int(message.text.strip())
        if val <= 0:
            raise ValueError
        seconds = val if unit == "sec" else val * 60
        r.set(f'repeat_interval:{chat_id}', str(seconds))
        _invalidate_group_config_cache(chat_id)
        bot.send_message(message.chat.id, f"✅ Interval set to {seconds} seconds.",
                         reply_markup=_back_markup(f"setup_repeat:{chat_id}"))
    except Exception:
        bot.send_message(message.chat.id, "❌ Please send a positive number.",
                         reply_markup=_back_markup(f"setup_repeat:{chat_id}"))

def process_set_repeat_text(message, chat_id):
    if message.from_user.id != OWNER_ID:
        return
    r.set(f'repeat_text:{chat_id}', message.text)
    _invalidate_group_config_cache(chat_id)
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
        _invalidate_group_config_cache(chat_id)
        bot.send_message(message.chat.id, f"✅ Self-delete set to {val} seconds.",
                         reply_markup=_back_markup(f"setup_repeat:{chat_id}"))
    except Exception:
        bot.send_message(message.chat.id, "❌ Please send a positive number.",
                         reply_markup=_back_markup(f"setup_repeat:{chat_id}"))

def process_global_repeat_text(message):
    if message.from_user.id != OWNER_ID:
        return
    r.set('global_repeat_text', message.text)
    _invalidate_global_cache('global_repeat_text')
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
        _invalidate_global_cache('global_repeat_interval')
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
        _invalidate_global_cache('global_repeat_self_delete')
        bot.send_message(message.chat.id, f"✅ Global self-delete set to {val} seconds.",
                         reply_markup=_back_markup("global_repeat_menu"))
    except Exception:
        bot.send_message(message.chat.id, "❌ Please send a positive number.",
                         reply_markup=_back_markup("global_repeat_menu"))

# ─── AAWM Process Functions ───────────────────────────────────────────────────

def process_aawm_text(message):
    if message.from_user.id != OWNER_ID:
        return
    r.set('aawm_text', message.text.strip())
    bot.send_message(message.chat.id, "✅ AAWM welcome text set.", reply_markup=_back_markup("aawm_menu"))

def process_aawm_btn_text(message):
    if message.from_user.id != OWNER_ID:
        return
    r.set(f'aawm_btn_pending_text:{OWNER_ID}', message.text.strip(), ex=300)
    bot.send_message(message.chat.id, "🔗 Now send the button URL:",
                     disable_web_page_preview=True)
    bot.register_next_step_handler(message, process_aawm_btn_url)

def process_aawm_btn_url(message):
    if message.from_user.id != OWNER_ID:
        return
    btn_text = r.get(f'aawm_btn_pending_text:{OWNER_ID}')
    r.delete(f'aawm_btn_pending_text:{OWNER_ID}')
    if not btn_text:
        bot.send_message(message.chat.id, "❌ Session expired.", reply_markup=_back_markup("aawm_manage_buttons"))
        return
    btns_raw = r.get('aawm_buttons_global')
    btns = json.loads(btns_raw) if btns_raw else []
    btns.append({'text': btn_text, 'url': message.text.strip()})
    r.set('aawm_buttons_global', json.dumps(btns))
    bot.send_message(message.chat.id, f"✅ Button added! ({len(btns)}/5)",
                     reply_markup=_back_markup("aawm_manage_buttons"))

# ─── Inline Button Collection Process Functions ───────────────────────────────

def _process_ibtn_text(message, ctx):
    if message.from_user.id != OWNER_ID:
        return
    r.set(f'ibtn_pending_text:{OWNER_ID}', message.text.strip(), ex=300)
    bot.send_message(message.chat.id, "🔗 Send the button URL:",
                     disable_web_page_preview=True)
    bot.register_next_step_handler(message, lambda m: _process_ibtn_url(m, ctx))

def _process_ibtn_url(message, ctx):
    if message.from_user.id != OWNER_ID:
        return
    btn_text = r.get(f'ibtn_pending_text:{OWNER_ID}')
    r.delete(f'ibtn_pending_text:{OWNER_ID}')
    if not btn_text:
        bot.send_message(message.chat.id, "❌ Session expired.",
                         reply_markup=_back_markup("back"))
        return
    btns = _get_pending_buttons(OWNER_ID)
    btns.append({'text': btn_text, 'url': message.text.strip()})
    _set_pending_buttons(OWNER_ID, btns)
    markup = types.InlineKeyboardMarkup(row_width=1)
    if len(btns) < 5:
        markup.add(types.InlineKeyboardButton("➕ Add Another Button", callback_data=f"ibtn_another:{ctx}"))
    markup.add(types.InlineKeyboardButton("✅ Done — Continue", callback_data=f"ibtn_done:{ctx}"))
    bot.send_message(
        message.chat.id,
        f"✅ Button added ({len(btns)}/5): <b>{btn_text}</b>\n\nAdd another or continue?",
        parse_mode='HTML', reply_markup=markup, disable_web_page_preview=True
    )


# ─── Create Post Process Functions ───────────────────────────────────────────

def _show_post_destination(cid, mid, post_key, answer_fn):
    """Show the destination selector for a post (all groups / selected group)."""
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(
        types.InlineKeyboardButton("📢 All Groups", callback_data=f"post_to_all:{post_key}"),
        types.InlineKeyboardButton("📤 Selected Group", callback_data=f"post_select_groups:{post_key}"),
        types.InlineKeyboardButton("❌ Cancel", callback_data="create_post_menu"),
    )
    try:
        bot.edit_message_text("📤 Where do you want to send this post?", cid, mid, reply_markup=markup)
    except Exception:
        pass
    answer_fn()

def process_create_post_text(message):
    if message.from_user.id != OWNER_ID:
        return
    text = message.text
    post_key = f"{OWNER_ID}:{int(time.time())}"
    r.set(f'post_draft:{post_key}', text, ex=600)
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(
        types.InlineKeyboardButton("✅ Yes — Add Buttons", callback_data=f"post_add_buttons:{post_key}"),
        types.InlineKeyboardButton("❌ No Buttons", callback_data=f"post_skip_buttons:{post_key}"),
    )
    bot.send_message(
        message.chat.id,
        "🔘 Do you want to add inline buttons to this post?\nUp to 5 buttons supported.",
        reply_markup=markup,
        disable_web_page_preview=True
    )

def process_post_btn_text(message, post_key):
    if message.from_user.id != OWNER_ID:
        return
    r.set(f'post_btn_pending_text:{OWNER_ID}', message.text.strip(), ex=300)
    bot.send_message(message.chat.id, "🔗 Send the button URL:", disable_web_page_preview=True)
    bot.register_next_step_handler(message, lambda m: process_post_btn_url(m, post_key))

def process_post_btn_url(message, post_key):
    if message.from_user.id != OWNER_ID:
        return
    btn_text = r.get(f'post_btn_pending_text:{OWNER_ID}')
    r.delete(f'post_btn_pending_text:{OWNER_ID}')
    if not btn_text:
        bot.send_message(message.chat.id, "❌ Session expired.", reply_markup=_back_markup("create_post_menu"))
        return
    btns = _get_pending_buttons(OWNER_ID)
    btns.append({'text': btn_text, 'url': message.text.strip()})
    _set_pending_buttons(OWNER_ID, btns)
    markup = types.InlineKeyboardMarkup(row_width=1)
    if len(btns) < 5:
        markup.add(types.InlineKeyboardButton("➕ Add Another Button", callback_data=f"post_another_btn:{post_key}"))
    markup.add(types.InlineKeyboardButton("✅ Done — Choose Destination", callback_data=f"post_done_buttons:{post_key}"))
    bot.send_message(
        message.chat.id,
        f"✅ Button added ({len(btns)}/5): <b>{btn_text}</b>\n\nAdd another or choose where to send?",
        parse_mode='HTML', reply_markup=markup, disable_web_page_preview=True
    )


# ─── Ban User Process Functions ───────────────────────────────────────────────

def process_ban_user(message):
    if message.from_user.id != OWNER_ID:
        return
    group_id_str = r.get(f'ban_target_group:{OWNER_ID}')
    r.delete(f'ban_target_group:{OWNER_ID}')
    if not group_id_str:
        bot.send_message(message.chat.id, "❌ Session expired. Please start over.",
                         reply_markup=_back_markup("ban_user_menu"))
        return
    group_id = int(group_id_str)
    title, _ = get_group_info(group_id)

    # Resolve user_id — either direct ID or forwarded message
    user_id = None
    if message.forward_from:
        user_id = message.forward_from.id
    else:
        try:
            user_id = int(message.text.strip())
        except (ValueError, AttributeError):
            bot.send_message(message.chat.id,
                             "❌ Invalid user ID. Please send a numeric ID or forward a message from the user.",
                             reply_markup=_back_markup("ban_user_menu"))
            return

    # Check if target is admin — if so, try demoting first
    try:
        target_member = bot.get_chat_member(group_id, user_id)
        is_admin = target_member.status in ['administrator', 'creator']
    except Exception:
        is_admin = False

    results = []

    if is_admin:
        if target_member.status == 'creator':
            bot.send_message(
                message.chat.id,
                f"❌ Cannot ban the group creator.",
                reply_markup=_back_markup(f"ban_select_group:{group_id}")
            )
            return
        # Try to demote first
        try:
            bot.promote_chat_member(
                group_id, user_id,
                can_manage_chat=False,
                can_change_info=False,
                can_delete_messages=False,
                can_restrict_members=False,
                can_invite_users=False,
                can_pin_messages=False,
                can_manage_video_chats=False,
                can_promote_members=False,
            )
            results.append("✅ Admin rights removed.")
        except telebot.apihelper.ApiTelegramException as e:
            results.append(f"⚠️ Could not demote admin: {str(e)[:80]}")

    # Now ban
    try:
        bot.ban_chat_member(group_id, user_id)
        results.append(f"✅ User `{user_id}` has been banned from *{title}*.")
    except telebot.apihelper.ApiTelegramException as e:
        err = str(e)
        if 'CHAT_ADMIN_REQUIRED' in err:
            results.append("❌ Bot lacks permission to ban in this group.")
        elif 'USER_NOT_PARTICIPANT' in err:
            results.append(f"⚠️ User `{user_id}` is not in the group. Banning anyway to prevent re-join.")
            try:
                bot.ban_chat_member(group_id, user_id)
                results.append("✅ Banned (will block re-join).")
            except Exception as e2:
                results.append(f"❌ Ban failed: {str(e2)[:80]}")
        else:
            results.append(f"❌ Ban failed: {err[:100]}")
    except Exception as e:
        results.append(f"❌ Unexpected error: {str(e)[:100]}")

    summary = "\n".join(results)
    try:
        bot.send_message(
            message.chat.id, summary,
            parse_mode='Markdown',
            reply_markup=_back_markup(f"ban_select_group:{group_id}")
        )
    except Exception:
        bot.send_message(
            message.chat.id, summary.replace('`', '').replace('*', ''),
            reply_markup=_back_markup(f"ban_select_group:{group_id}")
        )


def process_embedded_links_text(message):
    if message.from_user.id != OWNER_ID:
        return
    text = message.text.strip()
    words = text.split()
    if not words:
        bot.send_message(message.chat.id, "❌ Empty message. Please start over.", reply_markup=_back_markup("broadcast_embedded_links"))
        return
    numbered = "\n".join(f"{i+1}. {w}" for i, w in enumerate(words))
    bot.send_message(
        message.chat.id,
        f"📝 Your message words:\n\n{numbered}\n\n"
        f"Send the word number(s) you want to embed links into.\n"
        f"Example: <code>3 5</code> (space or comma separated)",
        parse_mode='HTML',
        reply_markup=_back_markup("broadcast_embedded_links")
    )
    # Store the words temporarily
    draft_key = f"emb_words:{message.from_user.id}"
    r.set(draft_key, json.dumps(words), ex=600)
    bot.register_next_step_handler(message, process_embedded_word_numbers)

def process_embedded_word_numbers(message):
    if message.from_user.id != OWNER_ID:
        return
    draft_key = f"emb_words:{message.from_user.id}"
    raw = r.get(draft_key)
    if not raw:
        bot.send_message(message.chat.id, "❌ Session expired. Please start over.", reply_markup=_back_markup("broadcast_embedded_links"))
        return
    words = json.loads(raw)
    raw_nums = re.split(r'[\s,]+', message.text.strip())
    try:
        chosen = [int(n) for n in raw_nums if n.isdigit()]
        if not chosen or any(n < 1 or n > len(words) for n in chosen):
            raise ValueError
    except ValueError:
        bot.send_message(message.chat.id, f"❌ Invalid numbers. Send numbers between 1 and {len(words)}.", reply_markup=_back_markup("broadcast_embedded_links"))
        return
    # Store chosen indices
    r.set(f"emb_chosen:{message.from_user.id}", json.dumps(chosen), ex=600)
    chosen_words = [words[n-1] for n in chosen]
    bot.send_message(
        message.chat.id,
        f"🔗 You selected word(s): <b>{', '.join(chosen_words)}</b>\n\n"
        f"Now send the URL(s) for these words.\n"
        f"If multiple words, send one URL per line (in the same order).",
        parse_mode='HTML',
        reply_markup=_back_markup("broadcast_embedded_links")
    )
    bot.register_next_step_handler(message, process_embedded_links_urls)

def process_embedded_links_urls(message):
    if message.from_user.id != OWNER_ID:
        return
    words_raw = r.get(f"emb_words:{message.from_user.id}")
    chosen_raw = r.get(f"emb_chosen:{message.from_user.id}")
    if not words_raw or not chosen_raw:
        bot.send_message(message.chat.id, "❌ Session expired. Please start over.", reply_markup=_back_markup("broadcast_embedded_links"))
        return
    words = json.loads(words_raw)
    chosen = json.loads(chosen_raw)
    urls = [u.strip() for u in message.text.strip().splitlines() if u.strip()]
    if len(urls) == 1 and len(chosen) > 1:
        urls = urls * len(chosen)  # same URL for all chosen words
    if len(urls) != len(chosen):
        bot.send_message(message.chat.id, f"❌ You selected {len(chosen)} word(s) but sent {len(urls)} URL(s). Please match them.", reply_markup=_back_markup("broadcast_embedded_links"))
        return
    # Build HTML message
    link_map = {chosen[i]-1: urls[i] for i in range(len(chosen))}
    result_parts = []
    for i, word in enumerate(words):
        if i in link_map:
            result_parts.append(f'<a href="{link_map[i]}">{word}</a>')
        else:
            result_parts.append(word)
    html_msg = " ".join(result_parts)
    # Store draft
    draft_key = f"emb_final:{message.from_user.id}:{int(time.time())}"
    r.set(f"embedded_draft:{draft_key}", html_msg, ex=600)
    # Clean up temp keys
    r.delete(f"emb_words:{message.from_user.id}")
    r.delete(f"emb_chosen:{message.from_user.id}")
    # Send preview
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(
        types.InlineKeyboardButton("🚀 Start Broadcast", callback_data=f"embedded_confirm_broadcast:{draft_key}"),
        types.InlineKeyboardButton("❌ Cancel", callback_data=f"embedded_cancel:{draft_key}"),
    )
    bot.send_message(message.chat.id, f"👁 Preview:\n\n{html_msg}", parse_mode='HTML', reply_markup=markup)


# ─── Button Broadcast Process Functions ──────────────────────────────────────

def process_broadcast_users_btn_text(message):
    if message.from_user.id != OWNER_ID:
        return
    text = message.text
    # Store text, ask about button
    btn_key = f"{message.from_user.id}:{int(time.time())}"
    r.set(f'btn_broadcast_text:{btn_key}', text, ex=600)
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(
        types.InlineKeyboardButton("✅ Yes", callback_data=f"add_inline_btn:{btn_key}"),
        types.InlineKeyboardButton("❌ No", callback_data=f"no_inline_btn:{btn_key}"),
    )
    bot.send_message(message.chat.id, "📎 Do you want to add an inline button?", reply_markup=markup)

def process_broadcast_users_btn_label(message, btn_key):
    if message.from_user.id != OWNER_ID:
        return
    r.set(f'btn_broadcast_btn_text:{btn_key}', message.text.strip(), ex=600)
    bot.send_message(message.chat.id, "🔗 Now send the Button URL:")
    bot.register_next_step_handler(message, lambda m: process_broadcast_users_btn_url(m, btn_key))

def process_broadcast_users_btn_url(message, btn_key):
    if message.from_user.id != OWNER_ID:
        return
    r.set(f'btn_broadcast_btn_url:{btn_key}', message.text.strip(), ex=600)
    btext = r.get(f'btn_broadcast_text:{btn_key}')
    bbtn_text = r.get(f'btn_broadcast_btn_text:{btn_key}')
    bbtn_url = r.get(f'btn_broadcast_btn_url:{btn_key}')
    # Send preview
    preview_markup = types.InlineKeyboardMarkup()
    preview_markup.add(types.InlineKeyboardButton(bbtn_text, url=bbtn_url))
    bot.send_message(message.chat.id, f"👁 Preview:\n\n{btext}", reply_markup=preview_markup)
    confirm_markup = types.InlineKeyboardMarkup(row_width=1)
    confirm_markup.add(
        types.InlineKeyboardButton("🚀 Confirm Broadcast", callback_data=f"confirm_broadcast_btn:{btn_key}"),
        types.InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_broadcast_btn:{btn_key}"),
    )
    bot.send_message(message.chat.id, "Ready to broadcast?", reply_markup=confirm_markup)


def process_broadcast_all(message):
    """
    Broadcasts to all groups in a background thread so the webhook thread
    is never blocked. Uses blocking safe_send() so message IDs are tracked.
    """
    if message.from_user.id != OWNER_ID:
        return
    text = message.text
    groups = get_groups()
    if not groups:
        bot.send_message(message.chat.id, "❌ No groups.", reply_markup=_back_markup("back"))
        _clear_pending_buttons(OWNER_ID)
        return

    reply_markup = _build_keyboard_from_pending(OWNER_ID)
    _clear_pending_buttons(OWNER_ID)

    bot.send_message(
        message.chat.id,
        f"📢 Broadcasting to {len(groups)} groups...\n"
        f"⏱ Estimated time: ~{int(len(groups) * _INTER_MSG_DELAY)}s\n"
        f"📡 You'll get a report when it's done.",
        reply_markup=_back_markup("back"),
        disable_web_page_preview=True
    )

    def _do_broadcast():
        sent_count = 0
        failed_count = 0
        for group in groups:
            sent = safe_send(group, text, reply_markup=reply_markup)
            if sent:
                r.set(f'last_sent:{group}', str(sent.message_id))
                save_last_sent(group, sent.message_id)
                sent_count += 1
            else:
                failed_count += 1
        try:
            bot.send_message(
                message.chat.id,
                f"✅ Broadcast complete!\n"
                f"👥 Sent to: {sent_count} groups\n"
                f"❌ Failed: {failed_count}",
                reply_markup=_back_markup("back"),
                disable_web_page_preview=True
            )
        except Exception:
            pass

    threading.Thread(target=_do_broadcast, daemon=True).start()

def process_single_message(message, group_id):
    if message.from_user.id != OWNER_ID:
        return
    text = message.text
    reply_markup = _build_keyboard_from_pending(OWNER_ID)
    _clear_pending_buttons(OWNER_ID)
    sent = safe_send(group_id, text, reply_markup=reply_markup)
    if sent:
        r.set(f'last_sent:{group_id}', str(sent.message_id))
        save_last_sent(group_id, sent.message_id)
        nav_markup = types.InlineKeyboardMarkup(row_width=1)
        if bot_can_pin(group_id):
            nav_markup.add(types.InlineKeyboardButton("📌 Pin Message", callback_data=f"pin_last:{group_id}"))
        nav_markup.add(types.InlineKeyboardButton("🔙 Group Menu", callback_data=f"group_menu:{group_id}"))
        bot.send_message(message.chat.id, "✅ Message sent!", reply_markup=nav_markup,
                         disable_web_page_preview=True)
        if r.get(f'repeat_task:{group_id}') == 'True' and not r.get(f'repeat_text:{group_id}'):
            r.set(f'repeat_text:{group_id}', text)
            bot.send_message(message.chat.id, "ℹ️ This message is now the repeating message (no prior set).",
                             reply_markup=_back_markup(f"group_menu:{group_id}"),
                             disable_web_page_preview=True)
    else:
        err_detail = r.get(f'group_error:{group_id}') or "Unknown error"
        bot.send_message(message.chat.id, f"❌ Error sending to group:\n{err_detail}",
                         reply_markup=_back_markup(f"group_menu:{group_id}"),
                         disable_web_page_preview=True)

def process_promote_to_admin(message, chat_id):
    """
    Promotes users to admin using only the permissions the bot itself holds.
    Telegram only allows granting permissions you have — attempting to grant
    more will error. We read our own permissions first and filter accordingly.
    """
    if message.from_user.id != OWNER_ID:
        return
    raw = message.text.strip()
    id_strings = [x.strip() for x in re.split(r'[\s,]+', raw) if x.strip()]
    results = []

    bot_perms = get_bot_admin_permissions(chat_id)
    if not bot_perms:
        bot.send_message(
            message.chat.id,
            "❌ Bot is not an admin in this group or has no grantable permissions.",
            reply_markup=_back_markup(f"add_to_group:{chat_id}:choose")
        )
        return

    # Only grant permissions the bot actually has
    promote_kwargs = {k: v for k, v in bot_perms.items() if v}

    for id_str in id_strings:
        try:
            user_id = int(id_str)
        except ValueError:
            results.append(f"❌ `{id_str}` — not a valid numeric ID")
            continue
        try:
            bot.promote_chat_member(chat_id, user_id, **promote_kwargs)
            granted = [k.replace('can_', '').replace('_', ' ').title()
                       for k, v in promote_kwargs.items() if v]
            results.append(
                f"✅ `{user_id}` — promoted successfully!\n"
                f"   Granted: {', '.join(granted)}"
            )
        except telebot.apihelper.ApiTelegramException as e:
            err = str(e)
            if "USER_NOT_PARTICIPANT" in err:
                results.append(
                    f"❌ `{user_id}` — Not in the group yet.\n"
                    f"   Use the Invite Link option to add them first."
                )
            elif "CHAT_ADMIN_REQUIRED" in err:
                results.append(f"❌ `{user_id}` — Bot lacks sufficient admin rights.")
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
        sent = safe_send(uid, text)
        if sent:
            save_private_sent(uid, sent.message_id)
            sent_count += 1
            if pin:
                try:
                    bot.pin_chat_message(uid, sent.message_id)
                    pin_count += 1
                except Exception:
                    pin_failed += 1
        else:
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
    # CRITICAL: Remove old global flood key that causes freezes
    r.delete('api_retry_after')
    r.delete('groups_with_errors')

    bot.remove_webhook()
    time.sleep(1)
    bot.set_webhook(WEBHOOK_URL)

    # Register bot command menu (visible when user types /)
    try:
        bot.set_my_commands([
            types.BotCommand('start',   'Open main menu'),
            types.BotCommand('stats',   'Bot statistics'),
            types.BotCommand('backup',  'Backup Redis data now'),
            types.BotCommand('restore', 'Restore Redis from backup file'),
            types.BotCommand('recover', 'Re-register groups after data wipe'),
            types.BotCommand('cancel',  'Cancel current operation'),
        ])
    except Exception as e:
        logger.error(f"[STARTUP] set_my_commands failed: {e}")

    # Restart any active per-group repeat tasks
    for g in get_groups():
        start_repeat_thread(g)

    # Restart global repeat if it was running before redeploy
    start_global_repeat_thread()

    # Start auto-backup thread
    start_backup_thread()

    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
