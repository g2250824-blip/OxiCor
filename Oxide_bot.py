import asyncio
import logging
import math
import os
import time
from collections import defaultdict
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple, List

import aiosqlite
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatMemberStatus, ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandObject, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    TelegramObject,
)
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "PASTE_YOUR_BOT_TOKEN_HERE")
DB_PATH = "oxide_survival.db"

ADMIN_IDS = [6130941321, 6919246443]
DEFAULT_CHANNELS = [
    {"channel_id": "-1003967734883", "link": "https://t.me/OxiCoreBotNews", "title": "Новости Oxide", "type": "news"},
    {"channel_id": "-1003812348241", "link": "https://t.me/UnknownSoftWizard", "title": "Спонсор UnknownSoft", "type": "sponsor"},
]
DEFAULT_OFFICIAL_SERVERS = [-5198339197, -5466654821, -5455856247]

FARM_CONFIG = {
    "wood": {"name": "🌲 Дерево", "amount": 1000, "duration": 60},
    "stone": {"name": "🪨 Камень", "amount": 1000, "duration": 120},
    "metal": {"name": "🔩 Металл", "amount": 1000, "duration": 600},
    "hqm": {"name": "💎 МВК", "amount": 100, "duration": 1200},
    "sulfur": {"name": "🟡 Сера", "amount": 1000, "duration": 3600},
}

BASE_TIERS = {
    0: {"name": "🏖 Пляж (Нет базы)", "cost": {}, "base_hp": 0, "upkeep_hourly": {}, "tc_limit": {}, "repair_res": None},
    1: {"name": "🛖 Тир 1: Деревянная кибитка", "cost": {"wood": 3000}, "base_hp": 500, "upkeep_hourly": {"wood": 100}, "tc_limit": {"wood": 7200}, "repair_res": ("wood", 2)},
    2: {"name": "🏠 Тир 2: Каменный дом", "cost": {"stone": 5000, "wood": 2000}, "base_hp": 1500, "upkeep_hourly": {"stone": 150}, "tc_limit": {"stone": 10800}, "repair_res": ("stone", 2)},
    3: {"name": "🏢 Тир 3: Металлическая база", "cost": {"metal": 4000, "stone": 3000}, "base_hp": 4500, "upkeep_hourly": {"metal": 60}, "tc_limit": {"metal": 4320}, "repair_res": ("metal", 1)},
    4: {"name": "🏰 Тир 4: Бункер из МВК", "cost": {"hqm": 400, "metal": 5000}, "base_hp": 9000, "upkeep_hourly": {"hqm": 3, "metal": 50}, "tc_limit": {"hqm": 216, "metal": 3600}, "repair_res": ("metal", 2)},
}

MAX_DOORS_LEVEL = 5
DOOR_UPGRADE_COST = {"metal": 500, "hqm": 20}
DOOR_HP_BONUS_PER_LEVEL = 0.20
CLAN_MAX_MEMBERS = 4
CLAN_CREATE_COST_WOOD = 1000

CRAFT_RECIPES = {
    "gunpowder": {"name": "⚫ Порох", "batch": 100, "cost": {"sulfur": 100, "wood": 200}, "damage": 0},
    "satchel": {"name": "🧨 Бобовка (Satchel)", "batch": 1, "cost": {"gunpowder": 300, "metal": 100}, "damage": 250},
    "rocket": {"name": "🚀 Ракета (Rocket)", "batch": 1, "cost": {"gunpowder": 650, "metal": 200, "hqm": 5}, "damage": 750},
    "c4": {"name": "💣 Взрывчатка C4", "batch": 1, "cost": {"gunpowder": 1000, "metal": 300, "hqm": 15}, "damage": 1500},
}

WIPE_RAID_BLOCK_SECONDS = 24 * 3600
NEWBIE_SHIELD_SECONDS = 12 * 3600
POST_RAID_SHIELD_SECONDS = 6 * 3600
REPAIR_RAID_BLOCK_SECONDS = 10 * 60
MAX_RAIDS_PER_DAY = 2
LOOT_STEAL_RATIO = 0.60
LOOT_KEEP_RATIO = 0.20

RESOURCE_NAMES = {
    "wood": "🌲 Дерево", "stone": "🪨 Камень", "metal": "🔩 Металл",
    "hqm": "💎 МВК", "sulfur": "🟡 Сера", "gunpowder": "⚫ Порох",
    "satchel": "🧨 Бобовка", "rocket": "🚀 Ракета", "c4": "💣 C4",
}

class LockManager:
    def __init__(self) -> None:
        self._locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
    def get_lock(self, key: str) -> asyncio.Lock:
        return self._locks[key]

locks = LockManager()

class ThrottlingMiddleware(BaseMiddleware):
    def __init__(self, rate_limit: float = 0.8) -> None:
        self.rate_limit = rate_limit
        self.user_timestamps: Dict[int, float] = {}
    async def __call__(self, handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]], event: TelegramObject, data: Dict[str, Any]) -> Any:
        user = data.get("event_from_user")
        if user:
            now = time.monotonic()
            last_time = self.user_timestamps.get(user.id, 0.0)
            if now - last_time < self.rate_limit:
                if isinstance(event, CallbackQuery):
                    try: await event.answer("⏳ Не так быстро!", show_alert=False)
                    except TelegramBadRequest: pass
                return
            self.user_timestamps[user.id] = now
        return await handler(event, data)

class AdminStates(StatesGroup):
    waiting_for_channel_id = State()
    waiting_for_channel_link = State()
    waiting_for_channel_type = State()
    waiting_for_server_id = State()

async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA foreign_keys = ON;")
        await db.execute("""CREATE TABLE IF NOT EXISTS servers (chat_id INTEGER PRIMARY KEY, last_wipe_time INTEGER NOT NULL)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS clans (id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER NOT NULL, tag TEXT NOT NULL, name TEXT NOT NULL, leader_id INTEGER NOT NULL, UNIQUE(chat_id, tag))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS bases (
            id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER NOT NULL, owner_user_id INTEGER NULL, owner_clan_id INTEGER NULL,
            tier INTEGER NOT NULL DEFAULT 0, doors_level INTEGER NOT NULL DEFAULT 0, hp INTEGER NOT NULL DEFAULT 0, max_hp INTEGER NOT NULL DEFAULT 0,
            wood INTEGER NOT NULL DEFAULT 0, stone INTEGER NOT NULL DEFAULT 0, metal INTEGER NOT NULL DEFAULT 0, hqm INTEGER NOT NULL DEFAULT 0, sulfur INTEGER NOT NULL DEFAULT 0,
            gunpowder INTEGER NOT NULL DEFAULT 0, satchel INTEGER NOT NULL DEFAULT 0, rocket INTEGER NOT NULL DEFAULT 0, c4 INTEGER NOT NULL DEFAULT 0,
            tc_wood INTEGER NOT NULL DEFAULT 0, tc_stone INTEGER NOT NULL DEFAULT 0, tc_metal INTEGER NOT NULL DEFAULT 0, tc_hqm INTEGER NOT NULL DEFAULT 0,
            is_decaying INTEGER NOT NULL DEFAULT 0, shield_until INTEGER NOT NULL DEFAULT 0, last_damaged_at INTEGER NOT NULL DEFAULT 0,
            raids_today INTEGER NOT NULL DEFAULT 0, raids_total INTEGER NOT NULL DEFAULT 0
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS players (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, chat_id INTEGER NOT NULL, username TEXT NOT NULL, base_id INTEGER NOT NULL,
            clan_id INTEGER NULL, clan_role TEXT NULL, farm_resource TEXT NULL, farm_end_time INTEGER NULL, UNIQUE(user_id, chat_id)
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT, channel_id TEXT NOT NULL UNIQUE, title TEXT, type TEXT NOT NULL, link TEXT
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS official_servers (chat_id INTEGER PRIMARY KEY, title TEXT)""")
        
        for ch in DEFAULT_CHANNELS:
            try: await db.execute("INSERT OR IGNORE INTO channels (channel_id, title, type, link) VALUES (?, ?, ?, ?)", (ch["channel_id"], ch["title"], ch["type"], ch["link"]))
            except Exception: pass
        for srv_id in DEFAULT_OFFICIAL_SERVERS:
            try: await db.execute("INSERT OR IGNORE INTO official_servers (chat_id) VALUES (?)", (srv_id,))
            
        await db.commit()

def calculate_max_hp(tier: int, doors_level: int) -> int:
    base_hp = BASE_TIERS[tier]["base_hp"]
    if base_hp == 0: return 0
    return int(round(base_hp * (1.0 + (DOOR_HP_BONUS_PER_LEVEL * doors_level))))

def calculate_hourly_upkeep(tier: int, clan_members: int, doors_level: int) -> Dict[str, int]:
    base_upkeep = BASE_TIERS[tier]["upkeep_hourly"]
    if not base_upkeep: return {}
    members = max(1, clan_members)
    clan_mult = 1.0 + 0.35 * (members - 1)
    doors_mult = 1.0 + 0.15 * doors_level
    return {res: int(math.ceil(amt * clan_mult * doors_mult)) for res, amt in base_upkeep.items()}

def format_duration(seconds: int) -> str:
    seconds = max(0, int(seconds))
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}" if hours > 0 else f"{minutes:02d}:{secs:02d}"

async def ensure_server_and_player(db: aiosqlite.Connection, chat_id: int, user_id: int, username: str) -> Tuple[aiosqlite.Row, aiosqlite.Row, Optional[str]]:
    db.row_factory = aiosqlite.Row
    now = int(time.time())
    async with db.execute("SELECT * FROM servers WHERE chat_id = ?", (chat_id,)) as cur:
        server = await cur.fetchone()
    if not server:
        await db.execute("INSERT INTO servers (chat_id, last_wipe_time) VALUES (?, ?)", (chat_id, now - WIPE_RAID_BLOCK_SECONDS))
    
    async with db.execute("SELECT * FROM players WHERE user_id = ? AND chat_id = ?", (user_id, chat_id)) as cur:
        player = await cur.fetchone()
    clean_username = username.lstrip("@") if username else f"id{user_id}"

    if not player:
        shield_until = now + NEWBIE_SHIELD_SECONDS
        cur = await db.execute("INSERT INTO bases (chat_id, owner_user_id, tier, doors_level, hp, max_hp, shield_until) VALUES (?, ?, 0, 0, 0, 0, ?)", (chat_id, user_id, shield_until))
        base_id = cur.lastrowid
        await db.execute("INSERT INTO players (user_id, chat_id, username, base_id) VALUES (?, ?, ?, ?)", (user_id, chat_id, clean_username, base_id))
        await db.commit()
        async with db.execute("SELECT * FROM players WHERE user_id = ? AND chat_id = ?", (user_id, chat_id)) as cur:
            player = await cur.fetchone()
    else:
        if player["username"] != clean_username:
            await db.execute("UPDATE players SET username = ? WHERE id = ?", (clean_username, player["id"]))
            await db.commit()

    farm_msg = None
    if player["farm_resource"] and player["farm_end_time"] and player["farm_end_time"] <= now:
        res_key = player["farm_resource"]
        cfg = FARM_CONFIG.get(res_key)
        if cfg:
            amount = cfg["amount"]
            await db.execute(f"UPDATE bases SET {res_key} = {res_key} + ? WHERE id = ?", (amount, player["base_id"]))
            farm_msg = f"✅ <b>Фарм завершён!</b> На склад доставлено: <b>+{amount} {cfg['name']}</b>"
        await db.execute("UPDATE players SET farm_resource = NULL, farm_end_time = NULL WHERE id = ?", (player["id"],))
        await db.commit()
        async with db.execute("SELECT * FROM players WHERE id = ?", (player["id"],)) as cur:
            player = await cur.fetchone()

    async with db.execute("SELECT * FROM bases WHERE id = ?", (player["base_id"],)) as cur:
        base = await cur.fetchone()
    return player, base, farm_msg

async def get_clan_info(db: aiosqlite.Connection, clan_id: Optional[int]) -> Tuple[Optional[aiosqlite.Row], int]:
    if not clan_id: return None, 1
    db.row_factory = aiosqlite.Row
    async with db.execute("SELECT * FROM clans WHERE id = ?", (clan_id,)) as cur: clan = await cur.fetchone()
    async with db.execute("SELECT COUNT(*) as cnt FROM players WHERE clan_id = ?", (clan_id,)) as cur: count = (await cur.fetchone())["cnt"] or 1
    return clan, count

async def get_official_servers_list(db: aiosqlite.Connection) -> List[int]:
    async with db.execute("SELECT chat_id FROM official_servers") as cur:
        rows = await cur.fetchall()
    return [row[0] for row in rows]

async def check_subscription(bot: Bot, user_id: int) -> Tuple[bool, InlineKeyboardMarkup]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM channels") as cur:
            channels = await cur.fetchall()
    if not channels: return True, InlineKeyboardMarkup(inline_keyboard=[])

    news_ch = [c for c in channels if c["type"] == "news"]
    sponsor_ch = [c for c in channels if c["type"] == "sponsor"]
    unsubscribed = []
    for ch in channels:
        try:
            member = await bot.get_chat_member(ch["channel_id"], user_id)
            if member.status not in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR):
                unsubscribed.append(ch)
        except Exception:
            logging.warning(f"Не удалось проверить подписку на {ch['title']} ({ch['channel_id']})")
            unsubscribed.append(ch) 

    kb_buttons = []
    if news_ch:
        kb_buttons.append([InlineKeyboardButton(text="📢 Новостные каналы", callback_data="ignore")])
        for ch in news_ch:
            if ch in unsubscribed:
                url = ch["link"] if ch["link"] else f"https://t.me/{str(ch['channel_id']).replace('-100', '')}"
                kb_buttons.append([InlineKeyboardButton(text=f"🔗 {ch['title'] or 'Подписаться'}", url=url)])
    if sponsor_ch:
        kb_buttons.append([InlineKeyboardButton(text="💎 Спонсорские каналы", callback_data="ignore")])
        for ch in sponsor_ch:
            if ch in unsubscribed:
                url = ch["link"] if ch["link"] else f"https://t.me/{str(ch['channel_id']).replace('-100', '')}"
                kb_buttons.append([InlineKeyboardButton(text=f"🔗 {ch['title'] or 'Подписаться'}", url=url)])
    
    kb_buttons.append([InlineKeyboardButton(text="✅ Я подписался, проверить", callback_data="check_sub")])
    if not unsubscribed: return True, InlineKeyboardMarkup(inline_keyboard=[])
    return False, InlineKeyboardMarkup(inline_keyboard=kb_buttons)

def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 База / Профиль", callback_data="nav:profile"), InlineKeyboardButton(text="⛏ Фарм", callback_data="nav:farm")],
        [InlineKeyboardButton(text="🔨 Стройка", callback_data="nav:build"), InlineKeyboardButton(text="🗄 Шкаф (TC)", callback_data="nav:tc")],
        [InlineKeyboardButton(text="🧨 Крафт", callback_data="nav:craft"), InlineKeyboardButton(text="👥 Клан", callback_data="nav:clan")],
        [InlineKeyboardButton(text="🏆 Топ сервера", callback_data="nav:top")],
    ])

def farm_menu_kb() -> InlineKeyboardMarkup:
    buttons = [[InlineKeyboardButton(text=f"{cfg['name']} (+{cfg['amount']} за {cfg['duration']//60} мин)", callback_data=f"farm:{res_id}")] for res_id, cfg in FARM_CONFIG.items()]
    buttons.append([InlineKeyboardButton(text="🔙 Назад к базе", callback_data="nav:profile")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def build_menu_kb(tier: int, doors_level: int, hp: int, max_hp: int) -> InlineKeyboardMarkup:
    buttons = []
    if tier < 4:
        n_cfg = BASE_TIERS[tier + 1]
        cost_str = ", ".join(f"{v} {RESOURCE_NAMES[k]}" for k, v in n_cfg["cost"].items())
        buttons.append([InlineKeyboardButton(text=f"⬆️ Улучшить до Тир {tier+1} ({cost_str})", callback_data="build:upgrade")])
    if tier > 0 and doors_level < MAX_DOORS_LEVEL:
        buttons.append([InlineKeyboardButton(text=f"🚪 Укрепить двери Ур. {doors_level+1}/5 (500 🔩 + 20 💎)", callback_data="build:doors")])
    if tier > 0 and hp < max_hp:
        buttons.append([InlineKeyboardButton(text=f"🔧 Починить базу ({hp}/{max_hp} HP)", callback_data="build:repair")])
    buttons.append([InlineKeyboardButton(text="🔙 Назад к базе", callback_data="nav:profile")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def tc_menu_kb(tier: int) -> InlineKeyboardMarkup:
    buttons = []
    if tier > 0:
        for res_key in BASE_TIERS[tier]["upkeep_hourly"].keys():
            buttons.append([InlineKeyboardButton(text=f"📥 Вложить 500 {RESOURCE_NAMES[res_key]}", callback_data=f"tc:dep:{res_key}:500"), InlineKeyboardButton(text="📥 До максимума", callback_data=f"tc:dep:{res_key}:max")])
    buttons.append([InlineKeyboardButton(text="🔙 Назад к базе", callback_data="nav:profile")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def craft_menu_kb() -> InlineKeyboardMarkup:
    buttons = []
    for item_id, cfg in CRAFT_RECIPES.items():
        title = f"{cfg['name']} (x{cfg['batch']})" if cfg["batch"] > 1 else cfg["name"]
        buttons.append([InlineKeyboardButton(text=f"🔨 {title} [x1]", callback_data=f"craft:{item_id}:1"), InlineKeyboardButton(text="[x5]", callback_data=f"craft:{item_id}:5"), InlineKeyboardButton(text="[Макс]", callback_data=f"craft:{item_id}:max")])
    buttons.append([InlineKeyboardButton(text="🔙 Назад к базе", callback_data="nav:profile")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def raid_action_kb(target_base_id: int, attacker_base: aiosqlite.Row) -> InlineKeyboardMarkup:
    buttons = []
    for exp_id in ("satchel", "rocket", "c4"):
        count = attacker_base[exp_id]
        cfg = CRAFT_RECIPES[exp_id]
        if count > 0:
            buttons.append([InlineKeyboardButton(text=f"💥 Использовать {cfg['name']} (-{cfg['damage']} HP) [В наличии: {count}]", callback_data=f"raid_hit:{target_base_id}:{exp_id}")])
    buttons.append([InlineKeyboardButton(text="🏳 Отступить", callback_data="nav:profile")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def admin_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика бота", callback_data="admin:stats")],
        [InlineKeyboardButton(text="➕ Добавить канал", callback_data="admin:add_ch")],
        [InlineKeyboardButton(text="⭐ Офиц. сервер", callback_data="admin:add_srv")],
        [InlineKeyboardButton(text="❌ Удалить канал", callback_data="admin:del_ch")],
        [InlineKeyboardButton(text="📋 Список каналов", callback_data="admin:list_ch")],
    ])

async def render_profile_text(db: aiosqlite.Connection, player: aiosqlite.Row, base: aiosqlite.Row, prefix_msg: Optional[str] = None) -> str:
    now = int(time.time())
    clan, clan_members = await get_clan_info(db, player["clan_id"])
    tier_cfg = BASE_TIERS[base["tier"]]
    header = f"🏴 <b>Клан [{clan['tag']}] {clan['name']}</b> ({clan_members}/{CLAN_MAX_MEMBERS} чел.)\n👤 Выживший: @{player['username']}" if clan else f"👤 <b>Выживший: @{player['username']}</b> (Соло)"
    
    farm_status = f"⛏ Добывает: <b>{FARM_CONFIG[player['farm_resource']]['name']}</b> (осталось <code>{format_duration(player['farm_end_time'] - now)}</code>)" if player["farm_resource"] and player["farm_end_time"] and player["farm_end_time"] > now else "💤 Свободен (готов к фарму)"
    shield_str = f"🛡 Активен ({format_duration(base['shield_until'] - now)})" if base["shield_until"] > now else "❌ Нет защиты"
    
    if base["tier"] == 0:
        tc_status = "Нет шкафа (постройте базу)"
    else:
        upkeep = calculate_hourly_upkeep(base["tier"], clan_members, base["doors_level"])
        parts = []
        hours_list = []
        for res_key, hourly_amt in upkeep.items():
            in_tc = base[f"tc_{res_key}"]
            limit = tier_cfg["tc_limit"][res_key]
            parts.append(f"{RESOURCE_NAMES[res_key]}: {in_tc}/{limit} (-{hourly_amt}/ч)")
            hours_list.append(in_tc // hourly_amt if hourly_amt > 0 else 999)
        min_hours = min(hours_list) if hours_list else 0
        decay_warn = " ⚠️ <b>ГНИЁТ!</b>" if base["is_decaying"] else f" (хватит на ~{min_hours} ч.)"
        tc_status = "; ".join(parts) + decay_warn

    text = f"{prefix_msg}\n\n" if prefix_msg else ""
    text += (
        f"{header}\n━━━━━━━━━━━━━━━━━━━━\n🏠 <b>Постройка:</b> {tier_cfg['name']}\n"
        f"🚪 <b>Укрепления (Двери):</b> {base['doors_level']}/{MAX_DOORS_LEVEL} ур.\n"
        f"❤️ <b>Прочность:</b> <code>{base['hp']} / {base['max_hp']} HP</code>\n"
        f"🛡 <b>Щит от рейдов:</b> {shield_str}\n🗄 <b>Шкаф (TC):</b> {tc_status}\n"
        f"📍 <b>Статус персонажа:</b> {farm_status}\n━━━━━━━━━━━━━━━━━━━━\n"
        f"📦 <b>Склад ресурсов:</b>\n🌲 Дерево: <code>{base['wood']}</code> | 🪨 Камень: <code>{base['stone']}</code>\n"
        f"🔩 Металл: <code>{base['metal']}</code> | 💎 МВК: <code>{base['hqm']}</code>\n🟡 Сера: <code>{base['sulfur']}</code>\n\n"
        f"💣 <b>Арсенал взрывчатки:</b>\n⚫ Порох: <code>{base['gunpowder']}</code> | 🧨 Бобовка: <code>{base['satchel']}</code>\n"
        f"🚀 Ракета: <code>{base['rocket']}</code> | 💣 C4: <code>{base['c4']}</code>\n"
        f"⚔️ Рейдов сегодня: <code>{base['raids_today']}/{MAX_RAIDS_PER_DAY}</code> (Всего: {base['raids_total']})"
    )
    return text

router = Router()
def get_username_str(user: Any) -> str:
    return user.username if user.username else user.full_name

@router.message(Command("start", "profile", "base"))
@router.message(F.text.lower().in_({"база", "профиль", "дом", "инвентарь"}))
async def cmd_profile(message: Message, bot: Bot) -> None:
    is_sub, kb = await check_subscription(bot, message.from_user.id)
    if not is_sub:
        await message.reply("⚠️ <b>Для игры необходимо подписаться на ВСЕ каналы!</b>\nНовостные каналы дают информацию, спонсорские поддерживают проект.", reply_markup=kb)
        return
    
    chat_id = message.chat.id
    if message.chat.type == "private":
        await message.reply("⚠️ <b>Играть нужно в чатах!</b>\nКаждый групповой чат — это отдельный сервер выживания.\nДобавьте меня в группу и напишите /start там.")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        off_servers = await get_official_servers_list(db)
    server_type = "🌐 Официальный сервер" if chat_id in off_servers else "🏝 Пользовательский сервер"
    
    async with locks.get_lock(f"chat:{chat_id}"):
        async with aiosqlite.connect(DB_PATH) as db:
            player, base, farm_msg = await ensure_server_and_player(db, chat_id, message.from_user.id, get_username_str(message.from_user))
            text = f"📡 <b>Сервер:</b> {server_type} (ID: <code>{chat_id}</code>)\n\n"
            text += await render_profile_text(db, player, base, farm_msg)
            await message.reply(text, reply_markup=main_menu_kb(), parse_mode=ParseMode.HTML)

@router.callback_query(F.data == "check_sub")
async def cb_check_sub(callback: CallbackQuery, bot: Bot) -> None:
    is_sub, kb = await check_subscription(bot, callback.from_user.id)
    if is_sub:
        await callback.message.delete()
        await callback.answer("✅ Подписка подтверждена! Теперь напишите /start в игровом чате.", show_alert=True)
    else:
        await callback.message.edit_reply_markup(reply_markup=kb)
        await callback.answer("❌ Вы подписались не на все каналы!", show_alert=True)

@router.message(Command("farm"))
@router.message(F.text.lower() == "фарм")
async def cmd_farm_menu(message: Message, bot: Bot) -> None:
    is_sub, kb = await check_subscription(bot, message.from_user.id)
    if not is_sub:
        await message.reply("⚠️ Сначала подпишитесь на каналы!", reply_markup=kb); return
    if message.chat.type == "private":
        await message.reply("Играйте в групповых чатах!"); return
    chat_id = message.chat.id
    async with locks.get_lock(f"chat:{chat_id}"):
        async with aiosqlite.connect(DB_PATH) as db:
            player, _, farm_msg = await ensure_server_and_player(db, chat_id, message.from_user.id, get_username_str(message.from_user))
            now = int(time.time())
            prefix = f"{farm_msg}\n\n" if farm_msg else ""
            if player["farm_resource"] and player["farm_end_time"] and player["farm_end_time"] > now:
                await message.reply(f"{prefix}⏳ Вы уже на фарме. Осталось: <code>{format_duration(player['farm_end_time'] - now)}</code>", reply_markup=main_menu_kb())
                return
            await message.reply(f"{prefix}⛏ <b>Куда отправимся на фарм?</b>", reply_markup=farm_menu_kb())

@router.callback_query(F.data.startswith("farm:"))
async def cb_start_farm(callback: CallbackQuery) -> None:
    res_id = callback.data.split(":")[1]
    if res_id not in FARM_CONFIG: return
    chat_id = callback.message.chat.id
    async with locks.get_lock(f"chat:{chat_id}"):
        async with aiosqlite.connect(DB_PATH) as db:
            player, base, farm_msg = await ensure_server_and_player(db, chat_id, callback.from_user.id, get_username_str(callback.from_user))
            now = int(time.time())
            if player["farm_resource"] and player["farm_end_time"] and player["farm_end_time"] > now:
                await callback.answer(f"Вы уже на фарме. Осталось: {format_duration(player['farm_end_time'] - now)}", show_alert=True); return
            cfg = FARM_CONFIG[res_id]
            await db.execute("UPDATE players SET farm_resource = ?, farm_end_time = ? WHERE id = ?", (res_id, now + cfg["duration"], player["id"]))
            await db.commit()
            async with db.execute("SELECT * FROM players WHERE id = ?", (player["id"],)) as cur: player = await cur.fetchone()
            msg = f"⛏ Вы отправились добывать <b>{cfg['name']}</b>. Время: {cfg['duration'] // 60} мин."
            if farm_msg: msg = f"{farm_msg}\n{msg}"
            text = await render_profile_text(db, player, base, msg)
            try: await callback.message.edit_text(text, reply_markup=main_menu_kb())
            except TelegramBadRequest: pass
            await callback.answer("Фарм запущен!")

@router.callback_query(F.data.startswith("nav:"))
async def cb_navigation(callback: CallbackQuery) -> None:
    section = callback.data.split(":")[1]
    chat_id = callback.message.chat.id
    async with locks.get_lock(f"chat:{chat_id}"):
        async with aiosqlite.connect(DB_PATH) as db:
            player, base, farm_msg = await ensure_server_and_player(db, chat_id, callback.from_user.id, get_username_str(callback.from_user))
            if section == "profile": text, kb = await render_profile_text(db, player, base, farm_msg), main_menu_kb()
            elif section == "farm": text, kb = "⛏ <b>Меню фарма ресурсов</b>", farm_menu_kb()
            elif section == "build":
                tier, cfg = base["tier"], BASE_TIERS[base["tier"]]
                lines = [f"🔨 <b>Стройка</b>\nБаза: <b>{cfg['name']}</b>\nДвери: <b>{base['doors_level']}/5</b>\nHP: <code>{base['hp']}/{base['max_hp']}</code>"]
                if tier < 4:
                    n_cfg = BASE_TIERS[tier+1]
                    lines.append(f"\n⬆️ Следующий: {n_cfg['name']}\nЦена: {', '.join(f'{v} {RESOURCE_NAMES[k]}' for k,v in n_cfg['cost'].items())}")
                text, kb = "\n".join(lines), build_menu_kb(base["tier"], base["doors_level"], base["hp"], base["max_hp"])
            elif section == "tc":
                if base["tier"] == 0: text = "🗄 Постройте базу!"
                else:
                    _, cm = await get_clan_info(db, player["clan_id"])
                    hourly = calculate_hourly_upkeep(base["tier"], cm, base["doors_level"])
                    text = "🗄 <b>Шкаф (TC)</b>\n" + "\n".join(f"• {RESOURCE_NAMES[k]}: <code>{base[f'tc_{k}']} / {BASE_TIERS[base['tier']]['tc_limit'][k]}</code> (-{v}/ч)" for k,v in hourly.items())
                kb = tc_menu_kb(base["tier"])
            elif section == "craft":
                text = f"🧨 <b>Крафт</b>\nРесурсы: 🌲{base['wood']} | 🪨{base['stone']} | 🔩{base['metal']} | 💎{base['hqm']} | 🟡{base['sulfur']}"
                kb = craft_menu_kb()
            elif section == "clan":
                if not player["clan_id"]: text = "👥 <b>Кланы</b>\n<code>/clan create ТЕГ Название</code> - создать (1000 дерева)\n<code>/clan invite @user</code> - пригласить"
                else:
                    clan, _ = await get_clan_info(db, player["clan_id"])
                    text = f"🏴 <b>Клан [{clan['tag']}] {clan['name']}</b>"
                kb = main_menu_kb()
            elif section == "top":
                async with db.execute("SELECT * FROM bases WHERE chat_id = ? ORDER BY hp DESC LIMIT 10", (chat_id,)) as cur: bases = await cur.fetchall()
                text = "🏆 <b>ТОП-10 СЕРВЕРА</b>\n" + "\n".join(f"{i+1}. HP: <code>{b['hp']}</code> | Рейдов: {b['raids_total']}" for i,b in enumerate(bases)) if bases else "Пока пусто."
                kb = main_menu_kb()
            else: return
            try: await callback.message.edit_text(text, reply_markup=kb)
            except TelegramBadRequest: pass
            await callback.answer()

@router.callback_query(F.data.startswith("build:"))
async def cb_build_actions(callback: CallbackQuery) -> None:
    action = callback.data.split(":")[1]
    chat_id = callback.message.chat.id
    async with locks.get_lock(f"chat:{chat_id}"):
        async with aiosqlite.connect(DB_PATH) as db:
            player, base, _ = await ensure_server_and_player(db, chat_id, callback.from_user.id, get_username_str(callback.from_user))
            now = int(time.time())
            if action == "upgrade":
                if base["tier"] >= 4: await callback.answer("Макс уровень!", show_alert=True); return
                nxt = base["tier"] + 1
                cost = BASE_TIERS[nxt]["cost"]
                for k,v in cost.items():
                    if base[k] < v: await callback.answer(f"Не хватает {v} {RESOURCE_NAMES[k]}", show_alert=True); return
                new_max = calculate_max_hp(nxt, base["doors_level"])
                set_parts = [f"{k} = {k} - {v}" for k,v in cost.items()]
                set_parts.extend(["tier = ?", "max_hp = ?", "hp = ?", "is_decaying = 0"])
                await db.execute(f"UPDATE bases SET {', '.join(set_parts)} WHERE id = ?", (nxt, new_max, new_max, base["id"]))
                await db.commit()
                await callback.answer(f"🎉 Построено: {BASE_TIERS[nxt]['name']}!", show_alert=True)
            elif action == "doors":
                if base["doors_level"] >= 5: await callback.answer("Макс дверей!", show_alert=True); return
                if base["metal"] < 500 or base["hqm"] < 20: await callback.answer("Не хватает ресурсов!", show_alert=True); return
                nd = base["doors_level"] + 1
                nm = calculate_max_hp(base["tier"], nd)
                await db.execute("UPDATE bases SET metal = metal - 500, hqm = hqm - 20, doors_level = ?, max_hp = ?, hp = hp + ? WHERE id = ?", (nd, nm, nm - base["max_hp"], base["id"]))
                await db.commit()
                await callback.answer(f"🚪 Двери улучшены до {nd}/5!", show_alert=True)
            elif action == "repair":
                if base["hp"] >= base["max_hp"]: await callback.answer("Не нужен ремонт!", show_alert=True); return
                if now - base["last_damaged_at"] < REPAIR_RAID_BLOCK_SECONDS: await callback.answer("Рейд-блок! Ждите.", show_alert=True); return
                rk, cph = BASE_TIERS[base["tier"]]["repair_res"]
                mh = min(base["max_hp"] - base["hp"], base[rk] // cph)
                if mh <= 0: await callback.answer("Нет ресурсов для ремонта!", show_alert=True); return
                await db.execute(f"UPDATE bases SET {rk} = {rk} - ?, hp = hp + ? WHERE id = ?", (mh * cph, mh, base["id"]))
                await db.commit()
                await callback.answer(f"🔧 Починено +{mh} HP!", show_alert=True)
            async with db.execute("SELECT * FROM bases WHERE id = ?", (base["id"],)) as cur: base = await cur.fetchone()
            tier, cfg = base["tier"], BASE_TIERS[base["tier"]]
            lines = [f"🔨 <b>Стройка</b>\nБаза: <b>{cfg['name']}</b>\nДвери: <b>{base['doors_level']}/5</b>\nHP: <code>{base['hp']}/{base['max_hp']}</code>"]
            if tier < 4:
                n_cfg = BASE_TIERS[tier+1]
                lines.append(f"\n⬆️ Следующий: {n_cfg['name']}\nЦена: {', '.join(f'{v} {RESOURCE_NAMES[k]}' for k,v in n_cfg['cost'].items())}")
            try: await callback.message.edit_text("\n".join(lines), reply_markup=build_menu_kb(base["tier"], base["doors_level"], base["hp"], base["max_hp"]))
            except TelegramBadRequest: pass

@router.callback_query(F.data.startswith("tc:dep:"))
async def cb_tc_deposit(callback: CallbackQuery) -> None:
    _, _, res_key, mode = callback.data.split(":")
    chat_id = callback.message.chat.id
    async with locks.get_lock(f"chat:{chat_id}"):
        async with aiosqlite.connect(DB_PATH) as db:
            player, base, _ = await ensure_server_and_player(db, chat_id, callback.from_user.id, get_username_str(callback.from_user))
            limit = BASE_TIERS[base["tier"]]["tc_limit"][res_key]
            space = max(0, limit - base[f"tc_{res_key}"])
            want = space if mode == "max" else int(mode)
            transfer = min(want, space, base[res_key])
            if transfer <= 0: await callback.answer("Нечего вкладывать!", show_alert=True); return
            await db.execute(f"UPDATE bases SET {res_key} = {res_key} - ?, tc_{res_key} = tc_{res_key} + ?, is_decaying = 0 WHERE id = ?", (transfer, transfer, base["id"]))
            await db.commit()
            await callback.answer(f"Вложено +{transfer} {RESOURCE_NAMES[res_key]}!", show_alert=True)

@router.callback_query(F.data.startswith("craft:"))
async def cb_craft_item(callback: CallbackQuery) -> None:
    _, item_id, qty_mode = callback.data.split(":")
    chat_id = callback.message.chat.id
    async with locks.get_lock(f"chat:{chat_id}"):
        async with aiosqlite.connect(DB_PATH) as db:
            player, base, _ = await ensure_server_and_player(db, chat_id, callback.from_user.id, get_username_str(callback.from_user))
            recipe = CRAFT_RECIPES[item_id]
            max_c = min(base[k] // v for k,v in recipe["cost"].items())
            if max_c <= 0: await callback.answer("Нет ресурсов!", show_alert=True); return
            cnt = max_c if qty_mode == "max" else min(int(qty_mode), max_c)
            total = cnt * recipe["batch"]
            set_p = [f"{k} = {k} - {v*cnt}" for k,v in recipe["cost"].items()]
            set_p.append(f"{item_id} = {item_id} + {total}")
            await db.execute(f"UPDATE bases SET {', '.join(set_p)} WHERE id = ?", (base["id"],))
            await db.commit()
            await callback.answer(f"✅ Скрафчено: +{total} {recipe['name']}", show_alert=True)

@router.message(Command("raid"))
@router.message(F.text.lower().startswith("рейд"))
async def cmd_raid(message: Message, bot: Bot) -> None:
    is_sub, kb = await check_subscription(bot, message.from_user.id)
    if not is_sub: await message.reply("⚠️ Сначала подпишитесь!", reply_markup=kb); return
    if message.chat.type == "private": await message.reply("Играйте в группах!"); return
    chat_id = message.chat.id
    async with locks.get_lock(f"chat:{chat_id}"):
        async with aiosqlite.connect(DB_PATH) as db:
            att, att_b, _ = await ensure_server_and_player(db, chat_id, message.from_user.id, get_username_str(message.from_user))
            now = int(time.time())
            t_pl = None
            if message.reply_to_message and message.reply_to_message.from_user and not message.reply_to_message.from_user.is_bot:
                t_pl, _, _ = await ensure_server_and_player(db, chat_id, message.reply_to_message.from_user.id, get_username_str(message.reply_to_message.from_user))
            if not t_pl: await message.reply("Ответьте командой /raid на сообщение жертвы!"); return
            if t_pl["base_id"] == att_b["id"]: await message.reply("Нельзя рейдить себя!"); return
            async with db.execute("SELECT * FROM bases WHERE id = ?", (t_pl["base_id"],)) as cur: vic_b = await cur.fetchone()
            if vic_b["shield_until"] > now: await message.reply(f"🛡 Щит активен ещё {format_duration(vic_b['shield_until'] - now)}"); return
            if att_b["shield_until"] > now:
                await db.execute("UPDATE bases SET shield_until = 0 WHERE id = ?", (att_b["id"],))
                await db.commit()
            if vic_b["tier"] == 0 or vic_b["hp"] <= 0:
                stolen = {}
                for k in ("wood","stone","metal","hqm","sulfur","gunpowder","satchel","rocket","c4"):
                    tot = vic_b[k] + (vic_b[f"tc_{k}"] if k in ("wood","stone","metal","hqm") else 0)
                    stl = int(tot * LOOT_STEAL_RATIO)
                    kep = int(tot * LOOT_KEEP_RATIO)
                    stolen[k] = stl
                    if k in ("wood","stone","metal","hqm"):
                        await db.execute(f"UPDATE bases SET {k} = ?, tc_{k} = 0 WHERE id = ?", (kep, vic_b["id"]))
                    else:
                        await db.execute(f"UPDATE bases SET {k} = ? WHERE id = ?", (kep, vic_b["id"]))
                    await db.execute(f"UPDATE bases SET {k} = {k} + ? WHERE id = ?", (stl, att_b["id"]))
                await db.execute("UPDATE bases SET tier=0, doors_level=0, hp=0, max_hp=0, shield_until=? WHERE id=?", (now + POST_RAID_SHIELD_SECONDS, vic_b["id"]))
                await db.execute("UPDATE bases SET raids_today=raids_today+1, raids_total=raids_total+1 WHERE id=?", (att_b["id"],))
                await db.commit()
                loot_s = ", ".join(f"{RESOURCE_NAMES[k]}: <b>{v}</b>" for k,v in stolen.items() if v>0) or "пусто"
                await message.reply(f"🏴‍☠️ <b>Ограбление без взрывчатки!</b>\nВынесено: {loot_s}")
                return
            await message.reply(f"💣 <b>ШТУРМ БАЗЫ</b>\nHP: <code>{vic_b['hp']}/{vic_b['max_hp']}</code>\nВыберите взрывчатку:", reply_markup=raid_action_kb(vic_b["id"], att_b))

@router.callback_query(F.data.startswith("raid_hit:"))
async def cb_raid_hit(callback: CallbackQuery) -> None:
    _, t_bid_str, exp_id = callback.data.split(":")
    t_bid = int(t_bid_str)
    chat_id = callback.message.chat.id
    async with locks.get_lock(f"chat:{chat_id}"):
        async with aiosqlite.connect(DB_PATH) as db:
            att, att_b, _ = await ensure_server_and_player(db, chat_id, callback.from_user.id, get_username_str(callback.from_user))
            if att_b[exp_id] <= 0: await callback.answer("Нет взрывчатки!", show_alert=True); return
            async with db.execute("SELECT * FROM bases WHERE id = ?", (t_bid,)) as cur: vic_b = await cur.fetchone()
            if not vic_b or vic_b["hp"] <= 0: await callback.answer("Уже разрушена!", show_alert=True); return
            dmg = CRAFT_RECIPES[exp_id]["damage"]
            nhp = max(0, vic_b["hp"] - dmg)
            await db.execute(f"UPDATE bases SET {exp_id} = {exp_id} - 1 WHERE id = ?", (att_b["id"],))
            await db.execute("UPDATE bases SET hp = ?, last_damaged_at = ? WHERE id = ?", (nhp, int(time.time()), t_bid))
            await db.commit()
            if nhp == 0:
                stolen = {}
                for k in ("wood","stone","metal","hqm","sulfur","gunpowder","satchel","rocket","c4"):
                    tot = vic_b[k] + (vic_b[f"tc_{k}"] if k in ("wood","stone","metal","hqm") else 0)
                    stl = int(tot * LOOT_STEAL_RATIO)
                    kep = int(tot * LOOT_KEEP_RATIO)
                    stolen[k] = stl
                    if k in ("wood","stone","metal","hqm"):
                        await db.execute(f"UPDATE bases SET {k} = ?, tc_{k} = 0 WHERE id = ?", (kep, t_bid))
                    else:
                        await db.execute(f"UPDATE bases SET {k} = ? WHERE id = ?", (kep, t_bid))
                    await db.execute(f"UPDATE bases SET {k} = {k} + ? WHERE id = ?", (stl, att_b["id"]))
                await db.execute("UPDATE bases SET tier=0, doors_level=0, hp=0, max_hp=0, shield_until=? WHERE id=?", (int(time.time()) + POST_RAID_SHIELD_SECONDS, t_bid))
                await db.execute("UPDATE bases SET raids_today=raids_today+1, raids_total=raids_total+1 WHERE id=?", (att_b["id"],))
                await db.commit()
                loot_s = ", ".join(f"{RESOURCE_NAMES[k]}: <b>{v}</b>" for k,v in stolen.items() if v>0) or "пусто"
                try: await callback.message.edit_text(f"💥 <b>БАЗА РАЗРУШЕНА!</b>\nЗахвачено: {loot_s}", reply_markup=main_menu_kb())
                except TelegramBadRequest: pass
                await callback.answer("💥 ВЗОРВАНО!", show_alert=True)
            else:
                async with db.execute("SELECT * FROM bases WHERE id = ?", (att_b["id"],)) as cur: att_b = await cur.fetchone()
                try: await callback.message.edit_text(f"💣 <b>ШТУРМ</b>\n-{dmg} HP!\nОсталось: <code>{nhp}/{vic_b['max_hp']}</code>", reply_markup=raid_action_kb(t_bid, att_b))
                except TelegramBadRequest: pass
                await callback.answer(f"💥 -{dmg} HP!")

@router.message(Command("wipe"))
async def cmd_wipe(message: Message, bot: Bot) -> None:
    chat_id = message.chat.id
    if message.chat.type == "private": return
    member = await bot.get_chat_member(chat_id, message.from_user.id)
    if member.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR) and message.from_user.id not in ADMIN_IDS:
        await message.reply("❌ Только для админов чата или глобальных админов бота!")
        return
    async with locks.get_lock(f"chat:{chat_id}"):
        async with aiosqlite.connect(DB_PATH) as db:
            now = int(time.time())
            await db.execute("DELETE FROM players WHERE chat_id = ?", (chat_id,))
            await db.execute("DELETE FROM bases WHERE chat_id = ?", (chat_id,))
            await db.execute("DELETE FROM clans WHERE chat_id = ?", (chat_id,))
            await db.execute("INSERT OR REPLACE INTO servers (chat_id, last_wipe_time) VALUES (?, ?)", (chat_id, now))
            await db.commit()
    await message.reply("☢️ <b>ПОЛНЫЙ ВАЙП СЕРВЕРА!</b> ☢️\nВсе постройки и ресурсы обнулены. Пишите /start!")

@router.message(Command("chatid"))
async def cmd_chatid(message: Message) -> None:
    await message.reply(f"📡 ID этого чата (сервера): <code>{message.chat.id}</code>")

# ==============================================================================
# СИСТЕМА КЛАНОВ (ВОССТАНОВЛЕНА)
# ==============================================================================
async def render_clan_info(db: aiosqlite.Connection, player: aiosqlite.Row, base: aiosqlite.Row) -> str:
    if not player["clan_id"]:
        return (
            "👥 <b>Система кланов (до 4 человек)</b>\n\n"
            "Вы сейчас выживаете в одиночку.\n"
            "В клане у всех игроков <b>общая база, общий склад и шкаф</b>, "
            "а фармить ресурсы могут все участники одновременно!\n\n"
            "📌 <b>Команды клана:</b>\n"
            "• <code>/clan create ТЕГ Название</code> — создать клан (стоит 1 000 🌲 Дерева)\n"
            "• <code>/clan invite @username</code> — пригласить игрока (или ответом на его сообщение)\n"
            "• <code>/clan role @username officer|member</code> — выдать роль\n"
            "• <code>/clan kick @username</code> — исключить игрока\n"
            "• <code>/clan leave</code> — покинуть клан"
        )
    clan, _ = await get_clan_info(db, player["clan_id"])
    async with db.execute("SELECT * FROM players WHERE clan_id = ? ORDER BY id ASC", (player["clan_id"],)) as cur:
        members = await cur.fetchall()
    now = int(time.time())
    role_icons = {"leader": "👑 Лидер", "officer": "⭐ Офицер", "member": "👷 Участник"}
    member_lines = []
    for m in members:
        if m["farm_resource"] and m["farm_end_time"] and m["farm_end_time"] > now:
            f_name = FARM_CONFIG[m["farm_resource"]]["name"]
            rem = format_duration(m["farm_end_time"] - now)
            status = f"⛏ Фармит {f_name} ({rem})"
        else:
            status = "💤 Свободен"
        member_lines.append(f"• {role_icons.get(m['clan_role'], '👷')} @{m['username']} — {status}")
    return (
        f"🏴 <b>Клан [{clan['tag']}] {clan['name']}</b> ({len(members)}/{CLAN_MAX_MEMBERS})\n"
        f"🏠 База клана: <b>{BASE_TIERS[base['tier']]['name']}</b> (<code>{base['hp']}/{base['max_hp']} HP</code>)\n\n"
        f"👥 <b>Состав клана:</b>\n" + "\n".join(member_lines) + "\n\n"
        "📌 Управление: <code>/clan invite @user</code> | <code>/clan role @user officer</code> | "
        "<code>/clan kick @user</code> | <code>/clan leave</code>"
    )

@router.message(Command("clan"))
@router.message(F.text.lower() == "клан")
async def cmd_clan(message: Message, command: Optional[CommandObject] = None) -> None:
    chat_id = message.chat.id
    user_id = message.from_user.id
    username = get_username_str(message.from_user)
    args = (command.args or "").strip().split() if command and command.args else []

    async with locks.get_lock(f"chat:{chat_id}"):
        async with aiosqlite.connect(DB_PATH) as db:
            player, base, _ = await ensure_server_and_player(db, chat_id, user_id, username)
            if not args or args[0].lower() == "info":
                text = await render_clan_info(db, player, base)
                await message.reply(text, reply_markup=main_menu_kb())
                return

            sub = args[0].lower()
            if sub == "create":
                if player["clan_id"]:
                    await message.reply("❌ Вы уже состоите в клане!")
                    return
                if len(args) < 3:
                    await message.reply("Использование: <code>/clan create ТЕГ Название клана</code>\nПример: <code>/clan create OXIDE Короли Острова</code>")
                    return
                tag = args[1].upper()
                name = " ".join(args[2:])[:30]
                if not (2 <= len(tag) <= 5) or not tag.isalnum():
                    await message.reply("❌ Тег клана должен быть от 2 до 5 букв/цифр!")
                    return
                if base["wood"] < CLAN_CREATE_COST_WOOD:
                    await message.reply(f"❌ Для создания клана нужно {CLAN_CREATE_COST_WOOD} 🌲 Дерева (у вас {base['wood']}).")
                    return
                try:
                    cur = await db.execute("INSERT INTO clans (chat_id, tag, name, leader_id) VALUES (?, ?, ?, ?)", (chat_id, tag, name, user_id))
                    clan_id = cur.lastrowid
                except aiosqlite.IntegrityError:
                    await message.reply(f"❌ Клан с тегом [{tag}] уже существует на этом сервере!")
                    return
                await db.execute("UPDATE bases SET wood = wood - ?, owner_user_id = NULL, owner_clan_id = ? WHERE id = ?", (CLAN_CREATE_COST_WOOD, clan_id, base["id"]))
                await db.execute("UPDATE players SET clan_id = ?, clan_role = 'leader' WHERE id = ?", (clan_id, player["id"]))
                await db.commit()
                await message.reply(f"🏴 <b>Клан [{tag}] {name} успешно создан!</b>\nВаша база стала базой клана. Приглашайте друзей командой <code>/clan invite @username</code>.")

            elif sub == "invite":
                if not player["clan_id"] or player["clan_role"] not in ("leader", "officer"):
                    await message.reply("❌ Приглашать игроков могут только Лидер и Офицер клана!")
                    return
                clan, count = await get_clan_info(db, player["clan_id"])
                if count >= CLAN_MAX_MEMBERS:
                    await message.reply(f"❌ В клане уже максимум участников ({CLAN_MAX_MEMBERS})!")
                    return
                target_player = None
                if message.reply_to_message and message.reply_to_message.from_user:
                    t_uid = message.reply_to_message.from_user.id
                    t_uname = get_username_str(message.reply_to_message.from_user)
                    target_player, _, _ = await ensure_server_and_player(db, chat_id, t_uid, t_uname)
                elif len(args) >= 2:
                    t_uname = args[1].lstrip("@")
                    async with db.execute("SELECT * FROM players WHERE chat_id = ? AND lower(username) = lower(?)", (chat_id, t_uname)) as cur:
                        target_player = await cur.fetchone()
                if not target_player:
                    await message.reply("❌ Игрок не найден в этом чате! Ответьте командой на его сообщение или укажите @username.")
                    return
                if target_player["clan_id"]:
                    await message.reply("❌ Этот игрок уже состоит в клане!")
                    return
                kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Принять приглашение", callback_data=f"clan_join:{clan['id']}:{target_player['user_id']}"), InlineKeyboardButton(text="❌ Отклонить", callback_data=f"clan_decline:{target_player['user_id']}")]])
                await message.reply(f"📨 @{target_player['username']}, вас приглашают в клан <b>[{clan['tag']}] {clan['name']}</b>!\n⚠️ <i>При вступлении ваша личная постройка разрушится, а все ваши ресурсы перейдут на общий склад клана.</i>", reply_markup=kb)

            elif sub == "role":
                if not player["clan_id"] or player["clan_role"] != "leader":
                    await message.reply("❌ Назначать офицеров может только Лидер клана!")
                    return
                if len(args) < 3 or args[2].lower() not in ("officer", "member"):
                    await message.reply("Использование: <code>/clan role @username officer</code> (или <code>member</code>)")
                    return
                t_uname = args[1].lstrip("@")
                new_role = args[2].lower()
                async with db.execute("SELECT * FROM players WHERE chat_id = ? AND clan_id = ? AND lower(username) = lower(?)", (chat_id, player["clan_id"], t_uname)) as cur:
                    t_pl = await cur.fetchone()
                if not t_pl or t_pl["user_id"] == user_id:
                    await message.reply("❌ Участник клана не найден!")
                    return
                await db.execute("UPDATE players SET clan_role = ? WHERE id = ?", (new_role, t_pl["id"]))
                await db.commit()
                await message.reply(f"✅ Роль игрока @{t_pl['username']} изменена на <b>{new_role}</b>!")

            elif sub in ("kick", "leave"):
                if not player["clan_id"]:
                    await message.reply("❌ Вы не состоите в клане!")
                    return
                if sub == "leave":
                    if player["clan_role"] == "leader":
                        clan_id = player["clan_id"]
                        async with db.execute("SELECT * FROM players WHERE clan_id = ? AND user_id != ?", (clan_id, user_id)) as cur:
                            others = await cur.fetchall()
                        for m in others:
                            c_base = await db.execute("INSERT INTO bases (chat_id, owner_user_id) VALUES (?, ?)", (chat_id, m["user_id"]))
                            await db.execute("UPDATE players SET clan_id = NULL, clan_role = NULL, base_id = ? WHERE id = ?", (c_base.lastrowid, m["id"]))
                        await db.execute("UPDATE bases SET owner_clan_id = NULL, owner_user_id = ? WHERE id = ?", (user_id, base["id"]))
                        await db.execute("UPDATE players SET clan_id = NULL, clan_role = NULL WHERE id = ?", (player["id"],))
                        await db.execute("DELETE FROM clans WHERE id = ?", (clan_id,))
                        await db.commit()
                        await message.reply("🏳 Вы распустили клан. База осталась в вашем личном владении.")
                    else:
                        c_base = await db.execute("INSERT INTO bases (chat_id, owner_user_id) VALUES (?, ?)", (chat_id, user_id))
                        await db.execute("UPDATE players SET clan_id = NULL, clan_role = NULL, base_id = ? WHERE id = ?", (c_base.lastrowid, player["id"]))
                        await db.commit()
                        await message.reply("🚪 Вы покинули клан и начали выживание с чистого листа.")
                elif sub == "kick":
                    if player["clan_role"] != "leader" or len(args) < 2:
                        await message.reply("Использование (только для Лидера): <code>/clan kick @username</code>")
                        return
                    t_uname = args[1].lstrip("@")
                    async with db.execute("SELECT * FROM players WHERE chat_id = ? AND clan_id = ? AND lower(username) = lower(?)", (chat_id, player["clan_id"], t_uname)) as cur:
                        t_pl = await cur.fetchone()
                    if not t_pl or t_pl["user_id"] == user_id:
                        await message.reply("❌ Игрок не найден в вашем клане!")
                        return
                    c_base = await db.execute("INSERT INTO bases (chat_id, owner_user_id) VALUES (?, ?)", (chat_id, t_pl["user_id"]))
                    await db.execute("UPDATE players SET clan_id = NULL, clan_role = NULL, base_id = ? WHERE id = ?", (c_base.lastrowid, t_pl["id"]))
                    await db.commit()
                    await message.reply(f"👢 Игрок @{t_pl['username']} исключён из клана.")

@router.callback_query(F.data.startswith("clan_join:"))
async def cb_clan_join(callback: CallbackQuery) -> None:
    _, clan_id_str, target_uid_str = callback.data.split(":")
    clan_id = int(clan_id_str)
    target_uid = int(target_uid_str)
    if callback.from_user.id != target_uid:
        await callback.answer("Это приглашение предназначено другому игроку!", show_alert=True)
        return
    chat_id = callback.message.chat.id
    username = get_username_str(callback.from_user)
    async with locks.get_lock(f"chat:{chat_id}"):
        async with aiosqlite.connect(DB_PATH) as db:
            player, old_base, _ = await ensure_server_and_player(db, chat_id, target_uid, username)
            if player["clan_id"]:
                await callback.answer("Вы уже состоите в клане!", show_alert=True)
                return
            clan, count = await get_clan_info(db, clan_id)
            if not clan or count >= CLAN_MAX_MEMBERS:
                await callback.answer("Клан не найден или уже заполнен!", show_alert=True)
                return
            async with db.execute("SELECT * FROM bases WHERE owner_clan_id = ? AND chat_id = ?", (clan_id, chat_id)) as cur:
                clan_base = await cur.fetchone()
            if not clan_base:
                await callback.answer("Ошибка базы клана!", show_alert=True)
                return
            await db.execute("UPDATE bases SET wood = wood + ?, stone = stone + ?, metal = metal + ?, hqm = hqm + ?, sulfur = sulfur + ?, gunpowder = gunpowder + ?, satchel = satchel + ?, rocket = rocket + ?, c4 = c4 + ? WHERE id = ?", (old_base["wood"] + old_base["tc_wood"], old_base["stone"] + old_base["tc_stone"], old_base["metal"] + old_base["tc_metal"], old_base["hqm"] + old_base["tc_hqm"], old_base["sulfur"], old_base["gunpowder"], old_base["satchel"], old_base["rocket"], old_base["c4"], clan_base["id"]))
            await db.execute("UPDATE players SET clan_id = ?, clan_role = 'member', base_id = ? WHERE id = ?", (clan_id, clan_base["id"], player["id"]))
            await db.execute("DELETE FROM bases WHERE id = ?", (old_base["id"],))
            await db.commit()
            try: await callback.message.edit_text(f"🎉 <b>@{player['username']} вступил в клан [{clan['tag']}] {clan['name']}!</b>\nЕго ресурсы перенесены на общий склад клана.")
            except TelegramBadRequest: pass
            await callback.answer("Добро пожаловать в клан!")

@router.callback_query(F.data.startswith("clan_decline:"))
async def cb_clan_decline(callback: CallbackQuery) -> None:
    target_uid = int(callback.data.split(":")[1])
    if callback.from_user.id != target_uid:
        await callback.answer("Это не ваше приглашение!", show_alert=True)
        return
    try: await callback.message.edit_text("❌ Приглашение в клан отклонено.")
    except TelegramBadRequest: pass

# ==============================================================================
# АДМИН-ПАНЕЛЬ
# ==============================================================================
@router.message(Command("admin"))
async def cmd_admin(message: Message) -> None:
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("❌ Доступ запрещён."); return
    await message.reply("⚙️ <b>Панель управления ботом</b>", reply_markup=admin_menu_kb())

@router.callback_query(F.data.startswith("admin:"))
async def cb_admin(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Доступ запрещён", show_alert=True); return
    action = callback.data.split(":")[1]
    
    if action == "stats":
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT COUNT(DISTINCT chat_id) FROM players") as cur: srv_count = (await cur.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM players") as cur: pl_count = (await cur.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM bases WHERE tier > 0") as cur: b_count = (await cur.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM clans") as cur: c_count = (await cur.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM official_servers") as cur: off_count = (await cur.fetchone())[0]
        text = (
            f"📊 <b>Глобальная статистика бота</b>\n\n"
            f"🌐 Всего серверов (чатов): <b>{srv_count}</b>\n"
            f"⭐ Официальных серверов: <b>{off_count}</b>\n"
            f"👤 Игроков всего: <b>{pl_count}</b>\n"
            f"🏠 Построено баз: <b>{b_count}</b>\n"
            f"🏴 Создано кланов: <b>{c_count}</b>"
        )
        try: await callback.message.edit_text(text, reply_markup=admin_menu_kb())
        except TelegramBadRequest: pass
        await callback.answer()
        
    elif action == "add_ch":
        await callback.message.edit_text("🆔 <b>Шаг 1/3:</b> Введите числовой ID канала (например, -1001234567890).\n<i>Узнать ID можно через @userinfobot.</i>")
        await state.set_state(AdminStates.waiting_for_channel_id)
        await callback.answer()

    elif action == "add_srv":
        await callback.message.edit_text("⭐ <b>Добавить официальный сервер</b>\n\nВведите ID чата, который станет официальным:\n<i>(узнать ID можно командой /chatid в этом чате)</i>")
        await state.set_state(AdminStates.waiting_for_server_id)
        await callback.answer()

    elif action == "del_ch":
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM channels") as cur: channels = await cur.fetchall()
        if not channels:
            await callback.answer("Список пуст!", show_alert=True); return
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"❌ {ch['title'] or ch['channel_id']}", callback_data=f"del_ch_confirm:{ch['id']}")] for ch in channels])
        kb.inline_keyboard.append([InlineKeyboardButton(text="🔙 Назад", callback_data="admin:back")])
        try: await callback.message.edit_text("Выберите канал для удаления:", reply_markup=kb)
        except TelegramBadRequest: pass
        await callback.answer()

    elif action == "list_ch":
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM channels") as cur: channels = await cur.fetchall()
            async with db.execute("SELECT * FROM official_servers") as cur: servers = await cur.fetchall()
        text = "📋 <b>Каналы:</b>\n"
        if not channels: text += "Нет каналов.\n"
        else:
            for ch in channels:
                t = "📢 Новостной" if ch["type"] == "news" else "💎 Спонсорский"
                text += f"• {ch['title'] or ch['channel_id']} (<code>{ch['channel_id']}</code>) — {t}\n"
        text += "\n⭐ <b>Официальные серверы:</b>\n"
        if not servers: text += "Нет официальных серверов."
        else:
            for srv in servers:
                text += f"• <code>{srv['chat_id']}</code> ({srv['title'] or 'Без названия'})\n"
        try: await callback.message.edit_text(text, reply_markup=admin_menu_kb())
        except TelegramBadRequest: pass
        await callback.answer()

    elif action == "back":
        try: await callback.message.edit_text("⚙️ <b>Панель управления ботом</b>", reply_markup=admin_menu_kb())
        except TelegramBadRequest: pass
        await callback.answer()

@router.callback_query(F.data.startswith("del_ch_confirm:"))
async def cb_del_ch_confirm(callback: CallbackQuery) -> None:
    if callback.from_user.id not in ADMIN_IDS: return
    ch_id = int(callback.data.split(":")[1])
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM channels WHERE id = ?", (ch_id,))
        await db.commit()
    await callback.answer("Канал удалён!", show_alert=True)
    try: await callback.message.edit_text("⚙️ <b>Панель управления ботом</b>", reply_markup=admin_menu_kb())
    except TelegramBadRequest: pass

@router.message(StateFilter(AdminStates.waiting_for_channel_id))
async def process_channel_id(message: Message, state: FSMContext) -> None:
    if message.from_user.id not in ADMIN_IDS: return
    cid = message.text.strip()
    if not (cid.startswith("-100") or cid.lstrip("-").isdigit()):
        await message.reply("❌ Неверный формат ID! Он должен выглядеть как -100xxxxxxxxxx")
        return
    await state.update_data(channel_id=cid)
    await message.reply("🔗 <b>Шаг 2/3:</b> Введите ссылку на канал (например, https://t.me/my_channel).")
    await state.set_state(AdminStates.waiting_for_channel_link)

@router.message(StateFilter(AdminStates.waiting_for_channel_link))
async def process_channel_link(message: Message, state: FSMContext) -> None:
    if message.from_user.id not in ADMIN_IDS: return
    await state.update_data(channel_link=message.text.strip())
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Новостной", callback_data="set_ch_type:news")],
        [InlineKeyboardButton(text="💎 Спонсорский", callback_data="set_ch_type:sponsor")]
    ])
    await message.reply("🏷️ <b>Шаг 3/3:</b> Выберите тип канала:", reply_markup=kb)
    await state.set_state(AdminStates.waiting_for_channel_type)

@router.callback_query(F.data.startswith("set_ch_type:"))
async def process_channel_type(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.from_user.id not in ADMIN_IDS: return
    ch_type = callback.data.split(":")[1]
    data = await state.get_data()
    cid = data.get("channel_id")
    link = data.get("channel_link")
    title = link
    try:
        chat = await callback.bot.get_chat(cid)
        title = chat.title or link
    except Exception:
        pass
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute("INSERT INTO channels (channel_id, link, title, type) VALUES (?, ?, ?, ?)", (cid, link, title, ch_type))
            await db.commit()
            await callback.message.edit_text(f"✅ Канал <b>{title}</b> (ID: <code>{cid}</code>) добавлен как {'📢 Новостной' if ch_type=='news' else '💎 Спонсорский'}!", reply_markup=admin_menu_kb())
        except aiosqlite.IntegrityError:
            await callback.message.edit_text("❌ Канал с таким ID уже существует!", reply_markup=admin_menu_kb())
    await callback.answer()
    await state.clear()

@router.message(StateFilter(AdminStates.waiting_for_server_id))
async def process_server_id(message: Message, state: FSMContext) -> None:
    if message.from_user.id not in ADMIN_IDS: return
    sid = message.text.strip()
    if not (sid.lstrip("-").isdigit()):
        await message.reply("❌ Неверный формат ID! Введите только числовой ID чата.")
        return
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            title = "Официальный сервер"
            try:
                chat = await message.bot.get_chat(int(sid))
                title = chat.title or title
            except Exception:
                pass
            await db.execute("INSERT OR REPLACE INTO official_servers (chat_id, title) VALUES (?, ?)", (int(sid), title))
            await db.commit()
            await message.reply(f"✅ Чат <code>{sid}</code> теперь является официальным сервером!", reply_markup=admin_menu_kb())
        except Exception as e:
            await message.reply(f"❌ Ошибка при добавлении сервера: {e}", reply_markup=admin_menu_kb())
    await state.clear()

# ==============================================================================
# ФОНОВЫЕ ЗАДАЧИ
# ==============================================================================
async def hourly_tc_and_decay_worker(bot: Bot) -> None:
    while True:
        await asyncio.sleep(3600)
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("SELECT * FROM bases WHERE tier > 0") as cur: active_bases = await cur.fetchall()
                for base in active_bases:
                    chat_id = base["chat_id"]
                    async with locks.get_lock(f"chat:{chat_id}"):
                        _, clan_members = await get_clan_info(db, base["owner_clan_id"])
                        upkeep = calculate_hourly_upkeep(base["tier"], clan_members, base["doors_level"])
                        can_pay = all(base[f"tc_{res_k}"] >= amt for res_k, amt in upkeep.items())
                        if can_pay:
                            set_parts = [f"tc_{k} = tc_{k} - {v}" for k, v in upkeep.items()]
                            set_parts.append("is_decaying = 0")
                            await db.execute(f"UPDATE bases SET {', '.join(set_parts)} WHERE id = ?", (base["id"],))
                        else:
                            decay_dmg = max(1, int(base["max_hp"] * 0.10))
                            new_hp = max(0, base["hp"] - decay_dmg)
                            if new_hp == 0:
                                await db.execute("UPDATE bases SET tier=0, doors_level=0, hp=0, max_hp=0, is_decaying=0 WHERE id=?", (base["id"],))
                                try: await bot.send_message(chat_id, f"🏚 <b>База сгнила из-за пустого шкафа!</b>")
                                except Exception: pass
                            else:
                                await db.execute("UPDATE bases SET hp=?, is_decaying=1 WHERE id=?", (new_hp, base["id"]))
                        await db.commit()
        except Exception as e: logging.error(f"Ошибка TC/Decay: {e}")

async def daily_raids_reset_worker() -> None:
    while True:
        await asyncio.sleep(86400)
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("UPDATE bases SET raids_today = 0")
                await db.commit()
        except Exception as e: logging.error(f"Ошибка сброса рейдов: {e}")

# ==============================================================================
# MAIN
# ==============================================================================
async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    if BOT_TOKEN == "PASTE_YOUR_BOT_TOKEN_HERE": logging.warning("⚠️ Токен не найден!")
    await init_db()
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.message.middleware(ThrottlingMiddleware(rate_limit=0.8))
    dp.callback_query.middleware(ThrottlingMiddleware(rate_limit=0.8))
    dp.include_router(router)
    asyncio.create_task(hourly_tc_and_decay_worker(bot))
    asyncio.create_task(daily_raids_reset_worker())
    logging.info("🚀 Бот «Oxide: Survival Island Chat RPG» v2.6 успешно запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): logging.info("Бот остановлен.")
