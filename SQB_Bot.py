# sqb_bot.py
# SQB signup bot implementing stacked/C-style columns layout with persistence.
#
# Requirements:
#   pip install -U "discord.py>=2.3.0" aiosqlite apscheduler python-dotenv
#
# .env must contain:
#   DISCORD_TOKEN=your_token
#   GUILD_ID=your_test_guild_id

import os
import asyncio
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo

import aiosqlite
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import discord
from discord.ext import commands
from discord import app_commands
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID") or 0)

if not TOKEN or not GUILD_ID:
    raise SystemExit("Set DISCORD_TOKEN and GUILD_ID in .env")


# ---------------- Configurable Constants ----------------
DB_FILE = "sqb_bot.db"
TIMEZONE = ZoneInfo("Europe/Stockholm")

MAX_AIR = 4
MAX_GROUND = 8
MAX_MAIN = 8
MAX_RESERVE = 6

INIT_AIR_DISPLAY = 2
INIT_GROUND_DISPLAY = 4
VEHICLE_CHAR_LIMIT = 12
UNDECIDED_REASON_CHAR_LIMIT = 60
BR_CHAR_LIMIT = 12

AIR_ROLES = {"Fighter", "Heli", "Bomber"}
GROUND_ROLES = {"MBT", "IFV", "SPAA"}

SECTION_AIR = "air"
SECTION_GROUND = "ground"
SECTION_RESERVE = "reserve"
SECTION_UNDECIDED = "undecided"
SECTION_UNAVAILABLE = "unavailable"
LEGACY_SECTION_MIA = "mia"

MAIN_SECTIONS = {SECTION_AIR, SECTION_GROUND}

intents = discord.Intents.default()
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree
scheduler = AsyncIOScheduler(timezone=str(TIMEZONE))

poll_message_map = {}   # poll_id -> message_id
scheduled_jobs = {}     # (channel_id, day, time) -> scheduler job

WEEKDAY_MAP = {
    "mon": 1, "monday": 1,
    "tue": 2, "tuesday": 2,
    "wed": 3, "wednesday": 3,
    "thu": 4, "thursday": 4,
    "fri": 5, "friday": 5,
    "sat": 6, "saturday": 6,
    "sun": 7, "sunday": 7
}

DAY_FULL = {
    "mon": "Monday", "monday": "Monday",
    "tue": "Tuesday", "tuesday": "Tuesday",
    "wed": "Wednesday", "wednesday": "Wednesday",
    "thu": "Thursday", "thursday": "Thursday",
    "fri": "Friday", "friday": "Friday",
    "sat": "Saturday", "saturday": "Saturday",
    "sun": "Sunday", "sunday": "Sunday"
}


# ---------------- DB Helpers ----------------
async def column_exists(db: aiosqlite.Connection, table_name: str, column_name: str) -> bool:
    cur = await db.execute(f"PRAGMA table_info({table_name})")
    rows = await cur.fetchall()
    return any(row[1] == column_name for row in rows)


async def init_db():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.executescript("""
        CREATE TABLE IF NOT EXISTS roles(
            name TEXT PRIMARY KEY
        );

        CREATE TABLE IF NOT EXISTS polls(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            day TEXT,
            time TEXT,
            channel_id INTEGER,
            creator_id INTEGER,
            ping_role_id INTEGER,
            is_open INTEGER DEFAULT 1,
            created_at INTEGER,
            message_id INTEGER,
            max_air_override INTEGER,
            max_ground_override INTEGER,
            header_text TEXT
        );

        CREATE TABLE IF NOT EXISTS slots(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            poll_id INTEGER,
            section TEXT,
            desired_section TEXT,
            slot_index INTEGER,
            user_id INTEGER,
            username TEXT,
            role TEXT,
            info TEXT,
            added_at INTEGER
        );

        CREATE TABLE IF NOT EXISTS autos(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            day TEXT,
            time TEXT,
            channel_id INTEGER,
            creator_id INTEGER,
            ping_role_id INTEGER,
            max_air_override INTEGER,
            max_ground_override INTEGER,
            header_text TEXT
        );
        """)

        if not await column_exists(db, "slots", "desired_section"):
            await db.execute("ALTER TABLE slots ADD COLUMN desired_section TEXT")

        if not await column_exists(db, "polls", "br_text"):
            await db.execute("ALTER TABLE polls ADD COLUMN br_text TEXT")

        if not await column_exists(db, "autos", "br_text"):
            await db.execute("ALTER TABLE autos ADD COLUMN br_text TEXT")

        await db.execute("""
            UPDATE slots
            SET desired_section = CASE
                WHEN role IN ('Fighter', 'Heli', 'Bomber') THEN 'air'
                WHEN role IN ('MBT', 'IFV', 'SPAA') THEN 'ground'
                ELSE desired_section
            END
            WHERE desired_section IS NULL
        """)

        await db.execute(
            "UPDATE slots SET section = ? WHERE section = ?",
            (SECTION_UNAVAILABLE, LEGACY_SECTION_MIA)
        )

        await db.commit()


async def fetch_allowed_role_names() -> set[str]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT name FROM roles")
        rows = await cur.fetchall()
    return {row[0] for row in rows}


async def member_is_allowed(member: discord.Member) -> bool:
    if member.guild_permissions.manage_guild:
        return True

    allowed_role_names = await fetch_allowed_role_names()
    member_role_names = {role.name for role in member.roles}
    return bool(allowed_role_names & member_role_names)


async def fetch_poll(poll_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM polls WHERE id = ?", (poll_id,))
        row = await cur.fetchone()
    return dict(row) if row else None


async def create_poll_record(
    title: str,
    day: str,
    time_str: str,
    channel_id: int,
    creator_id: int,
    ping_role_id: int | None = None,
    max_air: int | None = None,
    max_ground: int | None = None,
    header_text: str | None = None,
    br_text: str | None = None
) -> int:
    now_ts = int(datetime.now().timestamp())

    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("""
            INSERT INTO polls(
                title, day, time, channel_id, creator_id, ping_role_id,
                max_air_override, max_ground_override, header_text, br_text, created_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
        """, (
            title,
            day,
            time_str,
            channel_id,
            creator_id,
            ping_role_id,
            max_air,
            max_ground,
            header_text,
            br_text,
            now_ts
        ))
        await db.commit()
        return cur.lastrowid


async def get_slot_counts(db: aiosqlite.Connection, poll_id: int):
    cur = await db.execute(
        "SELECT COUNT(*) FROM slots WHERE poll_id = ? AND section IN (?, ?)",
        (poll_id, SECTION_AIR, SECTION_GROUND)
    )
    main_count = (await cur.fetchone())[0]

    cur = await db.execute("SELECT COUNT(*) FROM slots WHERE poll_id = ? AND section = ?", (poll_id, SECTION_AIR))
    air_count = (await cur.fetchone())[0]

    cur = await db.execute("SELECT COUNT(*) FROM slots WHERE poll_id = ? AND section = ?", (poll_id, SECTION_GROUND))
    ground_count = (await cur.fetchone())[0]

    cur = await db.execute("SELECT COUNT(*) FROM slots WHERE poll_id = ? AND section = ?", (poll_id, SECTION_RESERVE))
    reserve_count = (await cur.fetchone())[0]

    cur = await db.execute("SELECT COUNT(*) FROM slots WHERE poll_id = ? AND section = ?", (poll_id, SECTION_UNDECIDED))
    undecided_count = (await cur.fetchone())[0]

    cur = await db.execute("SELECT COUNT(*) FROM slots WHERE poll_id = ? AND section = ?", (poll_id, SECTION_UNAVAILABLE))
    unavailable_count = (await cur.fetchone())[0]

    return main_count, air_count, ground_count, reserve_count, undecided_count, unavailable_count


async def insert_slot(
    db: aiosqlite.Connection,
    poll_id: int,
    section: str,
    slot_index: int,
    user_id: int,
    username: str,
    role: str,
    info: str,
    desired_section: str | None
):
    await db.execute("""
        INSERT INTO slots(
            poll_id, section, desired_section, slot_index,
            user_id, username, role, info, added_at
        )
        VALUES(?,?,?,?,?,?,?,?,?)
    """, (
        poll_id, section, desired_section, slot_index,
        user_id, username, role, info, int(datetime.now().timestamp())
    ))


async def delete_user_slots(db: aiosqlite.Connection, poll_id: int, user_id: int):
    await db.execute("DELETE FROM slots WHERE poll_id = ? AND user_id = ?", (poll_id, user_id))


async def lowest_free_slot_index(db: aiosqlite.Connection, poll_id: int, section: str) -> int:
    cur = await db.execute(
        "SELECT slot_index FROM slots WHERE poll_id = ? AND section = ? ORDER BY slot_index",
        (poll_id, section)
    )
    used = [row[0] for row in await cur.fetchall()]
    idx = 0
    while idx in used:
        idx += 1
    return idx


async def renumber_section_slots(db: aiosqlite.Connection, poll_id: int, section: str):
    cur = await db.execute(
        "SELECT id FROM slots WHERE poll_id = ? AND section = ? ORDER BY slot_index, added_at",
        (poll_id, section)
    )
    rows = await cur.fetchall()

    for i, row in enumerate(rows):
        await db.execute("UPDATE slots SET slot_index = ? WHERE id = ?", (i, row[0]))


async def renumber_reserve_slots(db: aiosqlite.Connection, poll_id: int):
    await renumber_section_slots(db, poll_id, SECTION_RESERVE)


async def renumber_unordered_sections(db: aiosqlite.Connection, poll_id: int):
    await renumber_section_slots(db, poll_id, SECTION_UNDECIDED)
    await renumber_section_slots(db, poll_id, SECTION_UNAVAILABLE)


# ---------------- Time Helpers ----------------
def parse_weekday(day: str):
    return WEEKDAY_MAP.get(day.lower())


def next_occurrence_epoch(day: str, time_str: str) -> int:
    dow = parse_weekday(day)
    if dow is None:
        raise ValueError("Invalid weekday")

    hh, mm = map(int, time_str.split(":"))
    now = datetime.now(TIMEZONE)

    days_ahead = (dow - now.isoweekday() + 7) % 7
    candidate_date = (now + timedelta(days=days_ahead)).date()
    candidate = datetime.combine(candidate_date, dtime(hh, mm), tzinfo=TIMEZONE)

    if candidate <= now:
        candidate += timedelta(days=7)

    return int(candidate.timestamp())


# ---------------- Caps Compute ----------------
def compute_poll_caps(poll_row):
    raw_air = poll_row.get("max_air_override")
    raw_ground = poll_row.get("max_ground_override")

    poll_air = int(raw_air) if raw_air not in (None, "") else MAX_AIR
    poll_ground = int(raw_ground) if raw_ground not in (None, "") else MAX_GROUND

    poll_air = min(poll_air, MAX_AIR)
    poll_ground = min(poll_ground, MAX_GROUND)

    return poll_air, poll_ground, MAX_MAIN


# ---------------- Small Helpers ----------------
def normalize_short_text(value: str | None, max_len: int) -> str:
    text = (value or "").strip()
    if len(text) > max_len:
        text = text[:max_len] + "…"
    return text


def infer_desired_section_from_role(role_name: str) -> str | None:
    if role_name in AIR_ROLES:
        return SECTION_AIR
    if role_name in GROUND_ROLES:
        return SECTION_GROUND
    return None


async def get_ping_mention_for_poll(poll_row, client: discord.Client) -> str:
    if not poll_row.get("ping_role_id"):
        return ""

    try:
        channel = client.get_channel(poll_row["channel_id"]) or await client.fetch_channel(poll_row["channel_id"])
        guild = channel.guild if channel else None

        if guild:
            role = guild.get_role(int(poll_row["ping_role_id"]))
            return role.mention if role else f"<@&{poll_row['ping_role_id']}>"

        return f"<@&{poll_row['ping_role_id']}>"
    except Exception:
        return ""


# ---------------- Embed Builder ----------------
async def build_poll_embed_and_view(poll_row, ping_mention: str = ""):
    poll_id = poll_row["id"]
    title = poll_row["title"] or "SQB Signups"
    day = poll_row["day"]
    time_str = poll_row["time"]
    day_full = DAY_FULL.get(day.lower(), day.capitalize())
    header_text = (poll_row.get("header_text") or "").strip()
    br_text = (poll_row.get("br_text") or "").strip()

    epoch = next_occurrence_epoch(day, time_str)

    description_parts = []
    if header_text:
        description_parts.append(header_text)

    if ping_mention:
        if br_text:
            description_parts.append(f"{ping_mention} BR: ({br_text})")
        else:
            description_parts.append(ping_mention)
    elif br_text:
        description_parts.append(f"BR: ({br_text})")

    description = "\n".join(description_parts) if description_parts else ""

    embed = discord.Embed(
        title=f"[{title}] [{day_full}] SQB Signups",
        description=description,
        color=discord.Color.red()
    )
    embed.add_field(name="Time", value=f"<t:{epoch}:F> (Your Local Time)", inline=False)

    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("""
            SELECT section, desired_section, slot_index, user_id, username, role, info, added_at
            FROM slots
            WHERE poll_id = ?
            ORDER BY
                CASE section
                    WHEN 'air' THEN 0
                    WHEN 'ground' THEN 1
                    WHEN 'reserve' THEN 2
                    WHEN 'undecided' THEN 3
                    WHEN 'unavailable' THEN 4
                    ELSE 5
                END,
                slot_index,
                added_at
        """, (poll_id,))
        rows = await cur.fetchall()

    def rows_for(section: str):
        return [row for row in rows if row["section"] == section]

    def format_user(row):
        if row["user_id"]:
            return f"<@{row['user_id']}>"
        return row["username"]

    def build_main_lines(section_rows, max_slots):
        lines = []
        for i in range(max_slots):
            found = next((row for row in section_rows if row["slot_index"] == i), None)
            if not found:
                lines.append(f"{i+1}. — (empty)")
            else:
                user_text = format_user(found)
                info_text = f" • {found['info']}" if found["info"] else ""
                lines.append(f"{i+1}. {user_text} — {found['role']}{info_text}")
        return "\n".join(lines) or "_No Slots_"

    air_rows = rows_for(SECTION_AIR)
    ground_rows = rows_for(SECTION_GROUND)
    reserve_rows = rows_for(SECTION_RESERVE)
    undecided_rows = rows_for(SECTION_UNDECIDED)
    unavailable_rows = rows_for(SECTION_UNAVAILABLE)

    poll_air_cap, poll_ground_cap, poll_main_cap = compute_poll_caps(poll_row)

    current_air = len(air_rows)
    current_ground = len(ground_rows)

    display_air = max(INIT_AIR_DISPLAY, current_air + (1 if current_air < poll_air_cap else 0))
    display_air = min(display_air, poll_air_cap)

    display_ground = max(INIT_GROUND_DISPLAY, current_ground + (1 if current_ground < poll_ground_cap else 0))
    display_ground = min(display_ground, poll_ground_cap)

    target_total = INIT_AIR_DISPLAY + INIT_GROUND_DISPLAY

    while display_air + display_ground > poll_main_cap:
        if display_ground > max(INIT_GROUND_DISPLAY, current_ground):
            display_ground -= 1
        elif display_air > max(INIT_AIR_DISPLAY, current_air):
            display_air -= 1
        else:
            break

    while display_air + display_ground < target_total:
        if display_ground < poll_ground_cap:
            display_ground += 1
        elif display_air < poll_air_cap:
            display_air += 1
        else:
            break

        if display_air + display_ground > poll_main_cap:
            if display_ground > current_ground:
                display_ground -= 1
            elif display_air > current_air:
                display_air -= 1
            break

    air_text = build_main_lines(air_rows, display_air)
    ground_text = build_main_lines(ground_rows, display_ground)

    embed.add_field(name="Air", value=air_text, inline=False)
    embed.add_field(name="Ground", value=ground_text, inline=False)

    reserve_lines = []
    for i, row in enumerate(reserve_rows):
        desired = row["desired_section"].upper() if row["desired_section"] else "?"
        info_text = f" • {row['info']}" if row["info"] else ""
        reserve_lines.append(f"{i+1}. {format_user(row)} — {row['role']} [{desired}]{info_text}")
    reserve_text = "\n".join(reserve_lines) or "_No Reserves_"
    embed.add_field(name="Reserve", value=reserve_text, inline=False)

    undecided_lines = []
    for i, row in enumerate(undecided_rows):
        info_text = f" — {row['info']}" if row["info"] else ""
        undecided_lines.append(f"{i+1}. {format_user(row)}{info_text}")
    undecided_text = "\n".join(undecided_lines) or "_No Undecided Entries_"
    embed.add_field(name="Undecided / Backup", value=undecided_text, inline=False)

    unavailable_lines = []
    for i, row in enumerate(unavailable_rows):
        unavailable_lines.append(f"{i+1}. {format_user(row)}")
    unavailable_text = "\n".join(unavailable_lines) or "_No Unavailable Entries_"
    embed.add_field(name="Unavailable", value=unavailable_text, inline=False)

    view = PollView(poll_id, disabled=(not bool(poll_row.get("is_open", 1))))
    return embed, view


# ---------------- UI Components ----------------
class SignupSelect(discord.ui.Select):
    def __init__(self, poll_id: int):
        options = [
            discord.SelectOption(label="Air | Fighter", value="air|Fighter"),
            discord.SelectOption(label="Air | Heli", value="air|Heli"),
            discord.SelectOption(label="Air | Bomber", value="air|Bomber"),
            discord.SelectOption(label="Ground | MBT", value="ground|MBT"),
            discord.SelectOption(label="Ground | IFV", value="ground|IFV"),
            discord.SelectOption(label="Ground | SPAA", value="ground|SPAA"),
        ]
        super().__init__(
            placeholder="Choose Slot Type",
            min_values=1,
            max_values=1,
            options=options
        )
        self.poll_id = poll_id

    async def callback(self, interaction: discord.Interaction):
        value = self.values[0]
        section, role = value.split("|", 1)
        await interaction.response.send_modal(VehicleModal(self.poll_id, section, role))


class VehicleModal(discord.ui.Modal, title="Set Vehicle Info"):
    def __init__(self, poll_id: int, section: str, role_name: str):
        super().__init__()
        self.poll_id = poll_id
        self.section = section
        self.role_name = role_name

        self.vehicle = discord.ui.TextInput(
            label=f"Vehicle Or Note (Max {VEHICLE_CHAR_LIMIT} Chars)",
            required=False,
            max_length=120
        )
        self.add_item(self.vehicle)

    async def on_submit(self, interaction: discord.Interaction):
        info = normalize_short_text(self.vehicle.value, VEHICLE_CHAR_LIMIT)
        poll_id = self.poll_id
        uid = interaction.user.id
        uname = interaction.user.display_name if isinstance(interaction.user, discord.Member) else str(interaction.user)

        poll_row = await fetch_poll(poll_id)
        if not poll_row:
            await interaction.response.send_message("Poll Does Not Exist.", ephemeral=True)
            return

        poll_air_cap, poll_ground_cap, poll_main_cap = compute_poll_caps(poll_row)

        async with aiosqlite.connect(DB_FILE) as db:
            await delete_user_slots(db, poll_id, uid)
            await db.commit()

            main_count, air_count, ground_count, reserve_count, _, _ = await get_slot_counts(db, poll_id)

            placed_main = False

            if self.section == SECTION_AIR:
                if air_count < poll_air_cap and main_count < poll_main_cap:
                    idx = await lowest_free_slot_index(db, poll_id, SECTION_AIR)
                    await insert_slot(db, poll_id, SECTION_AIR, idx, uid, uname, self.role_name, info, SECTION_AIR)
                    placed_main = True

            elif self.section == SECTION_GROUND:
                if ground_count < poll_ground_cap and main_count < poll_main_cap:
                    idx = await lowest_free_slot_index(db, poll_id, SECTION_GROUND)
                    await insert_slot(db, poll_id, SECTION_GROUND, idx, uid, uname, self.role_name, info, SECTION_GROUND)
                    placed_main = True

            if placed_main:
                await db.commit()
                await refresh_poll_message(poll_id, interaction.client)
                await interaction.response.send_message(
                    f"Joined: {self.section.capitalize()} | {self.role_name}",
                    ephemeral=True
                )
                return

            if reserve_count >= MAX_RESERVE:
                await interaction.response.send_message(
                    f"Main Roster And Reserve Are Full. Reserve Cap: {MAX_RESERVE}.",
                    ephemeral=True
                )
                return

            reserve_idx = reserve_count
            await insert_slot(
                db,
                poll_id,
                SECTION_RESERVE,
                reserve_idx,
                uid,
                uname,
                self.role_name,
                info,
                self.section
            )
            await db.commit()

        await refresh_poll_message(poll_id, interaction.client)
        await interaction.response.send_message(
            f"Main Roster Full. Added To Reserve At Position #{reserve_idx + 1}.",
            ephemeral=True
        )


class UndecidedModal(discord.ui.Modal, title="Mark Undecided / Backup"):
    def __init__(self, poll_id: int):
        super().__init__()
        self.poll_id = poll_id

        self.reason = discord.ui.TextInput(
            label=f"Reason (Optional, Max {UNDECIDED_REASON_CHAR_LIMIT} Chars)",
            required=False,
            max_length=200
        )
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction):
        reason_text = normalize_short_text(self.reason.value, UNDECIDED_REASON_CHAR_LIMIT)
        poll_id = self.poll_id
        uid = interaction.user.id
        uname = interaction.user.display_name if isinstance(interaction.user, discord.Member) else str(interaction.user)

        async with aiosqlite.connect(DB_FILE) as db:
            await delete_user_slots(db, poll_id, uid)
            await renumber_reserve_slots(db, poll_id)
            await renumber_unordered_sections(db, poll_id)

            undecided_idx = await lowest_free_slot_index(db, poll_id, SECTION_UNDECIDED)
            await insert_slot(
                db,
                poll_id,
                SECTION_UNDECIDED,
                undecided_idx,
                uid,
                uname,
                "Undecided",
                reason_text,
                None
            )
            await db.commit()

        await refresh_poll_message(poll_id, interaction.client)
        await interaction.response.send_message("Marked As Undecided / Backup.", ephemeral=True)


class PollView(discord.ui.View):
    def __init__(self, poll_id: int, disabled: bool = False):
        super().__init__(timeout=None)
        self.poll_id = poll_id

        self.add_item(discord.ui.Button(
            label="Join",
            style=discord.ButtonStyle.success,
            custom_id=f"join:{poll_id}",
            disabled=disabled
        ))

        self.add_item(discord.ui.Button(
            label="Mark Undecided",
            style=discord.ButtonStyle.secondary,
            custom_id=f"undecided:{poll_id}",
            disabled=disabled
        ))

        self.add_item(discord.ui.Button(
            label="Mark Unavailable",
            style=discord.ButtonStyle.secondary,
            custom_id=f"unavailable:{poll_id}",
            disabled=disabled
        ))

        self.add_item(discord.ui.Button(
            label="End Poll" if not disabled else "Poll Closed",
            style=discord.ButtonStyle.danger,
            custom_id=f"close:{poll_id}",
            disabled=disabled
        ))


# ---------------- Refresh / Promote ----------------
async def refresh_poll_message(poll_id: int, client: discord.Client):
    poll_row = await fetch_poll(poll_id)
    if not poll_row:
        return

    ping_mention = await get_ping_mention_for_poll(poll_row, client)
    embed, view = await build_poll_embed_and_view(poll_row, ping_mention)

    channel_id = poll_row["channel_id"]
    message_id = poll_row.get("message_id")

    if message_id:
        try:
            channel = client.get_channel(channel_id) or await client.fetch_channel(channel_id)
            message = await channel.fetch_message(message_id)
            await message.edit(embed=embed, view=view)
            poll_message_map[poll_id] = message.id
            return
        except Exception:
            pass

    channel = client.get_channel(channel_id) or await client.fetch_channel(channel_id)
    message = await channel.send(embed=embed, view=view)
    poll_message_map[poll_id] = message.id

    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("UPDATE polls SET message_id = ? WHERE id = ?", (message.id, poll_id))
        await db.commit()


async def promote_from_reserve(poll_id: int, client: discord.Client):
    while True:
        poll_row = await fetch_poll(poll_id)
        if not poll_row:
            return

        poll_air_cap, poll_ground_cap, poll_main_cap = compute_poll_caps(poll_row)

        async with aiosqlite.connect(DB_FILE) as db:
            db.row_factory = aiosqlite.Row

            main_count, air_count, ground_count, _, _, _ = await get_slot_counts(db, poll_id)
            if main_count >= poll_main_cap:
                return

            cur = await db.execute("""
                SELECT id, desired_section, user_id, username, role, info
                FROM slots
                WHERE poll_id = ? AND section = ?
                ORDER BY slot_index, added_at
            """, (poll_id, SECTION_RESERVE))
            reserve_rows = await cur.fetchall()

            promotable = None

            for row in reserve_rows:
                desired_section = row["desired_section"]

                if desired_section == SECTION_AIR:
                    if air_count < poll_air_cap and main_count < poll_main_cap:
                        promotable = row
                        break

                elif desired_section == SECTION_GROUND:
                    if ground_count < poll_ground_cap and main_count < poll_main_cap:
                        promotable = row
                        break

            if not promotable:
                return

            target_section = promotable["desired_section"]
            new_index = await lowest_free_slot_index(db, poll_id, target_section)

            await db.execute("DELETE FROM slots WHERE id = ?", (promotable["id"],))
            await insert_slot(
                db,
                poll_id,
                target_section,
                new_index,
                promotable["user_id"],
                promotable["username"],
                promotable["role"],
                promotable["info"] or "",
                target_section
            )

            await renumber_reserve_slots(db, poll_id)
            await db.commit()

            promoted_user_id = promotable["user_id"]

        try:
            user = await client.fetch_user(promoted_user_id)
            await user.send(f"You Have Been Promoted To {target_section.upper()} Slot #{new_index + 1}.")
        except Exception:
            pass

        await refresh_poll_message(poll_id, client)


# ---------------- Scheduler Helpers ----------------
def schedule_auto_job(channel_id, day, time_str, creator_id, ping_role_id, max_air, max_ground, header_text, br_text):
    dow = parse_weekday(day)
    if dow is None:
        return None

    hh, mm = map(int, time_str.split(":"))
    day_name = day.lower()[:3]

    trigger = CronTrigger(day_of_week=day_name, hour=hh, minute=mm, timezone=TIMEZONE)

    def job_func(
        channel_id=channel_id,
        day=day,
        time_str=time_str,
        creator_id=creator_id,
        ping_role_id=ping_role_id,
        max_air=max_air,
        max_ground=max_ground,
        header_text=header_text,
        br_text=br_text
    ):
        async def coro():
            poll_id = await create_poll_record(
                title="Auto SQB",
                day=day,
                time_str=time_str,
                channel_id=channel_id,
                creator_id=creator_id,
                ping_role_id=ping_role_id,
                max_air=max_air,
                max_ground=max_ground,
                header_text=header_text,
                br_text=br_text
            )
            await refresh_poll_message(poll_id, bot)

        asyncio.create_task(coro())

    job = scheduler.add_job(job_func, trigger)
    scheduled_jobs[(channel_id, day, time_str)] = job
    return job


# ---------------- Interaction Handlers ----------------
async def handle_join(interaction: discord.Interaction, poll_id: int):
    select = SignupSelect(poll_id)
    view = discord.ui.View(timeout=60)
    view.add_item(select)
    await interaction.response.send_message("Choose Slot Type.", view=view, ephemeral=True)


async def handle_mark_undecided(interaction: discord.Interaction, poll_id: int):
    poll_row = await fetch_poll(poll_id)
    if not poll_row:
        await interaction.response.send_message("Poll Does Not Exist.", ephemeral=True)
        return

    await interaction.response.send_modal(UndecidedModal(poll_id))


async def handle_mark_unavailable(interaction: discord.Interaction, poll_id: int):
    poll_row = await fetch_poll(poll_id)
    if not poll_row:
        await interaction.response.send_message("Poll Does Not Exist.", ephemeral=True)
        return

    uid = interaction.user.id
    uname = interaction.user.display_name if isinstance(interaction.user, discord.Member) else str(interaction.user)

    async with aiosqlite.connect(DB_FILE) as db:
        await delete_user_slots(db, poll_id, uid)
        await renumber_reserve_slots(db, poll_id)
        await renumber_unordered_sections(db, poll_id)

        unavailable_idx = await lowest_free_slot_index(db, poll_id, SECTION_UNAVAILABLE)
        await insert_slot(
            db,
            poll_id,
            SECTION_UNAVAILABLE,
            unavailable_idx,
            uid,
            uname,
            "Unavailable",
            "",
            None
        )
        await db.commit()

    await refresh_poll_message(poll_id, interaction.client)
    await interaction.response.send_message("Marked As Unavailable.", ephemeral=True)
    await promote_from_reserve(poll_id, interaction.client)


async def handle_close_poll(interaction: discord.Interaction, poll_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT creator_id, channel_id, message_id FROM polls WHERE id = ?", (poll_id,))
        row = await cur.fetchone()

        if not row:
            await interaction.response.send_message("Poll Does Not Exist.", ephemeral=True)
            return

        creator_id, channel_id, message_id = row

    member = await interaction.guild.fetch_member(interaction.user.id)
    allowed = (interaction.user.id == creator_id) or await member_is_allowed(member)

    if not allowed:
        await interaction.response.send_message(
            "Only The Creator, An Admin, Or Configured Roles Can End This Poll.",
            ephemeral=True
        )
        return

    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM polls WHERE id = ?", (poll_id,))
        await db.execute("DELETE FROM slots WHERE poll_id = ?", (poll_id,))
        await db.commit()

    msg_id = poll_message_map.pop(poll_id, None) or message_id

    if msg_id and channel_id:
        try:
            channel = bot.get_channel(channel_id) or await bot.fetch_channel(channel_id)
            message = await channel.fetch_message(msg_id)
            await message.delete()
        except Exception:
            pass

    await interaction.response.send_message("Poll Ended And Removed.", ephemeral=True)


# ---------------- Interaction Routing ----------------
@bot.event
async def on_interaction(interaction: discord.Interaction):
    if interaction.type != discord.InteractionType.component:
        return

    custom_id = interaction.data.get("custom_id", "") if interaction.data else ""
    if not custom_id:
        return

    if custom_id.startswith("join:"):
        poll_id = int(custom_id.split(":", 1)[1])
        await handle_join(interaction, poll_id)
        return

    if custom_id.startswith("undecided:"):
        poll_id = int(custom_id.split(":", 1)[1])
        await handle_mark_undecided(interaction, poll_id)
        return

    if custom_id.startswith("unavailable:"):
        poll_id = int(custom_id.split(":", 1)[1])
        await handle_mark_unavailable(interaction, poll_id)
        return

    if custom_id.startswith("close:"):
        poll_id = int(custom_id.split(":", 1)[1])
        await handle_close_poll(interaction, poll_id)
        return


# ---------------- Slash Commands ----------------
@tree.command(
    name="sqbpoll",
    description="Create A New SQB Signup Poll",
    guild=discord.Object(id=GUILD_ID)
)
@app_commands.describe(
    day="Day Of The Event",
    time="Event Time In HH:MM",
    ping_role="Role To Ping",
    max_air=f"Air Slot Cap (1 To {MAX_AIR})",
    max_ground=f"Ground Slot Cap (1 To {MAX_GROUND})",
    header_text="Custom Text Shown At The Top",
    br="Optional BR Shown Next To The Ping"
)
async def cmd_sqbpoll(
    interaction: discord.Interaction,
    day: str,
    time: str,
    ping_role: discord.Role = None,
    max_air: int = None,
    max_ground: int = None,
    header_text: str = None,
    br: str = None
):
    member = await interaction.guild.fetch_member(interaction.user.id)

    if not await member_is_allowed(member):
        await interaction.response.send_message(
            "You Are Not Allowed To Create Polls. Contact An Officer.",
            ephemeral=True
        )
        return

    max_air = MAX_AIR if max_air is None else max_air
    max_ground = MAX_GROUND if max_ground is None else max_ground
    br = normalize_short_text(br, BR_CHAR_LIMIT)

    if not (1 <= max_air <= MAX_AIR):
        await interaction.response.send_message(f"Max_Air Must Be Between 1 And {MAX_AIR}.", ephemeral=True)
        return

    if not (1 <= max_ground <= MAX_GROUND):
        await interaction.response.send_message(f"Max_Ground Must Be Between 1 And {MAX_GROUND}.", ephemeral=True)
        return

    if parse_weekday(day) is None:
        await interaction.response.send_message("Invalid Weekday.", ephemeral=True)
        return

    try:
        hh, mm = map(int, time.split(":"))
        if not (0 <= hh <= 23 and 0 <= mm <= 59):
            raise ValueError
    except Exception:
        await interaction.response.send_message("Time Must Be HH:MM In 24h Format.", ephemeral=True)
        return

    poll_id = await create_poll_record(
        title=interaction.guild.name or "Squad",
        day=day,
        time_str=time,
        channel_id=interaction.channel_id,
        creator_id=interaction.user.id,
        ping_role_id=ping_role.id if ping_role else None,
        max_air=max_air,
        max_ground=max_ground,
        header_text=header_text,
        br_text=br
    )

    await refresh_poll_message(poll_id, bot)
    await interaction.response.send_message("Poll Created.", ephemeral=True)


@tree.command(
    name="sqbpoll_list",
    description="List Recent SQB Polls",
    guild=discord.Object(id=GUILD_ID)
)
async def cmd_sqbpoll_list(interaction: discord.Interaction):
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("""
            SELECT id, day, time, channel_id, creator_id
            FROM polls
            ORDER BY created_at DESC
            LIMIT 20
        """)
        rows = await cur.fetchall()

    if not rows:
        await interaction.response.send_message("No Open Polls Found.", ephemeral=True)
        return

    lines = [
        f"#{poll_id} — {day} {time_str} In <#{channel_id}> (Creator <@{creator_id}>)"
        for poll_id, day, time_str, channel_id, creator_id in rows
    ]

    await interaction.response.send_message("\n".join(lines), ephemeral=True)


@tree.command(
    name="sqbpoll_remove",
    description="Remove An Autopoll Schedule From This Channel",
    guild=discord.Object(id=GUILD_ID)
)
@app_commands.describe(day="Weekday", time="HH:MM")
async def cmd_sqbpoll_remove(interaction: discord.Interaction, day: str, time: str):
    member = await interaction.guild.fetch_member(interaction.user.id)

    if not await member_is_allowed(member):
        await interaction.response.send_message(
            "You Are Not Allowed To Remove Autopolls. Contact An Officer.",
            ephemeral=True
        )
        return

    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "DELETE FROM autos WHERE day = ? AND time = ? AND channel_id = ?",
            (day, time, interaction.channel_id)
        )
        await db.commit()

    key = (interaction.channel_id, day, time)
    job = scheduled_jobs.get(key)
    if job:
        job.remove()
        scheduled_jobs.pop(key, None)

    await interaction.response.send_message("Autopoll Removed.", ephemeral=True)


@tree.command(
    name="autosqb",
    description="Manage Scheduled Weekly Autoposts",
    guild=discord.Object(id=GUILD_ID)
)
@app_commands.describe(
    action="Create, Remove, Or List Autoposts",
    day="Weekday",
    time="HH:MM",
    ping_role="Role To Ping",
    max_air="Air Slot Cap For Autopost",
    max_ground="Ground Slot Cap For Autopost",
    header_text="Custom Text Shown At The Top",
    br="Optional BR Shown Next To The Ping"
)
async def cmd_autosqb(
    interaction: discord.Interaction,
    action: str,
    day: str = None,
    time: str = None,
    ping_role: discord.Role = None,
    max_air: int = None,
    max_ground: int = None,
    header_text: str = None,
    br: str = None
):
    action = (action or "").lower()
    member = await interaction.guild.fetch_member(interaction.user.id)

    if action not in ("create", "remove", "list"):
        await interaction.response.send_message("Action Must Be Create, Remove, Or List.", ephemeral=True)
        return

    if not await member_is_allowed(member):
        await interaction.response.send_message(
            "You Are Not Allowed To Manage Autos. Contact An Officer.",
            ephemeral=True
        )
        return

    if action == "list":
        async with aiosqlite.connect(DB_FILE) as db:
            cur = await db.execute("""
                SELECT id, day, time, channel_id, max_air_override, max_ground_override, header_text, br_text
                FROM autos
            """)
            rows = await cur.fetchall()

        if not rows:
            await interaction.response.send_message("No Autopolls Scheduled.", ephemeral=True)
            return

        lines = []
        for auto_id, auto_day, auto_time, channel_id, ma, mg, ht, br_text in rows:
            extra = []
            if ma is not None:
                extra.append(f"max_air={ma}")
            if mg is not None:
                extra.append(f"max_ground={mg}")
            if ht:
                extra.append("custom header")
            if br_text:
                extra.append(f"BR={br_text}")

            lines.append(f"{auto_id}) {auto_day} {auto_time} In <#{channel_id}> {' '.join(extra)}".strip())

        await interaction.response.send_message("\n".join(lines), ephemeral=True)
        return

    if not day or not time:
        await interaction.response.send_message("Provide Both Day And Time For Create/Remove.", ephemeral=True)
        return

    if action == "create":
        max_air = MAX_AIR if max_air is None else max_air
        max_ground = MAX_GROUND if max_ground is None else max_ground
        br = normalize_short_text(br, BR_CHAR_LIMIT)

        if not (1 <= max_air <= MAX_AIR):
            await interaction.response.send_message(f"Max_Air Must Be Between 1 And {MAX_AIR}.", ephemeral=True)
            return

        if not (1 <= max_ground <= MAX_GROUND):
            await interaction.response.send_message(f"Max_Ground Must Be Between 1 And {MAX_GROUND}.", ephemeral=True)
            return

        if parse_weekday(day) is None:
            await interaction.response.send_message("Invalid Weekday.", ephemeral=True)
            return

        try:
            hh, mm = map(int, time.split(":"))
            if not (0 <= hh <= 23 and 0 <= mm <= 59):
                raise ValueError
        except Exception:
            await interaction.response.send_message("Time Must Be HH:MM In 24h Format.", ephemeral=True)
            return

        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute("""
                INSERT INTO autos(
                    day, time, channel_id, creator_id, ping_role_id,
                    max_air_override, max_ground_override, header_text, br_text
                )
                VALUES(?,?,?,?,?,?,?,?,?)
            """, (
                day,
                time,
                interaction.channel_id,
                interaction.user.id,
                ping_role.id if ping_role else None,
                max_air,
                max_ground,
                header_text,
                br
            ))
            await db.commit()

        schedule_auto_job(
            interaction.channel_id,
            day,
            time,
            interaction.user.id,
            ping_role.id if ping_role else None,
            max_air,
            max_ground,
            header_text,
            br
        )

        await interaction.response.send_message("Weekly Autopoll Scheduled.", ephemeral=True)
        return

    if action == "remove":
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute(
                "DELETE FROM autos WHERE day = ? AND time = ? AND channel_id = ?",
                (day, time, interaction.channel_id)
            )
            await db.commit()

        key = (interaction.channel_id, day, time)
        job = scheduled_jobs.get(key)
        if job:
            job.remove()
            scheduled_jobs.pop(key, None)

        await interaction.response.send_message("Autopoll Removed.", ephemeral=True)
        return


@tree.command(
    name="setofficerrole",
    description="Manage Allowed Creator Roles",
    guild=discord.Object(id=GUILD_ID)
)
@app_commands.describe(
    action="Add, Remove, Or List Roles",
    role="Server Role To Add Or Remove"
)
async def cmd_config(interaction: discord.Interaction, action: str, role: discord.Role = None):
    action = (action or "").lower()

    if action not in ("add", "remove", "list"):
        await interaction.response.send_message("Use Add, Remove, Or List.", ephemeral=True)
        return

    member = await interaction.guild.fetch_member(interaction.user.id)

    if action in ("add", "remove") and not await member_is_allowed(member):
        await interaction.response.send_message(
            "Only Admins Or Configured Roles Can Change Allowed Creator Roles.",
            ephemeral=True
        )
        return

    async with aiosqlite.connect(DB_FILE) as db:
        if action == "add":
            if not role:
                await interaction.response.send_message("Pick A Server Role To Add.", ephemeral=True)
                return

            await db.execute("INSERT OR IGNORE INTO roles(name) VALUES(?)", (role.name,))
            await db.commit()
            await interaction.response.send_message(f"Allowed Role Added: {role.mention}", ephemeral=True)
            return

        if action == "remove":
            if not role:
                await interaction.response.send_message("Pick A Server Role To Remove.", ephemeral=True)
                return

            await db.execute("DELETE FROM roles WHERE name = ?", (role.name,))
            await db.commit()
            await interaction.response.send_message(f"Allowed Role Removed: {role.mention}", ephemeral=True)
            return

        cur = await db.execute("SELECT name FROM roles")
        rows = await cur.fetchall()
        configured = {row[0] for row in rows}

    lines = []
    for role_obj in interaction.guild.roles:
        if role_obj.is_default():
            continue
        mark = " (Allowed)" if role_obj.name in configured else ""
        lines.append(f"{role_obj.mention}{mark}")

    if not lines:
        await interaction.response.send_message("No Non-Default Roles Found On This Server.", ephemeral=True)
    else:
        await interaction.response.send_message("Server Roles:\n" + "\n".join(lines), ephemeral=True)


# ---------------- Startup ----------------
@bot.event
async def on_ready():
    print("Bot Ready:", bot.user)

    await init_db()

    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("""
            SELECT id, day, time, channel_id, creator_id, ping_role_id,
                   max_air_override, max_ground_override, header_text, br_text
            FROM autos
        """)
        autos = await cur.fetchall()

    for auto_id, day, time_str, channel_id, creator_id, ping_role_id, max_air, max_ground, header_text, br_text in autos:
        schedule_auto_job(
            channel_id,
            day,
            time_str,
            creator_id,
            ping_role_id,
            max_air,
            max_ground,
            header_text,
            br_text
        )

    if not scheduler.running:
        scheduler.start()

    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT id, channel_id, message_id FROM polls WHERE is_open = 1")
        polls = await cur.fetchall()

    for row in polls:
        poll_id = row["id"]
        channel_id = row["channel_id"]
        message_id = row["message_id"]

        if message_id:
            try:
                channel = bot.get_channel(channel_id) or await bot.fetch_channel(channel_id)
                await channel.fetch_message(message_id)
                await refresh_poll_message(poll_id, bot)
                continue
            except Exception:
                pass

        await refresh_poll_message(poll_id, bot)

    try:
        await tree.sync(guild=discord.Object(id=GUILD_ID))
    except Exception as e:
        print("Sync Warning:", e)


# ---------------- Run ----------------
if __name__ == "__main__":
    bot.run(TOKEN)