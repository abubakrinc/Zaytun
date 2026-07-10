import os
import re
import sys
import time
import json
import shutil
import asyncio
import uvloop
import httpx

# 1. Loop siyosatini o'rnatamiz
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

try:
    asyncio.get_event_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

from pyrogram import Client, filters
from pyrogram import idle

from error_analyzer import (
    parse_log,
    build_log_report,
    build_duplicates_report,
    find_duplicate_lessons,
)

# ================== SOZLAMALAR ==================
api_id = int(os.environ.get("API_ID", "30778935"))
api_hash = os.environ.get("API_HASH", "2114df663586afba07d8fffd3b9d5d70")

ARCHIVE_ACCESS_KEY = os.environ.get("ARCHIVE_ACCESS_KEY", "FsNFt4iInrLoTpGI")
ARCHIVE_SECRET_KEY = os.environ.get("ARCHIVE_SECRET_KEY", "vp37uhuXZJXTE68k")

ARCHIVE_IDENTIFIER = "savol-javob-abu-yahyo"
JSON_FILE_PATH = "savol-javob.json"

WATCHDOG_TIMEOUT = 60 * 30
MAX_CONSECUTIVE_FAILURES = 3
PAUSE_DURATION = 180
MAX_PARALLEL_UPLOADS = 1

DISK_SPACE_WARNING_MB = 500
BACKUP_INTERVAL_HOURS = 6
DAILY_REPORT_HOUR = 22
BACKUP_CHAT = "me"

json_lock = asyncio.Lock()
upload_queue = asyncio.Queue()

worker_state = {"consecutive_failures": 0}
worker_state_lock = asyncio.Lock()
pause_lock = asyncio.Lock()

network_ok_event = asyncio.Event()
network_ok_event.set()

manual_ok_event = asyncio.Event()
manual_ok_event.set()

active_uploads = {}
active_uploads_lock = asyncio.Lock()

app = None

# ================== JSON BAZA ==================
def load_json_data():
    if not os.path.exists(JSON_FILE_PATH):
        default_data = {"lessons": [], "queue": [], "next_id": 1}
        with open(JSON_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(default_data, f, indent=4, ensure_ascii=False)
        return default_data
    try:
        with open(JSON_FILE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            data.setdefault("lessons", [])
            data.setdefault("queue", [])
            data.setdefault("next_id", 1)
            return data
    except Exception:
        return {"lessons": [], "queue": [], "next_id": 1}

def save_json_data(data):
    tmp_path = JSON_FILE_PATH + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    os.replace(tmp_path, JSON_FILE_PATH)

async def get_next_lesson_id():
    async with json_lock:
        data = load_json_data()
        next_id = data.get("next_id", 1)
        data["next_id"] = next_id + 1
        save_json_data(data)
    return next_id

# ================== YORDAMCHI FUNKSIYALAR ==================
def cleanup_old_processes():
    current_pid = os.getpid()
    try:
        script_name = os.path.basename(__file__)
        output = os.popen(f"pgrep -f {script_name}").read().strip().split()
        for pid in output:
            if int(pid) != current_pid:
                os.kill(int(pid), 9)
                time.sleep(0.5)
    except Exception:
        pass

cleanup_old_processes()

session_path = "archive_stream_uploader"
ERROR_LOG_PATH = "upload_errors.log"

if os.path.exists(f"{session_path}.session-journal"):
    try:
        os.remove(f"{session_path}.session-journal")
    except Exception:
        pass

def format_mb(bytes_size):
    return bytes_size / (1024 * 1024)

def format_speed(kb_per_sec):
    if kb_per_sec >= 1024:
        return f"{kb_per_sec / 1024:.2f} MB/s"
    return f"{kb_per_sec:.1f} KB/s"

def get_content_type(filename: str) -> str:
    ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ""
    mapping = {
        "mp3": "audio/mpeg",
        "ogg": "audio/ogg",
        "oga": "audio/ogg",
        "opus": "audio/opus",
        "m4a": "audio/mp4",
        "wav": "audio/wav",
        "flac": "audio/flac",
    }
    return mapping.get(ext, "application/octet-stream")

def log_error(context: str, error) -> None:
    try:
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        with open(ERROR_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] {context} | {error}\n")
    except Exception:
        pass

def wake_lock_acquire():
    try:
        os.system("termux-wake-lock")
    except Exception:
        pass

def wake_lock_release():
    try:
        os.system("termux-wake-unlock")
    except Exception:
        pass

async def safe_edit(msg, text):
    try:
        await msg.edit_text(text)
    except Exception:
        pass

def find_lesson(data, query):
    lessons = data.get("lessons", [])
    query = query.strip()
    if query.isdigit():
        idx = int(query) - 1
        if 0 <= idx < len(lessons):
            return idx, lessons[idx]
        return None, None
    for i, l in enumerate(lessons):
        if l.get("unique_id") == query:
            return i, l
    return None, None

def check_disk_space() -> str:
    try:
        free_bytes = shutil.disk_usage(".").free
        free_mb = free_bytes / (1024 * 1024)
        if free_mb < DISK_SPACE_WARNING_MB:
            return f"\n\n⚠️ **Diqqat:** diskda faqat {free_mb:.0f} MB joy qoldi!"
    except Exception:
        pass
    return ""

async def delete_from_archive(clean_name: str) -> bool:
    url = f"https://s3.us.archive.org/{ARCHIVE_IDENTIFIER}/{clean_name}"
    headers = {"Authorization": f"LOW {ARCHIVE_ACCESS_KEY}:{ARCHIVE_SECRET_KEY}"}
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            resp = await client.delete(url, headers=headers)
            return resp.status_code in (200, 204, 404)
    except Exception as e:
        log_error("Archive'dan o'chirishda xato", e)
        return False

async def check_archive_limit() -> bool:
    url = (
        f"https://s3.us.archive.org/?check_limit=1"
        f"&accesskey={ARCHIVE_ACCESS_KEY}&bucket={ARCHIVE_IDENTIFIER}"
    )
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as client:
            resp = await client.get(url)
            data = resp.json()
            return int(data.get("over_limit", 0)) == 1
    except Exception as e:
        log_error("check_limit so'rovida xato", e)
        return False

def build_stats_text(data: dict) -> str:
    lessons = data.get("lessons", [])
    queue_count = len(data.get("queue", []))

    if not lessons:
        return f"📭 Hali hech qanday dars yuklanmagan.\n⏳ Navbatda: {queue_count} ta"

    total_count = len(lessons)

    total_mb = 0.0
    for l in lessons:
        cap_str = l.get("capacity", "0 MB")
        match = re.search(r"([\d.]+)", cap_str)
        if match:
            total_mb += float(match.group(1))
    total_gb = total_mb / 1024

    total_minutes = 0.0
    for l in lessons:
        dur_str = l.get("duration", "")
        m = re.search(r"(\d+)\s*min\s*(\d+)?\s*sek?", dur_str)
        if m:
            mins = int(m.group(1))
            secs = int(m.group(2)) if m.group(2) else 0
            total_minutes += mins + (secs / 60)

    total_hours = int(total_minutes // 60)
    remaining_minutes = int(total_minutes % 60)

    dates = [l.get("date", "") for l in lessons if l.get("date")]
    first_date = dates[0] if dates else "—"
    last_date = dates[-1] if dates else "—"

    duplicates = find_duplicate_lessons(lessons)
    duplicate_groups = len(duplicates)
    duplicate_extra = sum(len(items) - 1 for items in duplicates.values())

    error_entries = parse_log(ERROR_LOG_PATH)
    total_errors = len(error_entries)

    return (
        f"📊 **UMUMIY STATISTIKA**\n\n"
        f"📚 Jami darslar: **{total_count}** ta\n"
        f"⏳ Navbatda: {queue_count} ta\n\n"
        f"⚖️ Umumiy hajm: {total_mb:.2f} MB ({total_gb:.2f} GB)\n"
        f"⏱ Umumiy davomiylik: {total_hours} soat {remaining_minutes} min\n\n"
        f"🕐 Birinchi dars: {first_date}\n"
        f"🕐 Oxirgi dars: {last_date}\n\n"
        f"🔁 Takroriy guruhlar: {duplicate_groups} ta ({duplicate_extra} ta ortiqcha nusxa)\n"
        f"❌ Jami xatolar (log): {total_errors} ta\n\n"
        f"ℹ️ Batafsil ro'yxat: /list\n"
        f"ℹ️ Takroriylar: /duplicates\n"
        f"ℹ️ Xatolar: /errors"
    )

# ================== TELEGRAMDAN TO'G'RIDAN-TO'G'RI STREAM (DISKKA SAQLAMAY) ==================
async def stream_from_telegram(client, message, file_size, status_msg, worker_id, clean_name):
    uploaded = 0
    last_update = time.time()
    start_time = time.time()

    async with active_uploads_lock:
        active_uploads[worker_id] = {
            "name": clean_name, "uploaded": 0, "total": file_size,
            "percent": 0, "stage": "uzatilmoqda"
        }

    async for chunk in client.stream_media(message):
        uploaded += len(chunk)
        now = time.time()

        if now - last_update > 7.0 or uploaded >= file_size:
            last_update = now
            percent = int((uploaded / file_size) * 100) if file_size else 0
            elapsed = max(now - start_time, 0.001)
            speed_kb = (uploaded / 1024) / elapsed

            async with active_uploads_lock:
                if worker_id in active_uploads:
                    active_uploads[worker_id].update({
                        "uploaded": uploaded, "percent": percent
                    })

            asyncio.create_task(safe_edit(
                status_msg,
                f"🚀 **Stream orqali Internet Archive'ga uzatilmoqda...**\n"
                f"📦 {format_mb(uploaded):.2f}/{format_mb(file_size):.2f} MB ({percent}%)\n"
                f"⚡ Tezlik: {format_speed(speed_kb)}\n"
                f"💾 Diskka saqlanmayapti — to'g'ridan-to'g'ri uzatish"
            ))

        yield chunk
        await asyncio.sleep(0)

# ================== INTERNET ARCHIVE YUKLASH FUNKSIYASI (STREAMING) ==================
async def upload_stream_direct(client, message, clean_name, file_size, status_msg, worker_id):
    archive_put_url = f"https://s3.us.archive.org/{ARCHIVE_IDENTIFIER}/{clean_name}"

    headers = {
        "Authorization": f"LOW {ARCHIVE_ACCESS_KEY}:{ARCHIVE_SECRET_KEY}",
        "Content-Type": get_content_type(clean_name),
        "Content-Length": str(file_size),
        "x-archive-meta-mediatype": "audio",
        "x-archive-meta-title": clean_name.rsplit('.', 1)[0],
        "x-amz-auto-make-bucket": "1"
    }

    max_retries = 5

    await status_msg.edit_text(
        f"🚀 **Stream orqali yuklash boshlanmoqda...**\n"
        f"📦 Hajm: {format_mb(file_size):.2f} MB\n"
        f"💾 Diskka saqlanmaydi"
        f"{check_disk_space()}"
    )

    for attempt in range(1, max_retries + 1):
        try:
            wait_check = 0
            while await check_archive_limit():
                wait_check += 1
                wait_secs = min(15 * wait_check, 90)
                async with active_uploads_lock:
                    if worker_id in active_uploads:
                        active_uploads[worker_id]["stage"] = "rate-limit kutilmoqda"
                await status_msg.edit_text(
                    f"⏳ **Archive.org hozircha band (rate limit).**\n"
                    f"🔁 {wait_secs} soniyadan keyin qayta tekshiriladi... (urinish {attempt}/{max_retries})"
                )
                await asyncio.sleep(wait_secs)
                if wait_check >= 20:
                    break

            await status_msg.edit_text(
                f"🚀 **Internet Archive'ga uzatilmoqda...**\n"
                f"⚙️ Urinish: {attempt}/{max_retries}\n"
                f"📦 0.00/{format_mb(file_size):.2f} MB (0%)"
            )

            timeout = httpx.Timeout(connect=30.0, read=300.0, write=300.0, pool=30.0)

            async with httpx.AsyncClient(timeout=timeout) as async_client:
                response = await async_client.put(
                    archive_put_url,
                    headers=headers,
                    content=stream_from_telegram(client, message, file_size, status_msg, worker_id, clean_name)
                )
                response.raise_for_status()

            integrity_ok = False
            last_check_err = None
            check_attempts = 4

            for check_attempt in range(1, check_attempts + 1):
                await asyncio.sleep(3 if check_attempt == 1 else 8)
                try:
                    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0), follow_redirects=True) as check_client:
                        head_resp = await check_client.head(archive_put_url)
                        if head_resp.status_code == 200:
                            remote_size = int(head_resp.headers.get("content-length", 0))
                            if remote_size == file_size:
                                integrity_ok = True
                                break
                            else:
                                last_check_err = ValueError(f"Hajm mos kelmadi: kutilgan {file_size}, Archive'da {remote_size}")
                        else:
                            last_check_err = ValueError(f"Tekshiruv kodi: {head_resp.status_code}")
                except Exception as e:
                    last_check_err = e

            if not integrity_ok:
                raise last_check_err or Exception("Fayl butunligini tasdiqlab bo'lmadi")

            return response.status_code

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 503:
                log_error(f"503 SlowDown ({clean_name}), urinish {attempt}", e)
                if attempt == max_retries:
                    raise e
                wait_time = min(30 * (2 ** (attempt - 1)), 300)
                await status_msg.edit_text(
                    f"⏸ **503 SlowDown — server yuklamani rad etdi.**\n"
                    f"🔁 {wait_time} soniyadan keyin qayta urinamiz... (urinish {attempt}/{max_retries})"
                )
                await asyncio.sleep(wait_time)
                continue
            else:
                log_error(f"Archive yuklash xatosi ({clean_name}), urinish {attempt}", e)
                if attempt == max_retries:
                    raise e
                wait_time = min(10 * (2 ** (attempt - 1)), 120)
                await asyncio.sleep(wait_time)

        except Exception as e:
            log_error(f"Archive yuklash xatosi ({clean_name}), urinish {attempt}", e)
            if attempt == max_retries:
                raise e
            wait_time = min(10 * (2 ** (attempt - 1)), 120)
            await asyncio.sleep(wait_time)

# ================== PAUZA VA XATOLIKLAR ==================
async def register_failure_and_maybe_pause():
    async with worker_state_lock:
        worker_state["consecutive_failures"] += 1
        should_pause = worker_state["consecutive_failures"] >= MAX_CONSECUTIVE_FAILURES

    if should_pause:
        async with pause_lock:
            async with worker_state_lock:
                still_should_pause = worker_state["consecutive_failures"] >= MAX_CONSECUTIVE_FAILURES
            if still_should_pause:
                network_ok_event.clear()
                print(f"\n⏸ Ketma-ket {MAX_CONSECUTIVE_FAILURES}+ xatolik. {PAUSE_DURATION} soniya kutiladi...")
                await asyncio.sleep(PAUSE_DURATION)
                async with worker_state_lock:
                    worker_state["consecutive_failures"] = 0
                network_ok_event.set()

    await network_ok_event.wait()

async def register_success():
    async with worker_state_lock:
        worker_state["consecutive_failures"] = 0

# ================== WORKER ==================
async def queue_worker(worker_id: int = 0):
    while True:
        await network_ok_event.wait()
        await manual_ok_event.wait()
        client, message_id, status_msg = await upload_queue.get()
        success = False

        try:
            try:
                message = await client.get_messages(chat_id="me", message_ids=message_id)
            except Exception:
                async with json_lock:
                    data = load_json_data()
                    data["queue"] = [q for q in data["queue"] if q != message_id]
                    save_json_data(data)
                continue

            media = message.audio or message.voice if message else None
            if not media:
                async with json_lock:
                    data = load_json_data()
                    data["queue"] = [q for q in data["queue"] if q != message_id]
                    save_json_data(data)
                continue

            msg_id = message.id
            try:
                await status_msg.edit_text(f"⚡ Tayyorlanmoqda... (worker {worker_id})")
            except Exception:
                status_msg = await message.reply_text(f"⚡ Tayyorlanmoqda... (worker {worker_id})")

            raw_name = getattr(media, 'file_name', None) or f"audio_{msg_id}.mp3"
            name_part, ext_part = os.path.splitext(raw_name)
            ext_part = ext_part.lower() or ".mp3"
            name_part = re.sub(r'[^a-zA-Z0-9]+', '_', name_part)
            name_part = re.sub(r'_+', '_', name_part).strip('_')

            if not name_part:
                name_part = "audio"

            lesson_id = await get_next_lesson_id()
            clean_name = f"{lesson_id:04d}_{name_part}_{msg_id}{ext_part}"

            file_size = media.file_size
            capacity_mb = format_mb(file_size)

            print(f"\n[{worker_id}] Vazifa: {clean_name} ({capacity_mb:.2f} MB) — stream rejimida")

            async def process_stream():
                await upload_stream_direct(client, message, clean_name, file_size, status_msg, worker_id)

            await asyncio.wait_for(process_stream(), timeout=WATCHDOG_TIMEOUT)

            archive_url = f"https://archive.org/download/{ARCHIVE_IDENTIFIER}/{clean_name}"
            media_title = getattr(media, 'title', None)
            title = media_title if media_title else name_part.replace('_', ' ')
            caption_text = message.caption or ""
            description = caption_text if caption_text else "Audio dars"
            duration_sec = getattr(media, 'duration', 0) or 0
            duration_str = f"{duration_sec // 60} min {duration_sec % 60} sek" if duration_sec else "—"
            current_time = time.strftime("%d.%m.%Y %H:%M")

            lesson_entry = {
                "lesson_id": lesson_id,
                "unique_id": f"lesson_{int(time.time())}_{msg_id}",
                "title": title,
                "description": description,
                "caption": caption_text,
                "capacity": f"{capacity_mb:.2f} MB",
                "duration": duration_str,
                "date": current_time,
                "download_url": archive_url,
            }

            async with json_lock:
                data = load_json_data()
                data["lessons"].append(lesson_entry)
                data["queue"] = [q for q in data["queue"] if q != message_id]
                save_json_data(data)

            anomaly_warning = ""
            if duration_sec == 0:
                anomaly_warning = "\n\n⚠️ **Diqqat:** audio davomiyligi 0 sekund — fayl buzilgan bo'lishi mumkin!"

            await status_msg.edit_text(
                f"✅ **Internet Archive'ga yuklandi!**\n\n"
                f"🆔 **ID:** {lesson_id:04d}\n"
                f"📝 **Nomi:** {title}\n"
                f"📄 **Tavsif:** {description}\n"
                f"⚖️ **Hajmi:** {capacity_mb:.2f} MB\n"
                f"⏱ **Davomiyligi:** {duration_str}\n"
                f"🕐 **Vaqt:** {current_time}\n"
                f"🔗 {archive_url}"
                f"{anomaly_warning}",
                disable_web_page_preview=True
            )

            print(f"[{worker_id}] ✅ Muaffaqiyatli: {clean_name}")
            success = True

        except asyncio.TimeoutError:
            log_error(f"Watchdog timeout (message_id={message_id})", "Tugatishga ulgurmadi")
            print(f"\n[{worker_id}] ❌ Xatolik: Belgilangan vaqt ichida yakunlanmadi.")
            try:
                await status_msg.edit_text("❌ Xatolik: Timeout (internet aloqasi juda sekin bo'lishi mumkin).\nQayta urinish uchun: /pending")
            except Exception:
                pass
        except Exception as e:
            log_error(f"Umumiy xatolik (message_id={message_id})", e)
            print(f"\n[{worker_id}] ❌ Xatolik: {str(e)}")
            try:
                await status_msg.edit_text(f"❌ Xatolik: {str(e)}\nQayta urinish uchun: /pending")
            except Exception:
                pass
        finally:
            async with active_uploads_lock:
                active_uploads.pop(worker_id, None)
            upload_queue.task_done()
            if success:
                await register_success()
            else:
                await register_failure_and_maybe_pause()

# ================== SAVED MESSAGES ==================
async def restore_queue_from_json(client):
    async with json_lock:
        data = load_json_data()
        saved_queue = data.get("queue", [])

    if saved_queue:
        print(f"\n🔄 JSON bazadan {len(saved_queue)} ta chala qolgan vazifa topildi va tiklanmoqda...")
        for msg_id in saved_queue:
            status_msg = await client.send_message(chat_id="me", text=f"⏳ Navbat tiklandi (ID: {msg_id})...")
            await upload_queue.put((client, msg_id, status_msg))

# ================== FON VAZIFALARI ==================
async def periodic_backup_task(client):
    while True:
        await asyncio.sleep(BACKUP_INTERVAL_HOURS * 3600)
        try:
            if os.path.exists(JSON_FILE_PATH):
                await client.send_document(
                    chat_id=BACKUP_CHAT,
                    document=JSON_FILE_PATH,
                    caption=f"🔒 Avtomatik backup ({time.strftime('%d.%m.%Y %H:%M')})"
                )
        except Exception as e:
            log_error("Avtomatik backup xatosi", e)

async def daily_report_task(client):
    while True:
        now = time.localtime()
        target = time.struct_time((
            now.tm_year, now.tm_mon, now.tm_mday,
            DAILY_REPORT_HOUR, 0, 0, now.tm_wday, now.tm_yday, now.tm_isdst
        ))
        target_ts = time.mktime(target)
        now_ts = time.mktime(now)

        if target_ts <= now_ts:
            target_ts += 86400

        wait_seconds = target_ts - now_ts
        await asyncio.sleep(wait_seconds)

        try:
            async with json_lock:
                data = load_json_data()
            report = build_stats_text(data)
            await client.send_message(
                chat_id=BACKUP_CHAT,
                text=f"📅 **Kunlik hisobot** ({time.strftime('%d.%m.%Y')})\n\n{report}"
            )
        except Exception as e:
            log_error("Kunlik hisobot xatosi", e)

# ================== DEKORATORLAR ==================
async def setup_handlers(client):
    @client.on_message(filters.me & filters.chat("me") & filters.command("res", prefixes="/"))
    async def restart_bot(c, message):
        await message.reply_text("🔄 Skript qayta ishga tushirilmoqda...")
        await asyncio.sleep(1)
        try:
            await c.stop()
        except Exception:
            pass
        os.execv(sys.executable, [sys.executable] + sys.argv)

    @client.on_message(filters.me & filters.chat("me") & (filters.audio | filters.voice))
    async def process_saved_audio(c, message):
        msg_id = message.id
        async with json_lock:
            data = load_json_data()
            if msg_id not in data["queue"]:
                data["queue"].append(msg_id)
                save_json_data(data)

        status_msg = await message.reply_text("⏳ Stream navbatiga qo'shildi...")
        await upload_queue.put((c, msg_id, status_msg))

        current_queue_size = upload_queue.qsize()
        if current_queue_size > 1:
            await status_msg.edit_text(f"⏳ Navbatga qo'shildi. Oldinda {current_queue_size - 1} ta fayl bor...")

    # ---------- /list ----------
    @client.on_message(filters.me & filters.chat("me") & filters.command("list", prefixes="/"))
    async def list_lessons(c, message):
        async with json_lock:
            data = load_json_data()
        lessons = data.get("lessons", [])
        if not lessons:
            await message.reply_text("📭 Hali hech qanday dars yuklanmagan.")
            return

        args = message.text.split()
        page = int(args[1]) if len(args) > 1 and args[1].isdigit() else 1
        per_page = 10
        start = (page - 1) * per_page
        end = start + per_page
        chunk = lessons[start:end]
        total_pages = (len(lessons) + per_page - 1) // per_page

        text = f"📚 **Darslar ro'yxati** (sahifa {page}/{total_pages})\n\n"
        for i, l in enumerate(chunk, start=start + 1):
            lid = l.get("lesson_id")
            id_tag = f"[{lid:04d}] " if lid else ""
            text += f"**{i}.** {id_tag}{l['title']} — {l['capacity']} ({l['date']})\n"
        text += f"\nℹ️ Batafsil: /view <raqam>\n➡️ Keyingi sahifa: /list {page + 1}"
        await message.reply_text(text)

    # ---------- /view ----------
    @client.on_message(filters.me & filters.chat("me") & filters.command("view", prefixes="/"))
    async def view_lesson(c, message):
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await message.reply_text("❗️ Foydalanish: /view <raqam yoki id>")
            return
        async with json_lock:
            data = load_json_data()
        idx, lesson = find_lesson(data, args[1])
        if lesson is None:
            await message.reply_text("❌ Topilmadi.")
            return
        lid = lesson.get("lesson_id")
        await message.reply_text(
            f"🆔 **ID:** {f'{lid:04d}' if lid else '—'}\n"
            f"📝 **Nomi:** {lesson['title']}\n"
            f"📄 **Tavsif:** {lesson['description']}\n"
            f"⚖️ **Hajmi:** {lesson['capacity']}\n"
            f"⏱ **Davomiyligi:** {lesson['duration']}\n"
            f"🕐 **Vaqt:** {lesson['date']}\n"
            f"🔗 {lesson['download_url']}",
            disable_web_page_preview=True
        )

    # ---------- /search ----------
    @client.on_message(filters.me & filters.chat("me") & filters.command("search", prefixes="/"))
    async def search_lessons(c, message):
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await message.reply_text("❗️ Foydalanish: /search <matn>")
            return
        query = args[1].lower()
        async with json_lock:
            data = load_json_data()
        results = [
            (i + 1, l) for i, l in enumerate(data.get("lessons", []))
            if query in l["title"].lower() or query in l.get("description", "").lower()
        ]
        if not results:
            await message.reply_text("❌ Hech narsa topilmadi.")
            return
        text = f"🔍 **Natijalar** ({len(results)} ta):\n\n"
        for i, l in results[:20]:
            text += f"**{i}.** {l['title']} — {l['capacity']}\n"
        await message.reply_text(text)

    # ---------- /delete ----------
    @client.on_message(filters.me & filters.chat("me") & filters.command("delete", prefixes="/"))
    async def delete_lesson(c, message):
        args = message.text.split()
        if len(args) < 2:
            await message.reply_text("❗️ Foydalanish: /delete <raqam> [archive]")
            return

        also_archive = len(args) > 2 and args[2].lower() == "archive"

        async with json_lock:
            data = load_json_data()
            idx, lesson = find_lesson(data, args[1])
            if lesson is None:
                await message.reply_text("❌ Topilmadi.")
                return
            data["lessons"].pop(idx)
            save_json_data(data)

        reply = f"🗑 O'chirildi: {lesson['title']}"

        if also_archive:
            clean_name = lesson["download_url"].rsplit("/", 1)[-1]
            ok = await delete_from_archive(clean_name)
            reply += "\n✅ Archive.org'dan ham o'chirildi." if ok else "\n⚠️ Archive.org'dan o'chirishda xatolik."

        await message.reply_text(reply)

    # ---------- /clear ----------
    @client.on_message(filters.me & filters.chat("me") & filters.command("clear", prefixes="/"))
    async def clear_queue(c, message):
        drained = 0
        while not upload_queue.empty():
            try:
                upload_queue.get_nowait()
                upload_queue.task_done()
                drained += 1
            except asyncio.QueueEmpty:
                break

        async with json_lock:
            data = load_json_data()
            pending = len(data.get("queue", []))
            data["queue"] = []
            save_json_data(data)

        await message.reply_text(
            f"🧹 Navbat tozalandi.\n"
            f"📤 Xotiradagi navbatdan olib tashlandi: {drained} ta\n"
            f"💾 JSON navbatdan olib tashlandi: {pending} ta"
        )

    # ---------- /download ----------
    @client.on_message(filters.me & filters.chat("me") & filters.command("download", prefixes="/"))
    async def download_json(c, message):
        if not os.path.exists(JSON_FILE_PATH):
            await message.reply_text("❌ JSON fayl topilmadi.")
            return
        await message.reply_document(
            document=JSON_FILE_PATH,
            caption=f"📦 Baza fayli: {JSON_FILE_PATH} ({time.strftime('%d.%m.%Y %H:%M')})"
        )

    # ---------- /settings ----------
    @client.on_message(filters.me & filters.chat("me") & filters.command("settings", prefixes="/"))
    async def show_settings(c, message):
        await message.reply_text(
            "⚙️ **Joriy sozlamalar:**\n\n"
            f"🎯 identifier: `{ARCHIVE_IDENTIFIER}`\n"
            f"📄 json_file: `{JSON_FILE_PATH}`\n"
            f"👷 max_parallel: `{MAX_PARALLEL_UPLOADS}`\n"
            f"⏱ watchdog_timeout: `{WATCHDOG_TIMEOUT}` sek\n"
            f"❌ max_failures: `{MAX_CONSECUTIVE_FAILURES}`\n"
            f"⏸ pause_duration: `{PAUSE_DURATION}` sek\n"
            f"💾 disk_warning: `{DISK_SPACE_WARNING_MB}` MB\n"
            f"🔒 backup_interval: `{BACKUP_INTERVAL_HOURS}` soat\n"
            f"📅 daily_report_hour: `{DAILY_REPORT_HOUR}`:00\n\n"
            f"Sonli qiymat: /set <nom> <qiymat>\n"
            f"Archive identifier: /set identifier <yangi_nom>\n"
            f"JSON fayl nomi: /setjson <fayl_nomi.json>"
        )

    # ---------- /set (identifier + sonli sozlamalar) ----------
    @client.on_message(filters.me & filters.chat("me") & filters.command("set", prefixes="/"))
    async def set_setting(c, message):
        global WATCHDOG_TIMEOUT, MAX_CONSECUTIVE_FAILURES, PAUSE_DURATION
        global DISK_SPACE_WARNING_MB, BACKUP_INTERVAL_HOURS, DAILY_REPORT_HOUR
        global ARCHIVE_IDENTIFIER

        args = message.text.split(maxsplit=2)
        if len(args) < 3:
            await message.reply_text(
                "❗️ Foydalanish: /set <nom> <qiymat>\n\n"
                "Mavjud nomlar:\n"
                "identifier, watchdog_timeout, max_failures,\n"
                "pause_duration, disk_warning, backup_interval, daily_report_hour"
            )
            return

        key, value = args[1].lower(), args[2].strip()

        if key == "identifier":
            old_identifier = ARCHIVE_IDENTIFIER
            ARCHIVE_IDENTIFIER = value
            await message.reply_text(
                f"✅ **Archive identifier o'zgartirildi.**\n"
                f"Eski: `{old_identifier}`\n"
                f"Yangi: `{ARCHIVE_IDENTIFIER}`\n\n"
                f"⚠️ Keyingi barcha yuklashlar shu yangi sahifaga boradi.\n"
                f"ℹ️ Eski darslar JSON bazada shu holicha qoladi (ularning havolalari eski sahifaga ishora qiladi)."
            )
            return

        if not value.isdigit():
            await message.reply_text("❗️ Bu sozlama uchun qiymat butun son bo'lishi kerak.")
            return

        value_int = int(value)

        if key == "watchdog_timeout":
            WATCHDOG_TIMEOUT = value_int
        elif key == "max_failures":
            MAX_CONSECUTIVE_FAILURES = value_int
        elif key == "pause_duration":
            PAUSE_DURATION = value_int
        elif key == "disk_warning":
            DISK_SPACE_WARNING_MB = value_int
        elif key == "backup_interval":
            BACKUP_INTERVAL_HOURS = value_int
        elif key == "daily_report_hour":
            if 0 <= value_int <= 23:
                DAILY_REPORT_HOUR = value_int
            else:
                await message.reply_text("❌ Soat 0-23 oralig'ida bo'lishi kerak.")
                return
        else:
            await message.reply_text("❌ Noma'lum sozlama. /settings orqali ro'yxatni ko'ring.")
            return

        await message.reply_text(f"✅ `{key}` = `{value_int}` qilib o'zgartirildi.")

    # ---------- /setjson ----------
    @client.on_message(filters.me & filters.chat("me") & filters.command("setjson", prefixes="/"))
    async def set_json_file(c, message):
        global JSON_FILE_PATH

        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await message.reply_text(f"❗️ Foydalanish: /setjson <fayl_nomi.json>\nJoriy fayl: `{JSON_FILE_PATH}`")
            return

        new_path = args[1].strip()
        if not new_path.endswith(".json"):
            new_path += ".json"

        async with json_lock:
            old_path = JSON_FILE_PATH

            if os.path.exists(new_path):
                JSON_FILE_PATH = new_path
                await message.reply_text(f"✅ JSON fayl `{new_path}` ga o'zgartirildi (mavjud fayl ishlatiladi).")
            elif os.path.exists(old_path):
                try:
                    with open(old_path, "r", encoding="utf-8") as f:
                        old_data = f.read()
                    with open(new_path, "w", encoding="utf-8") as f:
                        f.write(old_data)
                    JSON_FILE_PATH = new_path
                    await message.reply_text(f"✅ Ma'lumotlar `{old_path}` dan `{new_path}` ga ko'chirildi.")
                except Exception as e:
                    log_error("JSON fayl ko'chirishda xato", e)
                    await message.reply_text(f"❌ Xatolik: {str(e)}")
            else:
                JSON_FILE_PATH = new_path
                load_json_data()
                await message.reply_text(f"✅ Yangi bo'sh baza yaratildi: `{JSON_FILE_PATH}`")

    # ---------- /errors ----------
    @client.on_message(filters.me & filters.chat("me") & filters.command("errors", prefixes="/"))
    async def show_errors_report(c, message):
        entries = parse_log(ERROR_LOG_PATH)
        report = build_log_report(entries)
        if len(report) > 4000:
            for i in range(0, len(report), 4000):
                await message.reply_text(report[i:i + 4000])
        else:
            await message.reply_text(report)

    # ---------- /errclear ----------
    @client.on_message(filters.me & filters.chat("me") & filters.command("errclear", prefixes="/"))
    async def clear_errors_log(c, message):
        if os.path.exists(ERROR_LOG_PATH):
            os.remove(ERROR_LOG_PATH)
            await message.reply_text("🧹 Xatolar jurnali tozalandi.")
        else:
            await message.reply_text("📭 Xatolar jurnali allaqachon bo'sh.")

    # ---------- /duplicates ----------
    @client.on_message(filters.me & filters.chat("me") & filters.command("duplicates", prefixes="/"))
    async def show_duplicates(c, message):
        async with json_lock:
            data = load_json_data()
        lessons = data.get("lessons", [])
        report, _ = build_duplicates_report(lessons)
        if len(report) > 4000:
            for i in range(0, len(report), 4000):
                await message.reply_text(report[i:i + 4000])
        else:
            await message.reply_text(report)

    # ---------- /delduplicates ----------
    @client.on_message(filters.me & filters.chat("me") & filters.command("delduplicates", prefixes="/"))
    async def delete_duplicates(c, message):
        args = message.text.split()
        also_archive = len(args) > 1 and args[1].lower() == "archive"

        async with json_lock:
            data = load_json_data()
            lessons = data.get("lessons", [])
            duplicates = find_duplicate_lessons(lessons)

            if not duplicates:
                await message.reply_text("✅ Takroriy darslar topilmadi.")
                return

            indices_to_remove = set()
            archive_delete_names = []

            for key, items in duplicates.items():
                items_sorted = sorted(items, key=lambda x: x[1].get("date", ""), reverse=True)
                for idx, lesson in items_sorted[1:]:
                    indices_to_remove.add(idx)
                    if also_archive:
                        clean_name = lesson["download_url"].rsplit("/", 1)[-1]
                        archive_delete_names.append(clean_name)

            new_lessons = [l for i, l in enumerate(lessons) if i not in indices_to_remove]
            data["lessons"] = new_lessons
            save_json_data(data)

        reply = f"🗑 {len(indices_to_remove)} ta takroriy dars JSON bazadan o'chirildi."

        if also_archive and archive_delete_names:
            await message.reply_text(reply + "\n⏳ Archive.org'dan ham o'chirilmoqda...")
            success_count = 0
            for name in archive_delete_names:
                ok = await delete_from_archive(name)
                if ok:
                    success_count += 1
            await message.reply_text(f"✅ Archive.org'dan {success_count}/{len(archive_delete_names)} ta fayl o'chirildi.")
        else:
            await message.reply_text(reply)

    # ---------- /stats ----------
    @client.on_message(filters.me & filters.chat("me") & filters.command("stats", prefixes="/"))
    async def show_stats(c, message):
        async with json_lock:
            data = load_json_data()
        await message.reply_text(build_stats_text(data))

    # ---------- /status ----------
    @client.on_message(filters.me & filters.chat("me") & filters.command("status", prefixes="/"))
    async def show_status(c, message):
        async with active_uploads_lock:
            snapshot = dict(active_uploads)

        if not snapshot:
            await message.reply_text(
                f"💤 Hozircha hech narsa yuklanmayapti.\n"
                f"⏳ Navbatda: {upload_queue.qsize()} ta fayl"
            )
            return

        lines = ["📡 **Joriy jarayon:**\n"]
        for worker_id, info in snapshot.items():
            uploaded_mb = format_mb(info["uploaded"])
            total_mb = format_mb(info["total"])
            lines.append(
                f"👷 Worker {worker_id}: `{info['name']}`\n"
                f"   {uploaded_mb:.2f}/{total_mb:.2f} MB ({info['percent']}%) — {info['stage']}"
            )
        lines.append(f"\n⏳ Navbatda kutilmoqda: {upload_queue.qsize()} ta")
        await message.reply_text("\n".join(lines))

    # ---------- /pause ----------
    @client.on_message(filters.me & filters.chat("me") & filters.command("pause", prefixes="/"))
    async def pause_uploads(c, message):
        manual_ok_event.clear()
        await message.reply_text("⏸ Yuklashlar qo'lda to'xtatildi. Davom ettirish: /resume")

    # ---------- /resume ----------
    @client.on_message(filters.me & filters.chat("me") & filters.command("resume", prefixes="/"))
    async def resume_uploads(c, message):
        manual_ok_event.set()
        await message.reply_text("▶️ Yuklashlar davom ettirildi.")

    # ---------- /pending ----------
    @client.on_message(filters.me & filters.chat("me") & filters.command("pending", prefixes="/"))
    async def show_pending(c, message):
        async with json_lock:
            data = load_json_data()
        pending_ids = data.get("queue", [])

        if not pending_ids:
            await message.reply_text("✅ Kutilayotgan yoki muvaffaqiyatsiz vazifalar yo'q.")
            return

        text = f"⏳ **Kutilayotgan/muvaffaqiyatsiz vazifalar** ({len(pending_ids)} ta):\n\n"
        for mid in pending_ids:
            text += f"• message_id: `{mid}`\n"
        text += "\n🔁 Qayta urinish: /retry <message_id>\n🔁 Barchasini qayta urinish: /retry all"
        await message.reply_text(text)

    # ---------- /retry ----------
    @client.on_message(filters.me & filters.chat("me") & filters.command("retry", prefixes="/"))
    async def retry_pending(c, message):
        args = message.text.split()
        if len(args) < 2:
            await message.reply_text("❗️ Foydalanish: /retry <message_id> yoki /retry all")
            return

        async with json_lock:
            data = load_json_data()
            pending_ids = data.get("queue", [])

        if args[1].lower() == "all":
            if not pending_ids:
                await message.reply_text("✅ Qayta urinish uchun hech narsa yo'q.")
                return
            for mid in pending_ids:
                status_msg = await message.reply_text(f"🔁 Qayta navbatga qo'shildi (ID: {mid})...")
                await upload_queue.put((c, mid, status_msg))
            await message.reply_text(f"✅ {len(pending_ids)} ta vazifa qayta navbatga qo'yildi.")
            return

        if not args[1].isdigit():
            await message.reply_text("❗️ message_id butun son bo'lishi kerak.")
            return

        mid = int(args[1])
        if mid not in pending_ids:
            await message.reply_text("❌ Bu ID navbatda topilmadi. /pending orqali tekshiring.")
            return

        status_msg = await message.reply_text(f"🔁 Qayta navbatga qo'shildi (ID: {mid})...")
        await upload_queue.put((c, mid, status_msg))

    # ---------- /rename ----------
    @client.on_message(filters.me & filters.chat("me") & filters.command("rename", prefixes="/"))
    async def rename_lesson(c, message):
        args = message.text.split(maxsplit=2)
        if len(args) < 3:
            await message.reply_text("❗️ Foydalanish: /rename <raqam> <yangi nom>")
            return

        async with json_lock:
            data = load_json_data()
            idx, lesson = find_lesson(data, args[1])
            if lesson is None:
                await message.reply_text("❌ Topilmadi.")
                return
            old_title = lesson["title"]
            lesson["title"] = args[2].strip()
            save_json_data(data)

        await message.reply_text(
            f"✅ Nom o'zgartirildi:\n"
            f"Eski: {old_title}\n"
            f"Yangi: {lesson['title']}\n\n"
            f"ℹ️ Eslatma: bu faqat JSON metama'lumotini o'zgartiradi, Archive.org'dagi fayl nomi o'zgarmaydi."
        )

    # ---------- /export ----------
    @client.on_message(filters.me & filters.chat("me") & filters.command("export", prefixes="/"))
    async def export_lessons(c, message):
        async with json_lock:
            data = load_json_data()
        lessons = data.get("lessons", [])

        if not lessons:
            await message.reply_text("📭 Export qilish uchun dars yo'q.")
            return

        txt_lines = [f"DARSLAR RO'YXATI — {time.strftime('%d.%m.%Y %H:%M')}\n" + ("=" * 40) + "\n"]
        for i, l in enumerate(lessons, start=1):
            lid = l.get("lesson_id")
            txt_lines.append(
                f"{i}. [{f'{lid:04d}' if lid else '—'}] {l['title']}\n"
                f"   Tavsif: {l.get('description', '—')}\n"
                f"   Hajmi: {l.get('capacity', '—')}\n"
                f"   Davomiyligi: {l.get('duration', '—')}\n"
                f"   Sana: {l.get('date', '—')}\n"
                f"   Havola: {l.get('download_url', '—')}\n"
            )
        txt_content = "\n".join(txt_lines)
        txt_path = "lessons_export.txt"
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(txt_content)

        csv_lines = ["ID;Nomi;Tavsif;Hajmi;Davomiyligi;Sana;Havola"]
        for l in lessons:
            lid = l.get("lesson_id", "")
            row = [
                str(lid),
                l.get("title", "").replace(";", ","),
                l.get("description", "").replace(";", ","),
                l.get("capacity", ""),
                l.get("duration", ""),
                l.get("date", ""),
                l.get("download_url", ""),
            ]
            csv_lines.append(";".join(row))
        csv_content = "\n".join(csv_lines)
        csv_path = "lessons_export.csv"
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write(csv_content)

        await message.reply_document(document=txt_path, caption=f"📄 {len(lessons)} ta dars (TXT format)")
        await message.reply_document(document=csv_path, caption=f"📊 {len(lessons)} ta dars (CSV format)")

        for p in (txt_path, csv_path):
            try:
                os.remove(p)
            except Exception:
                pass

    # ---------- /backup ----------
    @client.on_message(filters.me & filters.chat("me") & filters.command("backup", prefixes="/"))
    async def manual_backup(c, message):
        if not os.path.exists(JSON_FILE_PATH):
            await message.reply_text("❌ JSON fayl topilmadi, backup qilinmadi.")
            return

        files_to_send = [JSON_FILE_PATH]
        if os.path.exists(ERROR_LOG_PATH):
            files_to_send.append(ERROR_LOG_PATH)

        for fp in files_to_send:
            await message.reply_document(
                document=fp,
                caption=f"🔒 Qo'lda backup: {fp} ({time.strftime('%d.%m.%Y %H:%M')})"
            )

    # ---------- /verify ----------
    @client.on_message(filters.me & filters.chat("me") & filters.command("verify", prefixes="/"))
    async def verify_lesson(c, message):
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await message.reply_text("❗️ Foydalanish: /verify <raqam>")
            return

        async with json_lock:
            data = load_json_data()
        idx, lesson = find_lesson(data, args[1])
        if lesson is None:
            await message.reply_text("❌ Topilmadi.")
            return

        clean_name = lesson["download_url"].rsplit("/", 1)[-1]
        url = f"https://s3.us.archive.org/{ARCHIVE_IDENTIFIER}/{clean_name}"

        await message.reply_text(f"🔍 Tekshirilmoqda: {lesson['title']}...")

        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(20.0), follow_redirects=True) as check_client:
                resp = await check_client.head(url)

            if resp.status_code == 200:
                remote_size_mb = format_mb(int(resp.headers.get("content-length", 0)))
                await message.reply_text(
                    f"✅ **Fayl Archive.org'da mavjud.**\n"
                    f"📝 {lesson['title']}\n"
                    f"⚖️ Hozirgi hajm: {remote_size_mb:.2f} MB\n"
                    f"⚖️ JSON'dagi hajm: {lesson['capacity']}"
                )
            else:
                await message.reply_text(
                    f"❌ **Fayl Archive.org'da topilmadi!** (kod: {resp.status_code})\n"
                    f"📝 {lesson['title']}\n"
                    f"⚠️ Bu dars qayta yuklanishi kerak bo'lishi mumkin."
                )
        except Exception as e:
            await message.reply_text(f"⚠️ Tekshirishda xatolik: {str(e)}")

    # ---------- /help ----------
    @client.on_message(filters.me & filters.chat("me") & filters.command("help", prefixes="/"))
    async def show_help(c, message):
        await message.reply_text(
            "🤖 **Buyruqlar ro'yxati**\n\n"
            "**Darslar bilan ishlash:**\n"
            "/list [sahifa] — ro'yxat\n"
            "/view <raqam> — batafsil\n"
            "/search <matn> — qidirish\n"
            "/rename <raqam> <nom> — nomini o'zgartirish\n"
            "/delete <raqam> [archive] — o'chirish\n"
            "/verify <raqam> — Archive'da mavjudligini tekshirish\n\n"
            "**Navbat va jarayon:**\n"
            "/status — joriy yuklash holati\n"
            "/pending — kutilayotgan/muvaffaqiyatsiz vazifalar\n"
            "/retry <id|all> — qayta urinish\n"
            "/pause /resume — to'xtatish/davom ettirish\n"
            "/clear — navbatni tozalash\n\n"
            "**Statistika va tahlil:**\n"
            "/stats — umumiy statistika\n"
            "/duplicates — takroriy darslar\n"
            "/delduplicates [archive] — takroriylarni o'chirish\n"
            "/errors — xatolar tahlili\n"
            "/errclear — xatolar jurnalini tozalash\n\n"
            "**Fayl va backup:**\n"
            "/export — TXT/CSV qilib yuborish\n"
            "/download — data.json yuborish\n"
            "/backup — qo'lda backup\n\n"
            "**Sozlamalar:**\n"
            "/settings — joriy sozlamalar\n"
            "/set identifier <nom> — Archive.org sahifasini o'zgartirish\n"
            "/set <nom> <qiymat> — sonli sozlama o'zgartirish\n"
            "/setjson <fayl.json> — baza faylini almashtirish\n"
            "/res — botni qayta ishga tushirish"
        )

# ================== ISHGA TUSHIRISH ==================
async def main():
    global app
    app = Client(
        name=session_path,
        api_id=api_id,
        api_hash=api_hash,
        system_version="7.1.2 Windows"
    )

    await setup_handlers(app)
    for worker_id in range(MAX_PARALLEL_UPLOADS):
        asyncio.create_task(queue_worker(worker_id=worker_id + 1))

    print(
        f"✅ Stream Uploader ishga tushdi (RAM-only streaming rejimi).\n"
        f"🎯 Maqsad: '{ARCHIVE_IDENTIFIER}' sahifasi.\n"
        f"🚀 Workerlar soni: {MAX_PARALLEL_UPLOADS}"
    )
    await app.start()

    await restore_queue_from_json(app)

    asyncio.create_task(periodic_backup_task(app))
    asyncio.create_task(daily_report_task(app))

    await idle()
    await app.stop()

if __name__ == "__main__":
    wake_lock_acquire()
    try:
        current_loop = asyncio.get_event_loop()
        current_loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("\n🛑 Skript to'xtatildi.")
    finally:
        wake_lock_release()