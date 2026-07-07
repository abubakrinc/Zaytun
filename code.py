import os
import re
import sys
import time
import json
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

# ================== SOZLAMALAR ==================
api_id = int(os.environ.get("API_ID", "30778935"))
api_hash = os.environ.get("API_HASH", "2114df663586afba07d8fffd3b9d5d70")

ARCHIVE_ACCESS_KEY = os.environ.get("ARCHIVE_ACCESS_KEY", "KW28vrie4W934nnX")
ARCHIVE_SECRET_KEY = os.environ.get("ARCHIVE_SECRET_KEY", "FHkr9TX3Sh5dXSvb")

ARCHIVE_IDENTIFIER = "tafsir-sadiy-abdulhadi-domla"
JSON_FILE_PATH = "data.json"

WATCHDOG_TIMEOUT = 60 * 30       
MAX_CONSECUTIVE_FAILURES = 3     
PAUSE_DURATION = 180             
MAX_PARALLEL_UPLOADS = 1

json_lock = asyncio.Lock()
upload_queue = asyncio.Queue()

worker_state = {"consecutive_failures": 0}
worker_state_lock = asyncio.Lock()
pause_lock = asyncio.Lock()
network_ok_event = asyncio.Event()
network_ok_event.set()  

app = None

# ================== JSON BAZA ==================
def load_json_data():
    if not os.path.exists(JSON_FILE_PATH):
        default_data = {"lessons": [], "queue": []}
        with open(JSON_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(default_data, f, indent=4, ensure_ascii=False)
        return default_data
    try:
        with open(JSON_FILE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            data.setdefault("lessons", [])
            data.setdefault("queue", [])
            return data
    except Exception:
        return {"lessons": [], "queue": []}

def save_json_data(data):
    tmp_path = JSON_FILE_PATH + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    os.replace(tmp_path, JSON_FILE_PATH)

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

termux_home = os.path.expanduser("~")
session_path = os.path.join(termux_home, "archive_stream_uploader")
ERROR_LOG_PATH = os.path.join(termux_home, "upload_errors.log")

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

# ================== TELEGRAM XABARINI XAVFSIZ TAHRIRLASH ==================
async def safe_edit(msg, text):
    """edit_text'ni xavfsiz chaqiradi: MessageNotModified va boshqa
    Telegram xatoliklarini jim yutadi, shuning uchun 'Task exception was
    never retrieved' ogohlantirishlari konsolni to'ldirmaydi."""
    try:
        await msg.edit_text(text)
    except Exception:
        pass

# ================== INTERNET TEKSHIRUVI OLIB TASHLANDI ==================
# Eslatma: bu yerda avval archive.org'ga HEAD so'rov yuborib internetni
# tekshiruvchi funksiya bor edi, lekin Archive.org o'zi sekin javob berganda
# ham xato ravishda "internet yo'q" deb ko'rsatib yuborardi. Shu sabab olib
# tashlandi — endi tarmoq muammolari faqat haqiqiy PUT/stream xatoliklari
# orqali (pastdagi retry mexanizmi bilan) aniqlanadi.

# ================== OQIMLI (STREAM) YUKLASH ==================
async def upload_stream_direct(client, message, clean_name, file_size, status_msg, worker_id):
    archive_put_url = f"https://s3.us.archive.org/{ARCHIVE_IDENTIFIER}/{clean_name}"
    headers = {
        "Authorization": f"LOW {ARCHIVE_ACCESS_KEY}:{ARCHIVE_SECRET_KEY}",
        "Content-Type": get_content_type(clean_name),
        "Content-Length": str(file_size),
        "x-archive-media-type": "audio",
        "x-archive-meta-title": clean_name.rsplit('.', 1)[0],
        "x-amz-auto-make-bucket": "1"
    }

    max_retries = 5

    for attempt in range(1, max_retries + 1):
        progress = {
            "uploaded": 0, 
            "start_time": time.time(),
            "last_console_update": 0,
            "last_tg_update": 0
        }
        
        spinners = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
        spinner_idx = [0] # List ishlatildi, chunki ichki funksiyada o'zgaradi

        async def stream_generator():
            try:
                async for chunk in client.stream_media(message):
                    chunk_size = len(chunk)
                    progress["uploaded"] += chunk_size
                    now = time.time()
                    
                    # 1. Konsolni tez-tez, lekin protsessorni qiynamaydigan darajada yangilash (0.2 sek)
                    if now - progress["last_console_update"] > 0.2:
                        elapsed = now - progress["start_time"]
                        speed = (progress["uploaded"] / elapsed / 1024) if elapsed > 0 else 0
                        percent = (progress["uploaded"] / file_size) * 100 if file_size else 0
                        spinner = spinners[spinner_idx[0] % len(spinners)]
                        spinner_idx[0] += 1
                        
                        sys.stdout.write(f"\r\033[K[{worker_id}] {spinner} Oqim: {format_mb(progress['uploaded']):.2f}/{format_mb(file_size):.2f} MB ({percent:.1f}%) | 📡 {format_speed(speed)}")
                        sys.stdout.flush()
                        progress["last_console_update"] = now

                    # 2. Telegram statusini kamroq yangilash (Flood Wait xatosi va batareya tejamkorligi uchun - 7 sek)
                    if now - progress["last_tg_update"] > 7.0:
                        elapsed = now - progress["start_time"]
                        speed = (progress["uploaded"] / elapsed / 1024) if elapsed > 0 else 0
                        percent = int((progress["uploaded"] / file_size) * 100) if file_size else 0
                        progress["last_tg_update"] = now
                        
                        # Fonda yangilash (xavfsiz - xatoликlar jim yutiladi)
                        asyncio.create_task(safe_edit(
                            status_msg,
                            f"🚀 **To'g'ridan-to'g'ri oqim (Stream)...**\n"
                            f"📦 {format_mb(progress['uploaded']):.2f} MB / {format_mb(file_size):.2f} MB ({percent}%)\n"
                            f"📡 Tezlik: {format_speed(speed)}\n"
                            f"⚙️ Urinish: {attempt}/{max_retries}"
                        ))

                    yield chunk
            except Exception as e:
                log_error(f"Stream uzilishi ({clean_name})", e)
                raise e

        transport = httpx.AsyncHTTPTransport(limits=httpx.Limits(max_connections=1, max_keepalive_connections=1), retries=0)
        timeout = httpx.Timeout(connect=30.0, read=120.0, write=120.0, pool=30.0)

        try:
            async with httpx.AsyncClient(transport=transport, timeout=timeout) as async_client:
                response = await async_client.put(archive_put_url, headers=headers, content=stream_generator())
                response.raise_for_status()
            
            print() # Qatorni tozalash

            # ===== ARCHIVE FAYL TEKSHIRUVI (TUZATILGAN) =====
            # MUHIM TUZATISH: HEAD so'rovda follow_redirects=True yo'q edi.
            # Archive.org yuklangandan keyin ba'zan HEAD so'rovga redirect (3xx)
            # qaytaradi (fayl hali indekslanmoqda); redirect'ning o'zi ~406 baytlik
            # kichik sahifa bo'lib, u asl fayl bilan solishtirilib "mos kelmadi"
            # deb noto'g'ri xato chiqargan. Endi: redirectlarni kuzatamiz,
            # va fayl hali tayyor bo'lmasligi mumkinligi uchun bir necha marta,
            # orada kutib, qayta tekshiramiz — shunda muvaffaqiyatli yuklangan
            # faylni behuda qaytadan yuklashning oldi olinadi.
            integrity_ok = False
            last_check_err = None
            check_attempts = 4
            for check_attempt in range(1, check_attempts + 1):
                await asyncio.sleep(3 if check_attempt == 1 else 8)
                try:
                    async with httpx.AsyncClient(
                        timeout=httpx.Timeout(30.0), follow_redirects=True
                    ) as check_client:
                        head_resp = await check_client.head(archive_put_url)
                        if head_resp.status_code == 200:
                            remote_size = int(head_resp.headers.get("content-length", 0))
                            if remote_size == file_size:
                                integrity_ok = True
                                break
                            else:
                                last_check_err = ValueError(
                                    f"Fayl hajmi mos kelmadi: kutilgan {file_size}, "
                                    f"Archive'da {remote_size} (urinish {check_attempt}/{check_attempts})"
                                )
                        else:
                            # Fayl hali indekslanmoqda bo'lishi mumkin — darhol xato deb hisoblamaymiz
                            last_check_err = ValueError(
                                f"Tekshiruv kodi {head_resp.status_code} — fayl hali tayyor "
                                f"bo'lmasligi mumkin (urinish {check_attempt}/{check_attempts})"
                            )
                except Exception as e:
                    last_check_err = e

            if not integrity_ok:
                raise last_check_err or Exception("Fayl butunligini tasdiqlab bo'lmadi")

            return response.status_code

        except Exception as e:
            print() 
            log_error(f"Oqimli yuklash xatosi ({clean_name}), urinish {attempt}", e)
            if attempt == max_retries:
                raise e
            
            wait_time = min(10 * (2 ** (attempt - 1)), 120)
            try:
                await status_msg.edit_text(
                    f"⚠️ Tarmoq uzildi. Oqim boshidan boshlanadi (Urinish {attempt}/{max_retries}).\n"
                    f"{wait_time} soniyadan keyin qayta uriniladi..."
                )
            except Exception:
                pass
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

            clean_name = f"{name_part}_{msg_id}{ext_part}"
            file_size = media.file_size
            capacity_mb = format_mb(file_size)

            print(f"\n[{worker_id}] Vazifa: {clean_name} ({capacity_mb:.2f} MB)")

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

            await status_msg.edit_text(
                f"✅ **Oqim orqali yuklandi!**\n\n"
                f"📝 **Nomi:** {title}\n"
                f"📄 **Tavsif:** {description}\n"
                f"⚖️ **Hajmi:** {capacity_mb:.2f} MB\n"
                f"⏱ **Davomiyligi:** {duration_str}\n"
                f"🕐 **Vaqt:** {current_time}\n"
                f"🔗 {archive_url}",
                disable_web_page_preview=True
            )
            
            print(f"[{worker_id}] ✅ Muaffaqiyatli: {clean_name}")
            success = True

        except asyncio.TimeoutError:
            log_error(f"Watchdog timeout (message_id={message_id})", "Tugatishga ulgurmadi")
            print(f"\n[{worker_id}] ❌ Xatolik: Belgilangan vaqt ichida yakunlanmadi.")
            try:
                await status_msg.edit_text("❌ Xatolik: Timeout (internet aloqasi juda sekin bo'lishi mumkin).")
            except Exception:
                pass
        except Exception as e:
            log_error(f"Umumiy xatolik (message_id={message_id})", e)
            print(f"\n[{worker_id}] ❌ Xatolik: {str(e)}")
            try:
                await status_msg.edit_text(f"❌ Xatolik: {str(e)}")
            except Exception:
                pass
        finally:
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
        f"✅ Stream Uploader ishga tushdi.\n"
        f"🎯 Maqsad: '{ARCHIVE_IDENTIFIER}' sahifasi.\n"
        f"🚀 Workerlar soni: {MAX_PARALLEL_UPLOADS}"
    )
    await app.start()

    await restore_queue_from_json(app)
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
