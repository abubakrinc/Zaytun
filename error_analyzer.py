import re
import os
import json
import sys
from collections import defaultdict, Counter
from datetime import datetime

ERROR_LOG_PATH = "upload_errors.log"
DEFAULT_JSON_PATH = "data.json"

# Log qatori formati: [2026-07-07 14:23:11] context | error
LOG_LINE_RE = re.compile(r"^\[(?P<ts>[\d\-: ]+)\]\s*(?P<context>.*?)\s*\|\s*(?P<error>.*)$")

NAME_RE = re.compile(r"\(([^)]+)\)")
ATTEMPT_RE = re.compile(r"urinish\s+(\d+)")


# ================== XATOLAR JURNALI TAHLILI ==================
def classify_error(context: str, error: str) -> str:
    text = f"{context} {error}".lower()
    if "503" in text or "slowdown" in text:
        return "503_slowdown"
    if "watchdog timeout" in context.lower() or "timeout" in text:
        return "timeout"
    if "hajm mos kelmadi" in text:
        return "integrity_mismatch"
    if "check_limit" in context.lower():
        return "check_limit_error"
    if "o'chirishda xato" in context.lower():
        return "delete_error"
    if "json fayl ko'chirishda" in context.lower():
        return "json_migration_error"
    if "umumiy xatolik" in context.lower():
        return "general_error"
    return "other"


ERROR_TYPE_LABELS = {
    "503_slowdown": "⏸ 503 SlowDown (server bandligi)",
    "timeout": "⏱ Watchdog Timeout",
    "integrity_mismatch": "⚖️ Hajm/Butunlik nomosligi",
    "check_limit_error": "🔍 check_limit so'rovi xatosi",
    "delete_error": "🗑 O'chirishda xato",
    "json_migration_error": "📄 JSON ko'chirish xatosi",
    "general_error": "❗️ Umumiy xatolik",
    "other": "❓ Boshqa",
}


def parse_log(path: str = ERROR_LOG_PATH):
    entries = []
    if not os.path.exists(path):
        return entries

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            m = LOG_LINE_RE.match(line)
            if not m:
                continue

            ts_str = m.group("ts")
            context = m.group("context")
            error = m.group("error")

            try:
                ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                ts = None

            name_match = NAME_RE.search(context)
            identifier = name_match.group(1) if name_match else "noma'lum"

            attempt_match = ATTEMPT_RE.search(context)
            attempt = int(attempt_match.group(1)) if attempt_match else None

            error_type = classify_error(context, error)

            entries.append({
                "timestamp": ts,
                "raw_ts": ts_str,
                "context": context,
                "error": error,
                "identifier": identifier,
                "attempt": attempt,
                "type": error_type,
            })

    return entries


def build_log_report(entries, top_n: int = 10) -> str:
    if not entries:
        return "📭 Xatolar jurnali bo'sh yoki topilmadi."

    total = len(entries)
    per_identifier = Counter(e["identifier"] for e in entries)
    per_type = Counter(e["type"] for e in entries)

    max_attempts = defaultdict(int)
    for e in entries:
        if e["attempt"]:
            max_attempts[e["identifier"]] = max(max_attempts[e["identifier"]], e["attempt"])

    timestamps = [e["timestamp"] for e in entries if e["timestamp"]]
    first_ts = min(timestamps).strftime("%d.%m.%Y %H:%M") if timestamps else "—"
    last_ts = max(timestamps).strftime("%d.%m.%Y %H:%M") if timestamps else "—"

    recent_count = 0
    if timestamps:
        now = max(timestamps)
        recent_count = sum(1 for t in timestamps if (now - t).total_seconds() <= 86400)

    lines = []
    lines.append("📊 **XATOLAR TAHLILI**\n")
    lines.append(f"🗂 Jami yozuvlar: **{total}**")
    lines.append(f"🕐 Birinchi xato: {first_ts}")
    lines.append(f"🕐 Oxirgi xato: {last_ts}")
    lines.append(f"🔥 So'nggi 24 soatda: {recent_count} ta\n")

    lines.append("**— Xato turlari bo'yicha —**")
    for etype, count in per_type.most_common():
        label = ERROR_TYPE_LABELS.get(etype, etype)
        percent = (count / total) * 100
        lines.append(f"{label}: {count} ta ({percent:.1f}%)")

    lines.append(f"\n**— Eng ko'p xato bergan darslar (TOP {top_n}) —**")
    for ident, count in per_identifier.most_common(top_n):
        max_att = max_attempts.get(ident)
        att_info = f" | max urinish: {max_att}" if max_att else ""
        lines.append(f"• `{ident}` — {count} marta{att_info}")

    chronic = [(ident, c) for ident, c in per_identifier.items() if c >= 3]
    if chronic:
        lines.append(f"\n⚠️ **Doimiy muammoli darslar (3+ xato):**")
        for ident, count in sorted(chronic, key=lambda x: -x[1]):
            lines.append(f"• `{ident}` — {count} marta")

    return "\n".join(lines)


# ================== TAKRORIY DARSLARNI ANIQLASH ==================
def load_lessons(json_path: str = DEFAULT_JSON_PATH):
    if not os.path.exists(json_path):
        return []
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data.get("lessons", [])
    except Exception:
        return []


def normalize_title(title: str) -> str:
    t = (title or "").strip().lower()
    t = re.sub(r'\s+', ' ', t)
    return t


def parse_lesson_date(date_str: str):
    try:
        return datetime.strptime(date_str, "%d.%m.%Y %H:%M")
    except Exception:
        return None


def find_duplicate_lessons(lessons):
    """Bir xil nomdagi darslarni guruhlaydi (2+ marta yuklanganlar)"""
    groups = defaultdict(list)
    for idx, lesson in enumerate(lessons):
        key = normalize_title(lesson.get("title", ""))
        if key:
            groups[key].append((idx, lesson))

    return {key: items for key, items in groups.items() if len(items) > 1}


def build_duplicates_report(lessons, top_n: int = 20):
    """
    Qaytaradi: (matn_hisobot, delete_candidates)
    delete_candidates — [(index, lesson_dict), ...] — o'chirish tavsiya etilgan (eski nusxalar)
    """
    duplicates = find_duplicate_lessons(lessons)
    if not duplicates:
        return "✅ Takroriy (1 martadan ko'p yuklangan) darslar topilmadi.", []

    lines = [f"🔁 **TAKRORIY DARSLAR** — {len(duplicates)} ta guruh topildi\n"]
    delete_candidates = []
    shown = 0

    for key, items in sorted(duplicates.items(), key=lambda x: -len(x[1])):
        if shown >= top_n:
            lines.append(f"\n… va yana {len(duplicates) - shown} ta guruh (to'liq ro'yxat uchun /delduplicates ishlating)")
            break
        shown += 1

        items_sorted = sorted(
            items,
            key=lambda x: parse_lesson_date(x[1].get("date", "")) or datetime.min,
            reverse=True
        )
        keep_idx, keep_lesson = items_sorted[0]
        remove_items = items_sorted[1:]

        title_display = keep_lesson.get("title", "noma'lum")
        lines.append(f"**\"{title_display}\"** — {len(items)} marta yuklangan:")
        lines.append(f"  ✅ Saqlanadi (eng yangi): {keep_lesson.get('date', '—')} | {keep_lesson.get('capacity', '—')}")
        for idx, lesson in remove_items:
            lines.append(f"  ❌ O'chiriladi: {lesson.get('date', '—')} | {lesson.get('capacity', '—')}")
            delete_candidates.append((idx, lesson))
        lines.append("")

    total_removable = sum(len(items) - 1 for items in duplicates.values())
    lines.append(f"📌 Jami o'chirish mumkin bo'lgan nusxalar: **{total_removable} ta**")
    lines.append("🗑 Avtomatik tozalash: /delduplicates")
    lines.append("🗑 Archive.org'dan ham o'chirish: /delduplicates archive")

    return "\n".join(lines), delete_candidates


# ================== TO'LIQ HISOBOT (log + takroriylar) ==================
def build_full_report(json_path: str = DEFAULT_JSON_PATH, log_path: str = ERROR_LOG_PATH, top_n: int = 10):
    entries = parse_log(log_path)
    lessons = load_lessons(json_path)

    log_report = build_log_report(entries, top_n=top_n)
    dup_report, _ = build_duplicates_report(lessons, top_n=top_n)

    return log_report + "\n\n" + ("─" * 30) + "\n\n" + dup_report


def main():
    print(build_full_report())


if __name__ == "__main__":
    main()