#!/usr/bin/env python3
"""
Моніторинг завантаженості конкурентів через Altegio API.
Збирає дані на сьогодні і перевіряє кожну подію за 5 хвилин до старту.

Типи подій:
  - individual: індивідуальні послуги (book_times / book_staff)
  - group:      групові події (activity/search)
"""

import requests
import json
import csv
import os
import time
import threading
from datetime import datetime, date, timedelta

try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False

# ========================
# НАЛАШТУВАННЯ
# ========================

TOKEN = "gtcwf654agufy25gsadh"

COMPETITORS = [
    {
        "name": "Kuzmin Pilates",
        "company_id": 1287498,
        "base_url": "https://n1348393.alteg.io",
        "token": TOKEN,
    },
    {
        "name": "Sàmma Pilates",
        "company_id": 1262942,
        "base_url": "https://n1320145.alteg.io",
        "token": TOKEN,
    },
    {
        "name": "Reform Avenue",
        "company_id": 1279334,
        "base_url": "https://n1339183.alteg.io",
        "token": TOKEN,
    },
    {
        "name": "Miss Power",
        "company_id": 1325395,
        "base_url": "https://n1391198.alteg.io",
        "token": TOKEN,
    },
    {
        "name": "The Body Pilates Studio",
        "company_id": 1327203,
        "base_url": "https://n1393202.alteg.io",
        "token": TOKEN,
    },
]

CHECK_BEFORE_MINUTES = 5
DATA_FILE      = "occupancy_data.csv"
CONFIG_FILE    = "config.json"
BASELINE_FILE  = "individual_baseline.json"

# ── Google Sheets ─────────────────────────────────────────
GOOGLE_CREDENTIALS_FILE = "google_credentials.json"
GOOGLE_SPREADSHEET_ID   = ""
GOOGLE_SHEET_LOG        = "Log"
GOOGLE_SHEET_INDIVIDUAL = "Індивідуальні"
GOOGLE_SHEET_GROUP      = "Групові"
GOOGLE_SHEET_TABLE      = "Таблиця"
GOOGLE_SHEET_STATUS     = "Статус"
STATUS_MAX_ROWS         = 30  # скільки останніх оновлень показувати
# ─────────────────────────────────────────────────────────


# ========================
# ПЕРШИЙ ЗАПУСК / КОНФІГ
# ========================

def _extract_spreadsheet_id(url_or_id: str) -> str:
    """Витягує ID таблиці з повного URL або повертає як є."""
    import re
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", url_or_id)
    return m.group(1) if m else url_or_id.strip()


def load_config():
    """Завантажити конфіг з config.json (якщо є)."""
    global GOOGLE_SPREADSHEET_ID, GOOGLE_CREDENTIALS_FILE, COMPETITORS
    if not os.path.exists(CONFIG_FILE):
        return False
    with open(CONFIG_FILE, encoding="utf-8") as f:
        cfg = json.load(f)
    GOOGLE_SPREADSHEET_ID   = cfg.get("spreadsheet_id", "")
    GOOGLE_CREDENTIALS_FILE = cfg.get("credentials_file", "google_credentials.json")
    saved = cfg.get("competitors")
    if saved:
        COMPETITORS = saved
        # Переконатись що токен прописаний у кожного
        for c in COMPETITORS:
            if not c.get("token"):
                c["token"] = TOKEN
    return True


def save_config():
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump({
            "spreadsheet_id":   GOOGLE_SPREADSHEET_ID,
            "credentials_file": GOOGLE_CREDENTIALS_FILE,
            "competitors": [
                {"name": c["name"], "company_id": c["company_id"], "base_url": c["base_url"]}
                for c in COMPETITORS
            ],
        }, f, ensure_ascii=False, indent=2)


def setup():
    """Інтерактивне налаштування при першому запуску."""
    global GOOGLE_SPREADSHEET_ID, GOOGLE_CREDENTIALS_FILE

    print("=" * 55)
    print("  ПЕРШИЙ ЗАПУСК — налаштування")
    print("=" * 55)

    # ── 1. Google Таблиця ─────────────────────────────────
    print("\nКрок 1/4 — Google Таблиця")
    print("Відкрийте таблицю в браузері та скопіюйте посилання.")
    while True:
        raw = input("  Вставте посилання (або ID): ").strip()
        if not raw:
            print("  ! Не може бути порожнім, спробуйте ще раз.")
            continue
        GOOGLE_SPREADSHEET_ID = _extract_spreadsheet_id(raw)
        print(f"  ✓ ID таблиці: {GOOGLE_SPREADSHEET_ID}")
        break

    # ── 2. Файл credentials ───────────────────────────────
    print("\nКрок 2/4 — Файл Google credentials")
    print("Де знаходиться ваш google_credentials.json?")
    print(f"  [Enter] — залишити поточний шлях: {GOOGLE_CREDENTIALS_FILE}")
    raw = input("  Шлях до файлу: ").strip()
    if raw:
        GOOGLE_CREDENTIALS_FILE = raw

    if not os.path.exists(GOOGLE_CREDENTIALS_FILE):
        print()
        print("  ⚠️  Файл НЕ знайдено!")
        print("  Щоб отримати credentials:")
        print("  1. Зайдіть на https://console.cloud.google.com")
        print("  2. Створіть проєкт → увімкніть Google Sheets API")
        print("  3. IAM → Сервісні акаунти → Створити → Завантажити JSON")
        print("  4. Надайте доступ до таблиці цьому сервісному акаунту (редактор)")
        print(f"  5. Покладіть файл за шляхом: {GOOGLE_CREDENTIALS_FILE}")
        print()
        input("  Натисніть Enter коли файл буде готовий...")
        if not os.path.exists(GOOGLE_CREDENTIALS_FILE):
            print("  ! Файл все ще не знайдено. Продовжуємо без Google Sheets.")
    else:
        print(f"  ✓ Файл знайдено: {GOOGLE_CREDENTIALS_FILE}")

    # ── 3. Конкуренти ────────────────────────────────────
    print("\nКрок 3/4 — Список конкурентів")
    print(f"  Зараз у списку {len(COMPETITORS)} студій:")
    for i, c in enumerate(COMPETITORS, 1):
        print(f"    {i}. {c['name']}  (company_id={c['company_id']})")
    print()
    print("  Що зробити?")
    print("  [Enter] — залишити як є")
    print("  d <номер> — видалити (наприклад: d 2)")
    print("  a — додати нову студію")
    print("  q — завершити редагування")
    while True:
        cmd = input("  > ").strip().lower()
        if cmd in ("", "q"):
            break
        elif cmd.startswith("d "):
            try:
                idx = int(cmd[2:]) - 1
                removed = COMPETITORS.pop(idx)
                print(f"  ✓ Видалено: {removed['name']}")
                for i, c in enumerate(COMPETITORS, 1):
                    print(f"    {i}. {c['name']}")
            except (ValueError, IndexError):
                print("  ! Невірний номер")
        elif cmd == "a":
            name = input("  Назва студії: ").strip()
            if not name:
                print("  ! Назва не може бути порожньою")
                continue
            cid = input("  company_id (число): ").strip()
            if not cid.isdigit():
                print("  ! company_id має бути числом")
                continue
            base_url = input("  base_url (напр. https://n1234567.alteg.io): ").strip()
            if not base_url:
                print("  ! base_url не може бути порожнім")
                continue
            COMPETITORS.append({"name": name, "company_id": int(cid), "base_url": base_url, "token": TOKEN})
            print(f"  ✓ Додано: {name}")
        else:
            print("  ! Невідома команда. Введіть d <номер>, a або Enter/q")

    save_config()
    print("\n  Конфіг збережено у config.json")

    # ── 4. Cron ───────────────────────────────────────────
    print()
    print("Крок 4/4 — Автозапуск щодня о 06:00")
    print("  Налаштувати cron щоб скрипт запускався автоматично?")
    answer = input("  [y/n]: ").strip().lower()
    if answer in ("y", "yes"):
        _setup_cron()
    else:
        print("  Пропущено. Запускайте вручну: python3 competitor_occupancy.py")

    print("=" * 55 + "\n")


def _setup_cron():
    """Додати два cron-завдання:
    1. Щодня о 06:00 — повний запуск моніторингу
    2. Щогодини       — оновлення індивідуальних слотів на 7 днів
    """
    import subprocess
    script_path = os.path.abspath(__file__)
    script_dir  = os.path.dirname(script_path)
    log         = f"{script_dir}/occupancy.log"

    daily_cron  = f"0 6 * * * cd {script_dir} && python3 {script_path} >> {log} 2>&1"
    hourly_cron = (
        f"0 * * * * cd {script_dir} && python3 -c "
        f"\"import competitor_occupancy as m; m.load_config(); m.gs_update_individual_week()\" "
        f">> {log} 2>&1"
    )

    result  = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    current = result.stdout if result.returncode == 0 else ""

    to_add = [l for l in [daily_cron, hourly_cron] if l not in current]
    if not to_add:
        print("  ✓ Cron вже налаштовано.")
        return

    new_crontab = current.rstrip("\n") + "\n" + "\n".join(to_add) + "\n"
    proc = subprocess.run(["crontab", "-"], input=new_crontab, text=True, capture_output=True)
    if proc.returncode == 0:
        print("  ✓ Cron налаштовано:")
        print("    • щодня о 06:00 — повний моніторинг")
        print("    • щогодини      — оновлення індивідуальних слотів (7 днів)")
    else:
        print(f"  ! Помилка: {proc.stderr}")

# ========================
# API
# ========================

def api_get(url, token):
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=10)
    r.raise_for_status()
    return r.json()


def get_staff(base_url, company_id, token):
    data = api_get(f"{base_url}/api/v1/book_staff/{company_id}?without_seances=1", token)
    return data if isinstance(data, list) else data.get("data", [])


def get_individual_slots(base_url, company_id, staff_id, date_str, token):
    data = api_get(f"{base_url}/api/v1/book_times/{company_id}/{staff_id}/{date_str}", token)
    return data if isinstance(data, list) else []


def get_group_events(base_url, company_id, date_str, token):
    data = api_get(
        f"{base_url}/api/v1/activity/{company_id}/search"
        f"?count=100&from={date_str}&till={date_str}&page=1",
        token
    )
    return data.get("data", []) if data.get("success") else []


def get_group_event_by_id(base_url, company_id, event_id, token):
    """Отримати актуальний стан конкретної групової події за прямим endpoint."""
    data = api_get(f"{base_url}/api/v1/activity/{company_id}/{event_id}", token)
    if data.get("success"):
        return data["data"]
    return None


# ========================
# CSV
# ========================

FIELDNAMES = [
    "checked_at", "competitor", "company_id", "event_type",
    "event_id", "event_name", "date", "slot_time",
    "staff_name", "capacity", "records_count", "places_left",
    "occupancy_pct", "status",
]


def save_row(row: dict):
    # CSV
    file_exists = os.path.exists(DATA_FILE)
    with open(DATA_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        writer.writerow({k: row.get(k, "") for k in FIELDNAMES})
    # Google Sheets
    gs_append_log(row)


# ========================
# GOOGLE SHEETS
# ========================

_gs_client = None
_gs_log_ws  = None   # кешований worksheet Log
_gs_log_ready = False  # чи вже записаний заголовок

def get_gs_client():
    global _gs_client
    if _gs_client:
        return _gs_client
    if not GSPREAD_AVAILABLE:
        return None
    if not GOOGLE_SPREADSHEET_ID or not os.path.exists(GOOGLE_CREDENTIALS_FILE):
        return None
    try:
        creds = Credentials.from_service_account_file(
            GOOGLE_CREDENTIALS_FILE,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        _gs_client = gspread.authorize(creds)
        return _gs_client
    except Exception as e:
        print(f"  [GSheets] Помилка авторизації: {e}")
        return None


def gs_init_log():
    """Ініціалізувати аркуш Log один раз: кешувати worksheet і записати заголовок."""
    global _gs_log_ws, _gs_log_ready
    if _gs_log_ready:
        return
    gc = get_gs_client()
    if not gc:
        return
    try:
        sh = gc.open_by_key(GOOGLE_SPREADSHEET_ID)
        _gs_log_ws = _ensure_worksheet(sh, GOOGLE_SHEET_LOG)
        _gs_log_ws.clear()
        _gs_log_ws.append_row(FIELDNAMES)
        _gs_log_ready = True
        print("  [GSheets] Аркуш Log ініціалізовано")
    except Exception as e:
        print(f"  [GSheets] Помилка ініціалізації Log: {e}")


def gs_append_log(row: dict):
    """Дозаписати рядок у аркуш Log (без зайвих read-запитів)."""
    global _gs_log_ws, _gs_log_ready
    if not _gs_log_ready or not _gs_log_ws:
        return
    try:
        _gs_log_ws.append_row(
            [str(row.get(k, "")) for k in FIELDNAMES],
            value_input_option="USER_ENTERED"
        )
    except Exception as e:
        print(f"  [GSheets] Помилка запису в Log: {e}")
    gs_update_status(row)


_gs_status_rows = []   # буфер останніх оновлень (в пам'яті)
_gs_prev_state  = {}   # попередній стан: event_key -> (було_рядок, records_count, places_left)

STATUS_HEADERS = [
    "оновлено", "студія", "тип", "тренер / подія", "час",
    "було", "стало", "зміна",
]


def _format_state(row: dict) -> str:
    """Форматує стан події для колонок «було» / «стало»."""
    etype = row.get("event_type", "")
    if etype == "heartbeat":
        return row.get("event_name", "")
    if etype == "individual":
        return row.get("status", "")   # «вільний» / «зайнятий»
    # group
    rec = row.get("records_count", "")
    cap = row.get("capacity", "")
    pct = row.get("occupancy_pct", "")
    if rec != "" and cap != "":
        return f"{rec}/{cap} ({pct}%)"
    return row.get("status", "")


def gs_update_status(row: dict):
    """Оновити аркуш Статус: показати останні STATUS_MAX_ROWS змін у форматі Було → Стало."""
    global _gs_status_rows, _gs_prev_state
    gc = get_gs_client()
    if not gc or not GOOGLE_SPREADSHEET_ID:
        return
    try:
        etype = row.get("event_type", "")
        staff = row.get("staff_name", "")
        event = row.get("event_name", "")
        label = f"{event} ({staff})" if event and staff else (event or staff)

        state_now = _format_state(row)

        if etype == "heartbeat":
            було = ""
            стало = state_now
            зміна = ""
        else:
            event_key = (
                row.get("competitor", ""),
                etype,
                row.get("event_id", ""),
                row.get("staff_name", ""),
                row.get("date", ""),
                row.get("slot_time", ""),
            )
            prev = _gs_prev_state.get(event_key)
            було  = prev if prev is not None else "—"
            стало = state_now
            _gs_prev_state[event_key] = state_now

            # Зміна: різниця зайнятих місць (тільки для групових)
            if etype == "group":
                try:
                    prev_rec = int(_gs_prev_state.get(event_key + ("rec",), row.get("records_count", 0)))
                    cur_rec  = int(row.get("records_count", 0))
                    delta = cur_rec - prev_rec
                    зміна = f"+{delta}" if delta > 0 else (str(delta) if delta < 0 else "0")
                    _gs_prev_state[event_key + ("rec",)] = cur_rec
                except (TypeError, ValueError):
                    зміна = ""
            else:
                зміна = "" if було == "—" or було == стало else ("↓ скасовано" if стало == "вільний" else "↑ зайнятий")

            # Якщо нічого не змінилось — не додаємо рядок
            if було != "—" and було == стало:
                return

        entry = [
            row.get("checked_at", ""),
            row.get("competitor", ""),
            etype,
            label,
            row.get("slot_time", ""),
            було,
            стало,
            зміна,
        ]
        _gs_status_rows.append(entry)
        if len(_gs_status_rows) > STATUS_MAX_ROWS:
            _gs_status_rows = _gs_status_rows[-STATUS_MAX_ROWS:]

        sh = gc.open_by_key(GOOGLE_SPREADSHEET_ID)
        ws = _ensure_worksheet(sh, GOOGLE_SHEET_STATUS)

        # Нові рядки зверху
        data = [STATUS_HEADERS] + list(reversed(_gs_status_rows))
        ws.clear()
        ws.update(values=data, value_input_option="USER_ENTERED")
    except Exception as e:
        print(f"  [GSheets] Помилка оновлення Статус: {e}")


def _collect_today_events():
    """Зібрати всі події на сьогодні по всіх конкурентах. Повертає list of dicts."""
    from collections import defaultdict
    today = date.today().isoformat()
    events = []

    for comp in COMPETITORS:
        name = comp["name"]
        cid  = comp["company_id"]
        base = comp["base_url"]
        tok  = comp["token"]

        # Групові
        ev = api_get(
            f"{base}/api/v1/activity/{cid}/search?count=100&from={today}&till={today}&page=1",
            tok
        )
        for e in (ev.get("data", []) if ev.get("success") else []):
            cap = e["capacity"]
            rec = e["records_count"]
            raw_time = e["date"][11:16]           # "09:00"
            norm_time = raw_time.lstrip("0") or "0:00"  # "9:00" — без ведучого нуля
            if norm_time.startswith(":"):         # "09:00" → lstrip → ":00" edge case
                norm_time = "0" + norm_time
            events.append({
                "дата":             today,
                "студія":           name,
                "час":              norm_time,
                "тип":              "group",
                "подія":            e.get("service", {}).get("title", ""),
                "тренер":           e.get("staff", {}).get("name", ""),
                "місць_всього":     cap,
                "клієнтів":         rec,
                "вільних":          cap - rec,
                "завантаженість_%": round(rec / cap * 100) if cap else 0,
            })

        # Індивідуальні
        staff = api_get(f"{base}/api/v1/book_staff/{cid}?without_seances=1", tok)
        if not isinstance(staff, list):
            staff = staff.get("data", [])
        for s in staff:
            d = api_get(f"{base}/api/v1/book_dates/{cid}?staff_id={s['id']}", tok)
            working = today in d.get("working_dates", [])
            booking = today in d.get("booking_dates", [])
            if not working:
                continue
            # Якщо є вільні слоти — дізнаємось точну кількість
            if booking:
                slots = api_get(
                    f"{base}/api/v1/book_times/{cid}/{s['id']}/{today}", tok
                )
                if not isinstance(slots, list):
                    slots = []
                free = len(slots)
                # Для індивідуальних ємність = 1 майстер весь день
                # Показуємо як: вільних слотів / загальних = зайнятість
                events.append({
                    "дата":             today,
                    "студія":           name,
                    "час":              "весь день",
                    "тип":              "individual",
                    "подія":            "Індивідуальна",
                    "тренер":           s["name"],
                    "місць_всього":     "",
                    "клієнтів":         "",
                    "вільних":          free,
                    "завантаженість_%": "",
                })
            else:
                events.append({
                    "дата":             today,
                    "студія":           name,
                    "час":              "весь день",
                    "тип":              "individual",
                    "подія":            "Індивідуальна",
                    "тренер":           s["name"],
                    "місць_всього":     "",
                    "клієнтів":         "",
                    "вільних":          0,
                    "завантаженість_%": 100,
                })

    return events


# ========================
# СКАНУВАННЯ ІНДИВІДУАЛЬНИХ НА 7 ДНІВ
# ========================

def _fetch_individual_week() -> dict:
    """Отримує поточну кількість вільних слотів для всіх тренерів на 7 днів вперед.
    Повертає {(студія, тренер, дата): free_count | None}.
    None = не працює, 0 = повністю зайнятий.
    """
    today = date.today()
    week_dates = [(today + timedelta(days=i)).isoformat() for i in range(7)]
    result = {}

    for comp in COMPETITORS:
        name = comp["name"]
        base = comp["base_url"]
        cid  = comp["company_id"]
        tok  = comp["token"]

        try:
            staff_list = api_get(f"{base}/api/v1/book_staff/{cid}?without_seances=1", tok)
            if not isinstance(staff_list, list):
                staff_list = staff_list.get("data", [])
        except Exception as e:
            print(f"  [Індив] {name}: помилка staff — {e}")
            continue

        for s in staff_list:
            trainer = s["name"]
            try:
                dates_data = api_get(
                    f"{base}/api/v1/book_dates/{cid}?staff_id={s['id']}", tok
                )
                working = set(dates_data.get("working_dates", []))
                booking = set(dates_data.get("booking_dates", []))
            except Exception:
                working, booking = set(), set()

            for d in week_dates:
                key = (name, trainer, d)
                if d not in working:
                    result[key] = None        # не працює
                elif d not in booking:
                    result[key] = 0           # повністю зайнятий
                else:
                    try:
                        slots = api_get(
                            f"{base}/api/v1/book_times/{cid}/{s['id']}/{d}", tok
                        )
                        result[key] = len(slots) if isinstance(slots, list) else 0
                    except Exception:
                        result[key] = None

    return result


def gs_update_individual_week():
    """Оновлює аркуш «Індивідуальні» для всіх тренерів на 7 днів вперед.

    Логіка:
    - Перший скан дня → записує рядок, initial = поточна кількість вільних
    - Наступні скани → оновлює вільних/зайнято, initial не змінюється
    - Рядки минулих днів залишаються незмінними
    """
    gc = get_gs_client()
    if not gc:
        return
    try:
        current = _fetch_individual_week()

        sh = gc.open_by_key(GOOGLE_SPREADSHEET_ID)
        ws = _ensure_worksheet(sh, GOOGLE_SHEET_INDIVIDUAL)
        all_rows = ws.get_all_values()

        if not all_rows:
            ws.append_row(IND_HEADERS, value_input_option="RAW")
            all_rows = [IND_HEADERS]

        # Індекс існуючих рядків: (дата, студія, тренер) -> (номер рядка, initial_slots)
        header = all_rows[0]
        try:
            ci = {c: header.index(c) for c in
                  ["дата", "студія", "тренер", "слотів_на_початку_тижня"]}
        except ValueError:
            ci = {"дата": 0, "студія": 2, "тренер": 3, "слотів_на_початку_тижня": 4}

        row_map = {}  # (дата, студія, тренер) -> (row_num, initial_val)
        for i, row in enumerate(all_rows[1:], start=2):
            if len(row) > ci["тренер"]:
                k = (row[ci["дата"]], row[ci["студія"]], row[ci["тренер"]])
                init_val = row[ci["слотів_на_початку_тижня"]] if ci["слотів_на_початку_тижня"] < len(row) else ""
                row_map[k] = (i, init_val)

        batch_updates = []   # [{range: "A5", values: [[...]]}]
        rows_to_append = []  # рядки для дозапису

        for (studio, trainer, d_str), free in current.items():
            d_obj = date.fromisoformat(d_str)
            dow   = DAY_UK[d_obj.weekday()]
            key   = (d_str, studio, trainer)
            existing = row_map.get(key)

            if free is None:
                initial, booked, occ, status = "", "", "", "не працює"
            else:
                if existing:
                    try:
                        initial = int(existing[1])
                    except (ValueError, TypeError):
                        initial = free
                    # Тренер додала нові слоти — оновлюємо базу
                    if isinstance(initial, int) and free > initial:
                        initial = free
                else:
                    initial = free  # перший скан — фіксуємо

                booked = initial - free if isinstance(initial, int) else ""
                if isinstance(initial, int) and initial > 0:
                    occ = round(booked / initial * 100)
                elif free == 0:
                    occ = 100
                else:
                    occ = 0

                if free == 0:
                    status = "повністю зайнятий"
                elif booked == 0:
                    status = "немає записів"
                else:
                    status = "є вільні слоти"

            row_data = [d_str, dow, studio, trainer, initial, free, booked, occ, status]

            if existing:
                batch_updates.append({
                    "range": f"A{existing[0]}",
                    "values": [row_data],
                })
            else:
                rows_to_append.append(row_data)
                row_map[key] = (len(all_rows) + len(rows_to_append), initial)

        # Один batch update замість N окремих запитів
        if batch_updates:
            ws.batch_update(batch_updates, value_input_option="RAW")
        if rows_to_append:
            ws.append_rows(rows_to_append, value_input_option="RAW")

        print(f"  [GSheets] Індивідуальні (7 днів): оновлено {len(batch_updates)}, додано {len(rows_to_append)} рядків")
        gs_update_status({
            "checked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "competitor": "— всі студії —",
            "event_type": "heartbeat",
            "staff_name": "",
            "event_name": f"щогодинний скан (оновлено {len(batch_updates)}, додано {len(rows_to_append)})",
            "slot_time": "",
            "records_count": "",
            "places_left": "",
            "status": "ок",
        })
    except Exception as e:
        print(f"  [GSheets] Помилка оновлення Індивідуальні: {e}")


DAY_UK = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"]


def _ensure_worksheet(sh, title: str, rows: int = 2000, cols: int = 20):
    """Повернути worksheet з назвою title, створивши якщо не існує."""
    existing = [ws.title for ws in sh.worksheets()]
    if title not in existing:
        sh.add_worksheet(title=title, rows=rows, cols=cols)
    return sh.worksheet(title)


IND_HEADERS = [
    "дата", "день_тижня", "студія", "тренер",
    "слотів_на_початку_тижня", "слотів_вільних", "слотів_зайнято",
    "завантаженість_%", "статус",
]
GRP_HEADERS = [
    "дата", "день_тижня", "студія", "час", "година",
    "подія", "тренер", "місць_всього", "клієнтів",
    "вільних", "завантаженість_%", "статус",
]


def _build_row_index(all_rows: list, key_cols: list) -> dict:
    """Побудувати словник (key_tuple -> номер рядка 1-based) по існуючих даних.
    Перший рядок вважається заголовком і пропускається."""
    if not all_rows:
        return {}
    header = all_rows[0]
    try:
        key_indices = [header.index(c) for c in key_cols]
    except ValueError:
        return {}
    index = {}
    for i, row in enumerate(all_rows[1:], start=2):
        key = tuple(_norm_key_val(row[j]) if j < len(row) else "" for j in key_indices)
        index[key] = i
    return index


def _norm_key_val(v: str) -> str:
    """Нормалізує значення для порівняння ключів.
    Всі варіанти часу '09:00', '9:00', '9:00:00', '9:00:00 AM' → '9:00'."""
    v = str(v).strip()
    # "9:00:00 AM" / "9:00:00 PM" → беремо тільки HH:MM
    if v.upper().endswith(("AM", "PM")):
        v = v.split()[0]
    # "9:00:00" → "9:00" (відкидаємо секунди)
    parts = v.split(":")
    if len(parts) == 3:
        v = ":".join(parts[:2])
    # "09:00" → "9:00" (прибираємо ведучий нуль години)
    if len(v) >= 4 and v[0] == "0" and v[1] != ":":
        v = v[1:]
    return v


def ws_dedup(ws, key_cols: list):
    """Видаляє дублікати з worksheet. Залишає перший рядок для кожного ключа."""
    all_rows = ws.get_all_values()
    if len(all_rows) < 2:
        return 0
    header = all_rows[0]
    try:
        key_indices = [header.index(c) for c in key_cols]
    except ValueError:
        return 0

    seen = set()
    rows_to_delete = []
    for i, row in enumerate(all_rows[1:], start=2):
        key = tuple(_norm_key_val(row[j]) if j < len(row) else "" for j in key_indices)
        if key in seen:
            rows_to_delete.append(i)
        else:
            seen.add(key)

    # Видаляємо знизу вгору щоб не зсувались номери рядків
    for row_num in sorted(rows_to_delete, reverse=True):
        ws.delete_rows(row_num)

    return len(rows_to_delete)


def _ws_upsert(ws, all_rows: list, headers: list, key_cols: list, new_rows: list):
    """Оновити існуючі рядки або дозаписати нові.
    new_rows — список списків, перший елемент відповідає headers[0] і т.д."""
    if not all_rows:
        ws.append_row(headers, value_input_option="RAW")
        all_rows = [headers]

    row_index = _build_row_index(all_rows, key_cols)
    key_col_positions = [headers.index(c) for c in key_cols]

    updated = 0
    appended = 0
    for row_data in new_rows:
        key = tuple(_norm_key_val(row_data[i]) for i in key_col_positions)
        if key in row_index:
            row_num = row_index[key]
            ws.update(range_name=f"A{row_num}", values=[row_data], value_input_option="RAW")
            updated += 1
        else:
            ws.append_row(row_data, value_input_option="RAW")
            appended += 1
    return updated, appended


def gs_update_individual():
    """Аліас для зворотної сумісності — викликає gs_update_individual_week()."""
    gs_update_individual_week()


def gs_update_group():
    """Аркуш «Групові»: один рядок = одна подія на один день.
    Оновлює існуючий рядок якщо є, інакше дозаписує — дані за попередні дні не зникають.
    """
    gc = get_gs_client()
    if not gc:
        return
    try:
        events = _collect_today_events()
        dow = DAY_UK[date.today().weekday()]

        new_rows = []
        for e in events:
            if e["тип"] != "group":
                continue
            try:
                hour = int(e["час"].split(":")[0])
            except (ValueError, AttributeError):
                hour = ""
            occ    = e["завантаженість_%"]
            status = "повний зал" if occ == 100 else "є місця"
            new_rows.append([
                e["дата"], dow, e["студія"], e["час"], hour,
                e["подія"], e["тренер"], e["місць_всього"], e["клієнтів"],
                e["вільних"], occ, status,
            ])

        sh  = gc.open_by_key(GOOGLE_SPREADSHEET_ID)
        ws  = _ensure_worksheet(sh, GOOGLE_SHEET_GROUP)
        all_rows = ws.get_all_values()
        upd, app = _ws_upsert(ws, all_rows, GRP_HEADERS,
                              ["дата", "студія", "час", "подія"], new_rows)
        print(f"  [GSheets] Групові: оновлено {upd}, додано {app} рядків")
    except Exception as e:
        print(f"  [GSheets] Помилка оновлення Групові: {e}")


def gs_update_table():
    """Зведена таблиця студії × години з числом клієнтів — для графіків."""
    gc = get_gs_client()
    if not gc:
        return
    try:
        from collections import defaultdict
        today = date.today().isoformat()
        events = _collect_today_events()

        # Збираємо унікальні години (тільки групові)
        all_hours = sorted({e["час"] for e in events
                            if e["тип"] == "group" and e["час"] != "весь день"})
        studios   = [c["name"] for c in COMPETITORS]

        # clients[studio][hour] = кількість клієнтів
        clients = defaultdict(lambda: defaultdict(int))
        for e in events:
            if e["тип"] == "group" and e["клієнтів"] != "":
                clients[e["студія"]][e["час"]] += e["клієнтів"]

        # Рядок 1: заголовки
        header = ["Студія", "Дата"] + all_hours + ["Інд. повністю зайнятих"]

        # Підрахунок індивідуальних 100%
        ind_full = defaultdict(int)
        for e in events:
            if e["тип"] == "individual" and e["завантаженість_%"] == 100:
                ind_full[e["студія"]] += 1

        rows = [header]
        for studio in studios:
            row = [studio, today]
            for h in all_hours:
                row.append(clients[studio].get(h, ""))
            row.append(ind_full.get(studio, 0))
            rows.append(row)

        sh = gc.open_by_key(GOOGLE_SPREADSHEET_ID)
        ws = sh.worksheet(GOOGLE_SHEET_TABLE)
        ws.clear()
        ws.update(values=rows, value_input_option="USER_ENTERED")
        print(f"  [GSheets] Таблицю оновлено ({len(rows)-1} студій, {len(all_hours)} годин)")
    except Exception as e:
        print(f"  [GSheets] Помилка оновлення таблиці: {e}")


# ========================
# ПЛАНУВАННЯ ПЕРЕВІРОК
# ========================

def schedule_check(delay_seconds: float, fn, *args):
    """Запустити fn(*args) через delay_seconds секунд у окремому потоці."""
    if delay_seconds < 0:
        return None

    def run():
        time.sleep(delay_seconds)
        try:
            fn(*args)
        except Exception as e:
            print(f"  [!] Помилка перевірки: {e}")

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return t


def seconds_until(time_str: str, date_str: str, minus_minutes: int = 0) -> float:
    """Скільки секунд до заданого часу (мінус N хвилин)."""
    dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
    dt -= timedelta(minutes=minus_minutes)
    return (dt - datetime.now()).total_seconds()


# ========================
# ПЕРЕВІРКИ
# ========================

def check_individual_slot(competitor, staff, slot_time: str, date_str: str):
    """Перевіряє чи індивідуальний слот ще вільний."""
    slots = get_individual_slots(
        competitor["base_url"], competitor["company_id"],
        staff["id"], date_str, competitor["token"]
    )
    free_times = {s["time"] for s in slots}
    is_free = slot_time in free_times

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status = "вільний" if is_free else "зайнятий"
    print(f"[{now_str}] INDIVIDUAL | {competitor['name']} | "
          f"{staff['name']} | {date_str} {slot_time} → {status}")

    save_row({
        "checked_at": now_str,
        "competitor": competitor["name"],
        "company_id": competitor["company_id"],
        "event_type": "individual",
        "event_name": "Індивідуальна послуга",
        "date": date_str,
        "slot_time": slot_time,
        "staff_name": staff["name"],
        "capacity": 1,
        "records_count": 0 if is_free else 1,
        "places_left": 1 if is_free else 0,
        "occupancy_pct": 0 if is_free else 100,
        "status": status,
    })


def check_group_event(competitor, event_id: int, event_name: str,
                      event_time: str, date_str: str, staff_name: str):
    """Перевіряє актуальну завантаженість групової події."""
    event = get_group_event_by_id(
        competitor["base_url"], competitor["company_id"],
        event_id, competitor["token"]
    )

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if not event:
        print(f"[{now_str}] GROUP | {competitor['name']} | "
              f"{event_name} {event_time} → подію не знайдено")
        return

    capacity = event["capacity"]
    records = event["records_count"]
    places_left = capacity - records
    occupancy_pct = round(records / capacity * 100) if capacity > 0 else 0

    print(f"[{now_str}] GROUP | {competitor['name']} | "
          f"{staff_name} | {event_name} {event_time} → "
          f"{records}/{capacity} зайнято ({occupancy_pct}%)")

    save_row({
        "checked_at": now_str,
        "competitor": competitor["name"],
        "company_id": competitor["company_id"],
        "event_type": "group",
        "event_id": event_id,
        "event_name": event_name,
        "date": date_str,
        "slot_time": event_time,
        "staff_name": staff_name,
        "capacity": capacity,
        "records_count": records,
        "places_left": places_left,
        "occupancy_pct": occupancy_pct,
        "status": "зайнятий" if places_left == 0 else "є місця",
    })


# ========================
# ЗБІР ПОДІЙ НА СЬОГОДНІ
# ========================

_scheduled_group_sheet_times      = set()  # часи, для яких вже заплановано gs_update_group
_scheduled_individual_sheet_times = set()  # часи, для яких вже заплановано gs_update_individual


def schedule_today(competitor) -> list:
    today = date.today().isoformat()
    name = competitor["name"]
    base_url = competitor["base_url"]
    company_id = competitor["company_id"]
    token = competitor["token"]
    threads = []

    print(f"\n{'='*55}")
    print(f"  {name}  (company_id={company_id})")
    print(f"{'='*55}")

    # ── Індивідуальні ──────────────────────────────────────
    print("\n[Індивідуальні послуги]")
    try:
        staff_list = get_staff(base_url, company_id, token)
    except Exception as e:
        print(f"  Помилка: {e}")
        staff_list = []

    individual_total = 0
    for staff in staff_list:
        try:
            dates_data = api_get(
                f"{base_url}/api/v1/book_dates/{company_id}?staff_id={staff['id']}", token
            )
            working_today = today in dates_data.get("working_dates", [])
            booking_today = today in dates_data.get("booking_dates", [])
        except Exception as e:
            print(f"  {staff['name']}: помилка розкладу — {e}")
            working_today, booking_today = False, False

        if not working_today:
            print(f"  {staff['name']}: не працює сьогодні")
            # Записуємо у CSV що майстер не працює
            save_row({
                "checked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "competitor": name,
                "company_id": company_id,
                "event_type": "individual",
                "event_name": "Індивідуальна послуга",
                "date": today,
                "slot_time": "",
                "staff_name": staff["name"],
                "capacity": 0,
                "records_count": 0,
                "places_left": 0,
                "occupancy_pct": "",
                "status": "не працює",
            })
            continue

        if not booking_today:
            print(f"  {staff['name']}: повністю зайнятий")
            # Записуємо 100% завантаженість
            save_row({
                "checked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "competitor": name,
                "company_id": company_id,
                "event_type": "individual",
                "event_name": "Індивідуальна послуга",
                "date": today,
                "slot_time": "",
                "staff_name": staff["name"],
                "capacity": 1,
                "records_count": 1,
                "places_left": 0,
                "occupancy_pct": 100,
                "status": "повністю зайнятий",
            })
            continue

        # Є вільні слоти — плануємо перевірку за 5 хв до кожного
        try:
            slots = get_individual_slots(base_url, company_id, staff["id"], today, token)
        except Exception as e:
            print(f"  {staff['name']}: помилка слотів — {e}")
            continue

        print(f"  {staff['name']}: {len(slots)} вільних слотів — "
              + ", ".join(s["time"] for s in slots))

        for slot in slots:
            # Зберігаємо початковий стан для аркушу Статус
            _gs_prev_state[(name, "individual", "", staff["name"], today, slot["time"])] = "вільний"

            delay = seconds_until(slot["time"], today, CHECK_BEFORE_MINUTES)
            if delay < 0:
                print(f"    Пропуск {slot['time']} — вже минув")
                continue
            check_dt = datetime.now() + timedelta(seconds=delay)
            print(f"    Перевірка {slot['time']} о {check_dt.strftime('%H:%M')} "
                  f"(через {int(delay//60)}хв {int(delay%60)}с)")
            t = schedule_check(delay, check_individual_slot,
                               competitor, staff, slot["time"], today)
            if t:
                threads.append(t)
                individual_total += 1
            # Одне оновлення аркушу на унікальний час (не на кожного тренера)
            if slot["time"] not in _scheduled_individual_sheet_times:
                _scheduled_individual_sheet_times.add(slot["time"])
                t2 = schedule_check(delay, gs_update_individual)
                if t2:
                    threads.append(t2)

    if individual_total == 0 and all(True for _ in staff_list):
        print("  Всі майстри або не працюють, або повністю зайняті")

    # ── Групові ────────────────────────────────────────────
    print("\n[Групові події]")
    try:
        events = get_group_events(base_url, company_id, today, token)
    except Exception as e:
        print(f"  Помилка: {e}")
        events = []

    if not events:
        print("  Немає групових подій сьогодні")
    else:
        for event in events:
            dt_str = event["date"]                        # "2026-04-09 19:00:00"
            event_time = dt_str[11:16]                   # "19:00"
            event_name = event.get("service", {}).get("title", f"ID={event['id']}")
            staff_name = event.get("staff", {}).get("name", "?")
            capacity = event["capacity"]
            records = event["records_count"]
            places_left = capacity - records
            occupancy_pct = round(records / capacity * 100) if capacity > 0 else 0

            print(f"  {event_time} | {event_name} | {staff_name} | "
                  f"{records}/{capacity} зайнято ({occupancy_pct}%) | "
                  f"вільних: {places_left}")

            # Зберігаємо початковий стан для аркушу Статус
            _ev_key = (name, "group", str(event["id"]), staff_name, today, event_time)
            _gs_prev_state[_ev_key] = f"{records}/{capacity} ({occupancy_pct}%)"
            _gs_prev_state[_ev_key + ("rec",)] = records

            delay = seconds_until(event_time, today, CHECK_BEFORE_MINUTES)
            if delay < 0:
                print(f"    Пропуск — вже минув")
                continue

            check_dt = datetime.now() + timedelta(seconds=delay)
            print(f"    Перевірка о {check_dt.strftime('%H:%M')} "
                  f"(через {int(delay//60)}хв {int(delay%60)}с)")

            t = schedule_check(delay, check_group_event,
                               competitor, event["id"], event_name,
                               event_time, today, staff_name)
            if t:
                threads.append(t)
            # Одне оновлення аркушу на унікальний час (не на кожну подію)
            if event_time not in _scheduled_group_sheet_times:
                _scheduled_group_sheet_times.add(event_time)
                t2 = schedule_check(delay, gs_update_group)
                if t2:
                    threads.append(t2)

    return threads


# ========================
# ГОЛОВНИЙ ЗАПУСК
# ========================

def run_today():
    print(f"Моніторинг на {date.today().isoformat()}")
    print(f"Перевірка за {CHECK_BEFORE_MINUTES} хв до кожної події\n")

    all_threads = []
    for competitor in COMPETITORS:
        try:
            threads = schedule_today(competitor)
            all_threads.extend(threads)
        except Exception as e:
            print(f"Критична помилка для {competitor['name']}: {e}")

    if not all_threads:
        print("\nНемає подій для моніторингу сьогодні.")
        return

    print(f"\n{'='*55}")
    print(f"Заплановано {len(all_threads)} перевірок. Очікую...")
    print(f"Дані зберігаються у: {DATA_FILE}")
    if GOOGLE_SPREADSHEET_ID:
        print(f"Google Sheets: увімкнено")
        gs_init_log()           # підготувати аркуш Log
        gs_update_individual()  # аркуш Індивідуальні
        gs_update_group()       # аркуш Групові
        gs_update_table()       # зведена таблиця студії × години
    print("Ctrl+C для виходу\n")

    try:
        for t in all_threads:
            t.join()
        print("\nВсі перевірки виконано.")
    except KeyboardInterrupt:
        print("\nЗупинено.")


if __name__ == "__main__":
    if not load_config():
        setup()
    run_today()
