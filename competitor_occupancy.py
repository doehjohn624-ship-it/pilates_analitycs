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
DATA_FILE = "occupancy_data.csv"

# ── Google Sheets (заповніть після налаштування) ──────────
GOOGLE_CREDENTIALS_FILE = "/root/google_credentials.json"  # шлях до JSON ключа
GOOGLE_SPREADSHEET_ID   = "17kdfCu_ewPJPJBFdpULm_UIcfAKHw0I5PbZR3qoj21Y"
GOOGLE_SHEET_LOG        = "Log"            # технічний лог всіх перевірок
GOOGLE_SHEET_INDIVIDUAL = "Індивідуальні"  # дані по індивідуальних тренуваннях
GOOGLE_SHEET_GROUP      = "Групові"        # дані по групових тренуваннях
GOOGLE_SHEET_TABLE      = "Таблиця"        # зведена таблиця студії × години (клієнтів)
# ─────────────────────────────────────────────────────────

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
        _gs_log_ws = sh.worksheet(GOOGLE_SHEET_LOG)
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
            events.append({
                "дата":             today,
                "студія":           name,
                "час":              e["date"][11:16],
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


DAY_UK = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"]


def _ensure_worksheet(sh, title: str, rows: int = 2000, cols: int = 20):
    """Повернути worksheet з назвою title, створивши якщо не існує."""
    existing = [ws.title for ws in sh.worksheets()]
    if title not in existing:
        sh.add_worksheet(title=title, rows=rows, cols=cols)
    return sh.worksheet(title)


def gs_update_individual():
    """Аркуш «Індивідуальні»: один рядок = один тренер на сьогодні.

    Колонки (аналітика-дружні):
        дата | день_тижня | студія | тренер | статус
        вільних_слотів | завантаженість_%
    """
    gc = get_gs_client()
    if not gc:
        return
    try:
        events = _collect_today_events()
        today_date = date.today()
        dow = DAY_UK[today_date.weekday()]

        HEADERS = [
            "дата", "день_тижня", "студія", "тренер",
            "статус", "вільних_слотів", "завантаженість_%",
        ]
        rows = [HEADERS]
        for e in events:
            if e["тип"] != "individual":
                continue
            free = e["вільних"] if e["вільних"] != "" else 0
            occ  = e["завантаженість_%"]
            if occ == "":
                occ = 0

            if e["місць_всього"] == "" and free == 0 and occ == 0:
                status = "не працює"
            elif occ == 100:
                status = "повністю зайнятий"
            else:
                status = "є вільні слоти"

            rows.append([
                e["дата"], dow, e["студія"], e["тренер"],
                status, free, occ,
            ])

        sh = gc.open_by_key(GOOGLE_SPREADSHEET_ID)
        ws = _ensure_worksheet(sh, GOOGLE_SHEET_INDIVIDUAL)
        ws.clear()
        ws.update(rows, value_input_option="USER_ENTERED")
        print(f"  [GSheets] Індивідуальні оновлено ({len(rows)-1} рядків)")
    except Exception as e:
        print(f"  [GSheets] Помилка оновлення Індивідуальні: {e}")


def gs_update_group():
    """Аркуш «Групові»: один рядок = одна групова подія.

    Колонки (аналітика-дружні):
        дата | день_тижня | студія | час | година
        подія | тренер | місць_всього | клієнтів
        вільних | завантаженість_% | статус
    """
    gc = get_gs_client()
    if not gc:
        return
    try:
        events = _collect_today_events()
        today_date = date.today()
        dow = DAY_UK[today_date.weekday()]

        HEADERS = [
            "дата", "день_тижня", "студія", "час", "година",
            "подія", "тренер", "місць_всього", "клієнтів",
            "вільних", "завантаженість_%", "статус",
        ]
        rows = [HEADERS]
        for e in events:
            if e["тип"] != "group":
                continue
            # Числова година для угрупування/сортування
            try:
                hour = int(e["час"].split(":")[0])
            except (ValueError, AttributeError):
                hour = ""

            cap = e["місць_всього"]
            rec = e["клієнтів"]
            occ = e["завантаженість_%"]
            status = "повний зал" if occ == 100 else "є місця"

            rows.append([
                e["дата"], dow, e["студія"], e["час"], hour,
                e["подія"], e["тренер"], cap, rec,
                e["вільних"], occ, status,
            ])

        sh = gc.open_by_key(GOOGLE_SPREADSHEET_ID)
        ws = _ensure_worksheet(sh, GOOGLE_SHEET_GROUP)
        ws.clear()
        ws.update(rows, value_input_option="USER_ENTERED")
        print(f"  [GSheets] Групові оновлено ({len(rows)-1} рядків)")
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
        ws.update(rows, value_input_option="USER_ENTERED")
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
    run_today()
