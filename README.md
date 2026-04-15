# Pilates Analytics — Моніторинг конкурентів

Автоматичний збір даних про завантаженість конкурентів через Altegio API з записом у Google Sheets.

## Встановлення

### 1. Клонувати репозиторій

```bash
git clone https://github.com/doehjohn624-ship-it/pilates_analitycs.git
cd pilates_analitycs
```

### 2. Встановити залежності

```bash
pip3 install -r requirements.txt
```

### 3. Підготувати Google credentials

Перед першим запуском потрібен файл сервісного акаунту Google:

1. Зайдіть на [console.cloud.google.com](https://console.cloud.google.com)
2. Створіть проєкт → увімкніть **Google Sheets API**
3. **IAM → Сервісні акаунти → Створити** → завантажте JSON
4. Покладіть файл поруч зі скриптом під назвою `google_credentials.json`
5. Відкрийте вашу Google Таблицю та надайте доступ (редактор) email сервісного акаунту

### 4. Запустити та пройти налаштування

```bash
python3 competitor_occupancy.py
```

При першому запуску майстер попросить:
- **Крок 1** — посилання на Google Таблицю
- **Крок 2** — шлях до файлу `google_credentials.json`
- **Крок 3** — список студій для моніторингу (можна додати / видалити)
- **Крок 4** — налаштувати автозапуск (cron)

## Автозапуск (cron)

Якщо не налаштували під час майстра, додайте вручну:

```bash
crontab -e
```

```
0 6 * * * cd /шлях/до/папки && python3 competitor_occupancy.py >> occupancy.log 2>&1
0 * * * * cd /шлях/до/папки && python3 -c "import competitor_occupancy as m; m.load_config(); m.gs_update_individual_week()" >> occupancy.log 2>&1
```

## Файли

| Файл | Опис |
|------|------|
| `competitor_occupancy.py` | Головний скрипт |
| `config.json` | Налаштування (створюється автоматично) |
| `google_credentials.json` | Ключ сервісного акаунту Google (**не в git**) |
| `individual_baseline.json` | Базова лінія індивідуальних записів |
| `occupancy.log` | Лог виконання |
