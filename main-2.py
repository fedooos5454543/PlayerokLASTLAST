"""
Telegram-бот для продавца на Playerok. Всё в одном файле.

Возможности:
  1. Уведомление в Telegram о каждой продаже (товар, цена, покупатель).
  2. Пересылка в Telegram входящих сообщений от покупателей.
  3. Уведомление о новых отзывах.
  4. Авто-выставление проданного товара заново (AUTO_RELIST=true в .env).
  5. Telegram-команды (пишутся боту в личку):
       /help                      - список команд
       /items                     - список ваших активных товаров и цен
       /chats                     - список активных чатов с покупателями
       /reply <N> <текст>         - ответить в чат №N (номер из /chats) прямо с Playerok
       /report day|week|month     - отчёт по продажам за период
       /peak                      - в какие часы чаще всего покупают
       /autopublish_time HH:MM    - выставлять черновики товаров на продажу каждый день в это время
       /autopublish_time off      - выключить авто-публикацию по расписанию
       /autopublish_now           - опубликовать все черновики прямо сейчас

Запуск:
    pip install -r requirements.txt
    cp .env.example .env   # и заполнить своими данными
    python main.py

ВАЖНО: используется неофициальная библиотека PlayerokAPI (GraphQL/REST-запросы
от имени вашего залогиненного аккаунта через cookies/токен сессии). Часть полей
в ответах API не задокументирована до последней детали (особенно для чатов и
отзывов) — такие места помечены комментариями "best-effort" и обёрнуты в
защитные проверки. Если какая-то команда упадёт с ошибкой — пришлите точный
текст ошибки, это почти всегда лечится правкой одного названия поля.
"""

import logging
import os
import sqlite3
import tempfile
import threading
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
import requests

from playerokapi.account import Account
from playerokapi.enums import EventTypes, ItemStatuses
from playerokapi.listener.listener import EventListener
from playerokapi.exceptions import BotCheckDetectedException, UnauthorizedError


# =========================================================================
#  НАСТРОЙКА / ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ
# =========================================================================

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("main")

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
PLAYEROK_COOKIES = os.getenv("PLAYEROK_COOKIES")
PLAYEROK_USER_AGENT = os.getenv("PLAYEROK_USER_AGENT")
AUTO_RELIST = os.getenv("AUTO_RELIST", "false").strip().lower() in ("1", "true", "yes")
RELIST_PRIORITY_STATUS_ID = os.getenv("RELIST_PRIORITY_STATUS_ID") or None

# Смещение часового пояса (в часах) относительно UTC — используется только
# для красивого отображения "часа" в отчётах/пиках. По умолчанию МСК (+3).
TZ_OFFSET_HOURS = int(os.getenv("TZ_OFFSET_HOURS", "3"))

# Bothost и некоторые другие хостинги дают отдельную постоянную папку для
# данных (переживает пересборки) через переменную DATA_DIR. Если её нет —
# используем текущую директорию (для локального запуска).
DATA_DIR = os.getenv("DATA_DIR", ".")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "bot_data.db")

REQUIRED_VARS = {
    "TG_BOT_TOKEN": TG_BOT_TOKEN,
    "TG_CHAT_ID": TG_CHAT_ID,
    "PLAYEROK_COOKIES": PLAYEROK_COOKIES,
    "PLAYEROK_USER_AGENT": PLAYEROK_USER_AGENT,
}

# Лок на все запросы к Playerok API, которые могут выполняться параллельно
# из разных потоков (основной цикл слушателя событий + поток Telegram-команд).
# Неофициальная библиотека не гарантирует потокобезопасность своего HTTP-клиента,
# поэтому подстраховываемся.
api_lock = threading.Lock()


def check_env():
    missing = [k for k, v in REQUIRED_VARS.items() if not v]
    if missing:
        raise SystemExit(
            "Не заполнены переменные окружения: "
            + ", ".join(missing)
            + "\nСкопируйте .env.example в .env и заполните значения."
        )


# =========================================================================
#  БАЗА ДАННЫХ (sqlite, локальный файл) — история продаж, отзывы, настройки
# =========================================================================

def db():
    """Открывает новое соединение с БД (по одному на вызов — просто и потокобезопасно)."""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sales (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                deal_id TEXT UNIQUE,
                item_name TEXT,
                price INTEGER,
                buyer TEXT,
                created_at TEXT
            )
        """)
        conn.execute("CREATE TABLE IF NOT EXISTS seen_reviews (review_id TEXT PRIMARY KEY)")
        conn.execute("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS chat_index (
                idx INTEGER PRIMARY KEY,
                chat_id TEXT,
                label TEXT
            )
        """)
        conn.commit()


def record_sale(deal_id: str, item_name: str, price, buyer: str):
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        with db() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO sales (deal_id, item_name, price, buyer, created_at) VALUES (?, ?, ?, ?, ?)",
                (deal_id, item_name, int(price) if price is not None else None, buyer, now_iso),
            )
            conn.commit()
    except Exception:
        logger.exception("Не удалось записать продажу в БД")


def get_sales_since(cutoff_iso: str):
    with db() as conn:
        rows = conn.execute(
            "SELECT price, created_at FROM sales WHERE created_at >= ? ORDER BY created_at",
            (cutoff_iso,),
        ).fetchall()
    return rows


def is_review_seen(review_id: str) -> bool:
    with db() as conn:
        row = conn.execute("SELECT 1 FROM seen_reviews WHERE review_id = ?", (review_id,)).fetchone()
    return row is not None


def mark_review_seen(review_id: str):
    with db() as conn:
        conn.execute("INSERT OR IGNORE INTO seen_reviews (review_id) VALUES (?)", (review_id,))
        conn.commit()


def kv_get(key: str, default=None):
    with db() as conn:
        row = conn.execute("SELECT value FROM kv WHERE key = ?", (key,)).fetchone()
    return row[0] if row else default


def kv_set(key: str, value: str):
    with db() as conn:
        conn.execute("INSERT INTO kv (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
        conn.commit()


def save_chat_index(pairs):
    """pairs: список (chat_id, label) — сохраняется под номерами 1..N для команды /reply."""
    with db() as conn:
        conn.execute("DELETE FROM chat_index")
        conn.executemany(
            "INSERT INTO chat_index (idx, chat_id, label) VALUES (?, ?, ?)",
            [(i + 1, chat_id, label) for i, (chat_id, label) in enumerate(pairs)],
        )
        conn.commit()


def get_chat_id_by_index(n: int):
    with db() as conn:
        row = conn.execute("SELECT chat_id FROM chat_index WHERE idx = ?", (n,)).fetchone()
    return row[0] if row else None


# =========================================================================
#  TELEGRAM: отправка сообщений
# =========================================================================

class TelegramNotifier:
    """Простой клиент для Telegram Bot API (только requests, без сторонних либ)."""

    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.api_url = f"https://api.telegram.org/bot{bot_token}"

    def send(self, text: str, disable_preview: bool = True) -> bool:
        try:
            resp = requests.post(
                f"{self.api_url}/sendMessage",
                json={
                    "chat_id": self.chat_id,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": disable_preview,
                },
                timeout=15,
            )
            data = resp.json()
            if not data.get("ok"):
                logger.error("Telegram API вернул ошибку: %s", data)
                return False
            return True
        except Exception:
            logger.exception("Не удалось отправить сообщение в Telegram")
            return False

    def get_updates(self, offset=None, timeout=20):
        try:
            params = {"timeout": timeout}
            if offset is not None:
                params["offset"] = offset
            resp = requests.get(f"{self.api_url}/getUpdates", params=params, timeout=timeout + 10)
            data = resp.json()
            if not data.get("ok"):
                logger.error("getUpdates вернул ошибку: %s", data)
                return []
            return data.get("result", [])
        except Exception:
            logger.exception("Не удалось получить обновления Telegram")
            return []


# =========================================================================
#  АВТО-ВЫСТАВЛЕНИЕ ПРОДАННОГО ТОВАРА ЗАНОВО (сразу после продажи)
# =========================================================================

def _download_attachments(acc, attachments) -> list:
    paths = []
    for att in attachments or []:
        try:
            file_bytes = acc.download_file(att.url)
            suffix = os.path.splitext(att.filename or "")[1] or ".jpg"
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
            tmp.write(file_bytes)
            tmp.close()
            paths.append(tmp.name)
        except Exception:
            logger.exception("Не удалось скачать вложение %s", getattr(att, "url", "?"))
    return paths


def relist_sold_item(acc, sold_item_id: str, priority_status_id: str | None = None):
    """Пересоздаёт и публикует копию проданного товара."""
    original = acc.get_item(id=sold_item_id)

    attachment_paths = _download_attachments(acc, getattr(original, "attachments", None))
    data_fields = getattr(original, "data_fields", None) or []
    options = getattr(original, "attributes", None) or {}

    new_item = acc.create_item(
        game_category_id=original.category.id,
        obtaining_type_id=original.obtaining_type.id if original.obtaining_type else None,
        name=original.name,
        price=original.price,
        description=original.description,
        options=options,
        data_fields=data_fields,
        attachments=attachment_paths,
    )

    for p in attachment_paths:
        try:
            os.remove(p)
        except OSError:
            pass

    statuses = acc.get_item_priority_statuses(new_item.id, new_item.price)
    if priority_status_id:
        status = next((s for s in statuses if s.id == priority_status_id), None)
    else:
        status = next((s for s in statuses if s.price == 0), None)
    if status is None and statuses:
        status = statuses[0]
    if status is None:
        raise RuntimeError("Не удалось найти статус приоритета для публикации нового товара")

    return acc.publish_item(new_item.id, status.id)


# =========================================================================
#  АВТО-ПУБЛИКАЦИЯ ЧЕРНОВИКОВ ПО РАСПИСАНИЮ
# =========================================================================

def publish_all_drafts(acc, tg: TelegramNotifier):
    """Публикует все товары-черновики (ItemStatuses.DRAFT) под бесплатным статусом приоритета."""
    published, failed = [], []
    after_cursor = None
    seen_pages = 0

    with api_lock:
        while seen_pages < 10:
            seen_pages += 1
            page = acc.get_my_items(statuses=[ItemStatuses.DRAFT], count=24, after_cursor=after_cursor)
            items = getattr(page, "items", None)
            if items is None:
                items = getattr(page, "profiles", [])  # best-effort запасное имя поля

            for item in items:
                try:
                    statuses = acc.get_item_priority_statuses(item.id, item.price)
                    status = next((s for s in statuses if s.price == 0), None) or (statuses[0] if statuses else None)
                    if status is None:
                        failed.append((item.name, "нет доступного статуса приоритета"))
                        continue
                    acc.publish_item(item.id, status.id)
                    published.append(item.name)
                except Exception as e:
                    failed.append((getattr(item, "name", item.id), str(e)))

            page_info = getattr(page, "page_info", None)
            if not page_info or not getattr(page_info, "has_next_page", False):
                break
            after_cursor = page_info.end_cursor

    lines = ["♻️ <b>Авто-публикация черновиков</b>"]
    if published:
        lines.append(f"✅ Опубликовано ({len(published)}): " + ", ".join(published[:20]))
    if failed:
        lines.append(f"⚠️ Не удалось ({len(failed)}): " + ", ".join(f"{n} — {err}" for n, err in failed[:10]))
    if not published and not failed:
        lines.append("Черновиков не найдено — публиковать нечего.")
    tg.send("\n".join(lines))


def maybe_run_scheduled_autopublish(acc, tg: TelegramNotifier):
    """Проверяет, не пора ли запустить ежедневную авто-публикацию черновиков."""
    autopublish_time = kv_get("autopublish_time")  # формат "HH:MM" или None/"" если выключено
    if not autopublish_time:
        return

    now = datetime.now(timezone.utc) + timedelta(hours=TZ_OFFSET_HOURS)
    today_str = now.strftime("%Y-%m-%d")
    current_hm = now.strftime("%H:%M")

    if current_hm != autopublish_time:
        return
    if kv_get("autopublish_last_run_date") == today_str:
        return  # уже запускали сегодня

    kv_set("autopublish_last_run_date", today_str)
    logger.info("Запуск авто-публикации черновиков по расписанию (%s)", autopublish_time)
    try:
        publish_all_drafts(acc, tg)
    except Exception:
        logger.exception("Ошибка авто-публикации по расписанию")
        tg.send("⚠️ Не удалось выполнить авто-публикацию черновиков по расписанию. Проверьте логи.")


# =========================================================================
#  ОБРАБОТЧИКИ СОБЫТИЙ PLAYEROK (продажи / сообщения / отзывы)
# =========================================================================

def format_price(value) -> str:
    try:
        return f"{int(value):,}".replace(",", " ") + " ₽"
    except (TypeError, ValueError):
        return str(value)


def handle_sale(acc, tg: TelegramNotifier, deal, seen_deal_ids: set):
    if deal.id in seen_deal_ids:
        return
    seen_deal_ids.add(deal.id)

    item = deal.item
    buyer = deal.user

    item_name = getattr(item, "name", "неизвестный товар")
    item_price = getattr(item, "price", None)
    buyer_name = getattr(buyer, "username", "неизвестный покупатель")

    record_sale(deal.id, item_name, item_price, buyer_name)

    text = (
        "🛒 <b>Новая продажа!</b>\n\n"
        f"📦 Товар: <b>{item_name}</b>\n"
        f"💰 Цена: {format_price(item_price)}\n"
        f"👤 Покупатель: {buyer_name}\n"
        f"🆔 Сделка: <code>{deal.id}</code>"
    )
    tg.send(text)
    logger.info("Продажа: %s купил(а) '%s' за %s", buyer_name, item_name, format_price(item_price))

    if AUTO_RELIST and item is not None:
        try:
            with api_lock:
                new_item = relist_sold_item(acc, item.id, RELIST_PRIORITY_STATUS_ID)
            tg.send(
                "♻️ Товар автоматически выставлен на продажу заново:\n"
                f"<b>{new_item.name}</b> — {format_price(new_item.price)}\n"
                "⚠️ Рекомендую проверить карточку вручную (опции/фото могли скопироваться не полностью)."
            )
        except Exception:
            logger.exception("Не удалось авто-выставить товар заново (item_id=%s)", item.id)
            tg.send(f"⚠️ Не получилось автоматически перевыставить проданный товар «{item_name}».")


def handle_new_message(acc, tg: TelegramNotifier, event):
    message = event.message
    if message.user.id == acc.id:
        return
    sender = getattr(message.user, "username", "Собеседник")
    text = message.text or "[вложение/изображение]"
    tg.send(f"💬 <b>{sender}</b>:\n{text}")
    logger.info("Новое сообщение от %s: %s", sender, text[:80])


def handle_new_review(tg: TelegramNotifier, event):
    """event.deal.review — best-effort: точные названия полей Review не задокументированы."""
    deal = event.deal
    review = getattr(deal, "review", None)
    if review is None:
        return

    review_id = getattr(review, "id", None) or f"deal:{deal.id}"
    if is_review_seen(review_id):
        return
    mark_review_seen(review_id)

    rating = getattr(review, "rating", None)
    text = getattr(review, "text", None) or getattr(review, "comment", None) or ""
    item_name = getattr(deal.item, "name", "товар") if deal.item else "товар"
    buyer_name = getattr(deal.user, "username", "покупатель")
    stars = "⭐" * rating if isinstance(rating, int) and rating > 0 else "—"

    lines = [
        "🌟 <b>Новый отзыв!</b>",
        f"📦 Товар: {item_name}",
        f"👤 От: {buyer_name}",
        f"Оценка: {stars}",
    ]
    if text:
        lines.append(f"💬 {text}")
    tg.send("\n".join(lines))


# =========================================================================
#  TELEGRAM-КОМАНДЫ
# =========================================================================

HELP_TEXT = (
    "<b>Доступные команды</b>\n\n"
    "/items — список ваших активных товаров и цен\n"
    "/chats — список активных чатов с покупателями\n"
    "/reply &lt;N&gt; &lt;текст&gt; — ответить в чат №N (номер из /chats)\n"
    "/report day|week|month — отчёт по продажам за период\n"
    "/peak — в какие часы чаще всего покупают\n"
    "/autopublish_time HH:MM — публиковать черновики каждый день в это время\n"
    "/autopublish_time off — выключить авто-публикацию по расписанию\n"
    "/autopublish_now — опубликовать все черновики прямо сейчас\n"
    "/help — это сообщение"
)


def cmd_items(acc, tg: TelegramNotifier):
    with api_lock:
        page = acc.get_my_items(statuses=[ItemStatuses.APPROVED], count=24)
    items = getattr(page, "items", None) or getattr(page, "profiles", [])
    total = getattr(page, "total_count", len(items))

    if not items:
        tg.send("Активных товаров на продаже не найдено.")
        return

    lines = [f"📦 <b>Ваши товары в продаже</b> (показано {len(items)} из {total})\n"]
    for i, item in enumerate(items, 1):
        name = getattr(item, "name", "?")
        price = format_price(getattr(item, "price", "?"))
        lines.append(f"{i}. {name} — {price}")
    tg.send("\n".join(lines))


def cmd_chats(acc, tg: TelegramNotifier):
    with api_lock:
        page = acc.get_chats(count=24)
    chats = getattr(page, "chats", None) or getattr(page, "items", [])

    if not chats:
        tg.send("Активных чатов не найдено.")
        return

    pairs = []
    lines = ["💬 <b>Ваши чаты</b>\n"]
    for i, chat in enumerate(chats, 1):
        interlocutor = getattr(chat, "user", None) or getattr(chat, "participant", None) or getattr(chat, "interlocutor", None)
        label = getattr(interlocutor, "username", None) if interlocutor else None
        label = label or f"чат {chat.id[:8]}"
        pairs.append((chat.id, label))
        lines.append(f"{i}. {label}")

    save_chat_index(pairs)
    lines.append("\nОтветить: <code>/reply НОМЕР текст</code>")
    tg.send("\n".join(lines))


def cmd_reply(acc, tg: TelegramNotifier, args: str):
    parts = args.strip().split(maxsplit=1)
    if len(parts) < 2 or not parts[0].isdigit():
        tg.send("Использование: /reply НОМЕР текст\nСначала вызовите /chats, чтобы узнать номера.")
        return

    n = int(parts[0])
    text = parts[1]
    chat_id = get_chat_id_by_index(n)
    if not chat_id:
        tg.send(f"Чат №{n} не найден. Сначала вызовите /chats.")
        return

    try:
        with api_lock:
            acc.send_message(chat_id, text=text, mark_chat_as_read=True)
        tg.send(f"✅ Отправлено в чат №{n}.")
    except Exception as e:
        logger.exception("Не удалось отправить сообщение в чат %s", chat_id)
        tg.send(f"⚠️ Не удалось отправить сообщение: {e}")


def cmd_report(tg: TelegramNotifier, args: str):
    period = (args or "day").strip().lower()
    days_map = {"day": 1, "week": 7, "month": 30}
    days = days_map.get(period)
    if days is None:
        tg.send("Использование: /report day | /report week | /report month")
        return

    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    rows = get_sales_since(cutoff)

    count = len(rows)
    total = sum(r[0] for r in rows if r[0] is not None)
    period_ru = {"day": "последние 24 часа", "week": "последние 7 дней", "month": "последние 30 дней"}[period]

    lines = [
        f"📊 <b>Отчёт за {period_ru}</b>",
        f"Продаж: {count}",
        f"Выручка: {format_price(total)}",
    ]
    if count:
        lines.append(f"Средний чек: {format_price(total // count)}")
    tg.send("\n".join(lines))


def cmd_peak(tg: TelegramNotifier):
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    rows = get_sales_since(cutoff)

    if not rows:
        tg.send("Пока недостаточно данных о продажах для анализа (нужна хотя бы одна продажа после установки бота).")
        return

    hour_counter = Counter()
    for _, created_at in rows:
        dt = datetime.fromisoformat(created_at) + timedelta(hours=TZ_OFFSET_HOURS)
        hour_counter[dt.hour] += 1

    top = hour_counter.most_common(3)
    lines = ["⏰ <b>Часы пиковых продаж</b> (за последние 30 дней)\n"]
    for hour, cnt in top:
        lines.append(f"{hour:02d}:00–{(hour + 1) % 24:02d}:00 — {cnt} продаж(и)")
    tg.send("\n".join(lines))


def cmd_autopublish_time(tg: TelegramNotifier, args: str):
    value = (args or "").strip()
    if value.lower() == "off":
        kv_set("autopublish_time", "")
        tg.send("Авто-публикация по расписанию выключена.")
        return

    try:
        datetime.strptime(value, "%H:%M")
    except ValueError:
        tg.send("Использование: /autopublish_time HH:MM (например, /autopublish_time 10:00)\nИли: /autopublish_time off")
        return

    kv_set("autopublish_time", value)
    kv_set("autopublish_last_run_date", "")  # сбрасываем, чтобы новое время сразу учитывалось
    tg.send(f"✅ Черновики будут автоматически публиковаться каждый день в {value} (часовой пояс UTC+{TZ_OFFSET_HOURS}).")


def dispatch_command(acc, tg: TelegramNotifier, text: str):
    text = text.strip()
    if not text.startswith("/"):
        return

    parts = text.split(maxsplit=1)
    command = parts[0].lower().split("@")[0]  # игнорируем @botusername, если есть
    args = parts[1] if len(parts) > 1 else ""

    try:
        if command == "/help" or command == "/start":
            tg.send(HELP_TEXT)
        elif command == "/items":
            cmd_items(acc, tg)
        elif command == "/chats":
            cmd_chats(acc, tg)
        elif command == "/reply":
            cmd_reply(acc, tg, args)
        elif command == "/report":
            cmd_report(tg, args)
        elif command == "/peak":
            cmd_peak(tg)
        elif command == "/autopublish_time":
            cmd_autopublish_time(tg, args)
        elif command == "/autopublish_now":
            with api_lock:
                publish_all_drafts(acc, tg)
        else:
            tg.send("Неизвестная команда. /help — список команд.")
    except Exception as e:
        logger.exception("Ошибка при обработке команды %s", command)
        tg.send(f"⚠️ Ошибка при выполнении команды: {e}\nЕсли повторяется — пришлите этот текст разработчику бота.")


def telegram_and_scheduler_loop(acc, tg: TelegramNotifier, stop_event: threading.Event):
    """Фоновый поток: принимает Telegram-команды и раз в минуту проверяет расписание авто-публикации."""
    updates = tg.get_updates(timeout=1)
    offset = (max(u["update_id"] for u in updates) + 1) if updates else None
    logger.info("Telegram-команды: пропускаем %d старых обновлений, начинаем с offset=%s", len(updates), offset)

    last_schedule_check = 0
    while not stop_event.is_set():
        try:
            updates = tg.get_updates(offset=offset, timeout=20)
            for update in updates:
                offset = update["update_id"] + 1
                message = update.get("message") or update.get("edited_message")
                if not message:
                    continue
                chat_id = str(message.get("chat", {}).get("id", ""))
                if chat_id != str(TG_CHAT_ID):
                    continue  # игнорируем сообщения не из вашего чата
                text = message.get("text")
                if text:
                    dispatch_command(acc, tg, text)

            if time.time() - last_schedule_check > 60:
                last_schedule_check = time.time()
                maybe_run_scheduled_autopublish(acc, tg)

        except Exception:
            logger.exception("Ошибка в цикле Telegram-команд, продолжаем через 5 сек.")
            time.sleep(5)


# =========================================================================
#  ГЛАВНЫЙ ЦИКЛ
# =========================================================================

def run():
    check_env()
    init_db()

    tg = TelegramNotifier(TG_BOT_TOKEN, TG_CHAT_ID)

    logger.info("Авторизация в Playerok...")
    acc = Account(
        cookies=PLAYEROK_COOKIES,
        user_agent=PLAYEROK_USER_AGENT,
    ).get()
    logger.info("Успешно авторизован как %s (id=%s)", acc.username, acc.id)

    autopublish_time = kv_get("autopublish_time")
    status_line = f"⏰ авто-публикация черновиков в {autopublish_time}" if autopublish_time else "⏰ авто-публикация по расписанию выключена"
    tg.send(
        f"🤖 Бот запущен. Аккаунт Playerok: <b>{acc.username}</b>\n{status_line}\n\nОтправьте /help — список команд."
    )

    stop_event = threading.Event()
    tg_thread = threading.Thread(target=telegram_and_scheduler_loop, args=(acc, tg, stop_event), daemon=True)
    tg_thread.start()

    seen_deal_ids = set()
    listener = EventListener(acc)

    backoff = 5
    while True:
        try:
            for event in listener.listen():
                if event.type is EventTypes.NEW_MESSAGE:
                    handle_new_message(acc, tg, event)

                elif event.type in (EventTypes.NEW_DEAL, EventTypes.ITEM_PAID):
                    handle_sale(acc, tg, event.deal, seen_deal_ids)

                elif event.type == EventTypes.NEW_REVIEW:
                    handle_new_review(tg, event)

                elif event.type == EventTypes.DEAL_HAS_PROBLEM:
                    tg.send(f"❗ Проблема по сделке <code>{event.deal.id}</code>, проверьте Playerok.")

            backoff = 5

        except (BotCheckDetectedException, UnauthorizedError) as e:
            logger.error("Проблема с авторизацией/антибот-защитой: %s", e)
            tg.send(
                "🚫 Бот потерял доступ к аккаунту Playerok (истекли куки или сработала "
                "антибот-защита). Обновите PLAYEROK_COOKIES и перезапустите бота."
            )
            time.sleep(60)

        except Exception:
            logger.exception("Слушатель событий упал, переподключаюсь через %s сек.", backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 300)


if __name__ == "__main__":
    run()
