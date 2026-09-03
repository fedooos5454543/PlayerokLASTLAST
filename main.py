"""
Telegram-бот для продавца на Playerok. Всё в одном файле.

Что делает:
  1. При продаже товара — присылает в Telegram сообщение с названием товара,
     ценой и ником покупателя.
  2. Пересылает в Telegram входящие сообщения от покупателей из чатов Playerok.
  3. (Опционально) Автоматически пересоздаёт и заново публикует проданный
     товар — чтобы у вас снова появился активный лот.

Запуск:
    pip install -r requirements.txt
    cp .env.example .env   # и заполнить своими данными
    python main.py

ВАЖНО: используется неофициальная библиотека PlayerokAPI (GraphQL-запросы
от имени вашего залогиненного аккаунта через cookies/токен сессии).
Официального публичного API для продавцов Playerok не предоставляет.
Куки могут "протухать" — их периодически нужно обновлять из браузера.
"""

import logging
import os
import tempfile
import time

from dotenv import load_dotenv
import requests

from playerokapi.account import Account
from playerokapi.enums import EventTypes
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

REQUIRED_VARS = {
    "TG_BOT_TOKEN": TG_BOT_TOKEN,
    "TG_CHAT_ID": TG_CHAT_ID,
    "PLAYEROK_COOKIES": PLAYEROK_COOKIES,
    "PLAYEROK_USER_AGENT": PLAYEROK_USER_AGENT,
}


def check_env():
    missing = [k for k, v in REQUIRED_VARS.items() if not v]
    if missing:
        raise SystemExit(
            "Не заполнены переменные окружения: "
            + ", ".join(missing)
            + "\nСкопируйте .env.example в .env и заполните значения."
        )


# =========================================================================
#  TELEGRAM: отправка сообщений
# =========================================================================

class TelegramNotifier:
    """Простой клиент для отправки сообщений в Telegram через Bot API (только requests)."""

    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.api_url = f"https://api.telegram.org/bot{bot_token}"

    def send(self, text: str, disable_preview: bool = True) -> bool:
        """Отправляет текстовое сообщение в чат. Возвращает True/False по успеху."""
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


# =========================================================================
#  АВТО-ВЫСТАВЛЕНИЕ ПРОДАННОГО ТОВАРА ЗАНОВО
# =========================================================================

def _download_attachments(acc, attachments) -> list:
    """Скачивает файлы-вложения предмета во временные файлы и возвращает список путей."""
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
    """
    Пересоздаёт и публикует копию проданного товара.

    Ограничения: для некоторых категорий часть опций/полей может требовать
    ручной проверки — API не документирует все edge-кейсы каждой категории.
    """
    original = acc.get_item(id=sold_item_id)

    attachment_paths = _download_attachments(acc, getattr(original, "attachments", None))

    # data_fields нужно передавать только с типом ITEM_DATA (те, что заполняет продавец).
    data_fields = getattr(original, "data_fields", None) or []

    # attributes — это словарь уже выбранных опций категории, create_item принимает
    # его напрямую вместо list[GameCategoryOption].
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

    # чистим временные файлы
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

    published = acc.publish_item(new_item.id, status.id)
    return published


# =========================================================================
#  ОБРАБОТЧИКИ СОБЫТИЙ PLAYEROK
# =========================================================================

def format_price(value) -> str:
    try:
        return f"{int(value):,}".replace(",", " ") + " ₽"
    except (TypeError, ValueError):
        return str(value)


def handle_sale(acc, tg: TelegramNotifier, deal, seen_deal_ids: set):
    """Обрабатывает факт продажи товара: шлёт уведомление и (опц.) релистит товар."""
    if deal.id in seen_deal_ids:
        return  # уже обработали эту сделку (события могут дублироваться)
    seen_deal_ids.add(deal.id)

    item = deal.item
    buyer = deal.user

    item_name = getattr(item, "name", "неизвестный товар")
    item_price = format_price(getattr(item, "price", "?"))
    buyer_name = getattr(buyer, "username", "неизвестный покупатель")

    text = (
        "🛒 <b>Новая продажа!</b>\n\n"
        f"📦 Товар: <b>{item_name}</b>\n"
        f"💰 Цена: {item_price}\n"
        f"👤 Покупатель: {buyer_name}\n"
        f"🆔 Сделка: <code>{deal.id}</code>"
    )
    tg.send(text)
    logger.info("Продажа: %s купил(а) '%s' за %s", buyer_name, item_name, item_price)

    if AUTO_RELIST and item is not None:
        try:
            new_item = relist_sold_item(acc, item.id, RELIST_PRIORITY_STATUS_ID)
            tg.send(
                "♻️ Товар автоматически выставлен на продажу заново:\n"
                f"<b>{new_item.name}</b> — {format_price(new_item.price)}\n"
                "⚠️ Рекомендую проверить карточку вручную (опции/фото могли скопироваться не полностью)."
            )
            logger.info("Авто-релист: создан новый предмет %s", new_item.id)
        except Exception:
            logger.exception("Не удалось авто-выставить товар заново (item_id=%s)", item.id)
            tg.send(
                "⚠️ Не получилось автоматически перевыставить проданный товар "
                f"«{item_name}». Проверьте вручную и логи бота."
            )


def handle_new_message(acc, tg: TelegramNotifier, event):
    """Пересылает в Telegram входящие сообщения от покупателей/собеседников."""
    message = event.message
    if message.user.id == acc.id:
        return  # это наше собственное сообщение, не пересылаем

    sender = getattr(message.user, "username", "Собеседник")
    text = message.text or "[вложение/изображение]"

    tg.send(f"💬 <b>{sender}</b>:\n{text}")
    logger.info("Новое сообщение от %s: %s", sender, text[:80])


# =========================================================================
#  ГЛАВНЫЙ ЦИКЛ
# =========================================================================

def run():
    check_env()

    tg = TelegramNotifier(TG_BOT_TOKEN, TG_CHAT_ID)

    logger.info("Авторизация в Playerok...")
    acc = Account(
        cookies=PLAYEROK_COOKIES,
        user_agent=PLAYEROK_USER_AGENT,
    ).get()
    logger.info("Успешно авторизован как %s (id=%s)", acc.username, acc.id)
    tg.send(f"🤖 Бот запущен. Аккаунт Playerok: <b>{acc.username}</b>")

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

                elif event.type == EventTypes.DEAL_HAS_PROBLEM:
                    tg.send(f"❗ Проблема по сделке <code>{event.deal.id}</code>, проверьте Playerok.")

            backoff = 5  # сбрасываем backoff после нормального завершения цикла

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
