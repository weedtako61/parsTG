"""Сборщик публикаций Telegram в JSON для последующей обработки нейросетью.

ВАЖНО О БЕЗОПАСНОСТИ:
Файл .session даёт полный доступ к вашему Telegram-аккаунту.
Не коммитьте его, не пересылайте и не храните в общих папках.
Первую авторизацию лучше выполнить на своём компьютере; затем при желании
можно перенести защищённый файл сессии на сервер.

Python 3.10+, Telethon 1.x.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import re
import time
import traceback
from collections import defaultdict
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any
from unicodedata import category as unicode_category
from urllib.parse import urlparse

from dotenv import load_dotenv
import qrcode
from telethon import TelegramClient, events
from telethon.errors import (
    ChannelInvalidError,
    ChannelPrivateError,
    FloodWaitError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    PhoneNumberInvalidError,
    SessionPasswordNeededError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
)
from telethon.tl.types import InputPeerChannel, MessageMediaPoll

import config


# ---------------------------------------------------------------------------
# Конфигурация процесса и пути
# ---------------------------------------------------------------------------
load_dotenv()
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / config.OUTPUT_DIR
LOG_PATH = BASE_DIR / config.LOG_FILE
STATE_PATH = BASE_DIR / config.STATE_FILE
CACHE_PATH = BASE_DIR / config.CHANNELS_CACHE_FILE

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

API_ID_VALUE = os.getenv("API_ID", "").strip()
API_HASH = os.getenv("API_HASH", "").strip()
SESSION_NAME = os.getenv("SESSION_NAME", "telegram_digest").strip()

if not API_ID_VALUE or not API_HASH:
    raise RuntimeError("Укажите API_ID и API_HASH в .env (см. .env.example).")
try:
    API_ID = int(API_ID_VALUE)
except ValueError as exc:
    raise RuntimeError("API_ID должен быть целым числом.") from exc

logger = logging.getLogger("telegram_digest")
digest_lock = asyncio.Lock()


def setup_logging() -> None:
    """Настраивает консольный и ротируемый файловый лог без текстов постов."""
    logger.setLevel(getattr(logging, config.LOG_LEVEL.upper(), logging.INFO))
    logger.handlers.clear()
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    file_handler = RotatingFileHandler(
        LOG_PATH,
        maxBytes=config.LOG_MAX_BYTES,
        backupCount=config.LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(console)
    logger.addHandler(file_handler)


# ---------------------------------------------------------------------------
# Каналы, время и JSON
# ---------------------------------------------------------------------------
def normalize_username(value: str) -> str:
    """Убирает @, пробелы и префикс t.me; возвращает username без @."""
    value = value.strip()
    if not value:
        return ""
    if value.startswith(("https://", "http://")):
        parsed = urlparse(value)
        if parsed.netloc.lower() not in {"t.me", "www.t.me"}:
            return ""
        value = parsed.path.strip("/").split("/")[0]
    value = value.strip().lstrip("@").strip()
    return value if value and not any(ch.isspace() for ch in value) else ""


def get_channels() -> list[str]:
    """Возвращает уникальные каналы, сохраняя порядок из config.py."""
    result: list[str] = []
    seen: set[str] = set()
    for item in config.CHANNELS:
        username = normalize_username(item)
        key = username.casefold()
        if username and key not in seen:
            result.append(username)
            seen.add(key)
        elif not username:
            logger.warning("Пропущена некорректная запись канала в config.py.")
    return result


def now_moscow() -> datetime:
    """Возвращает текущее время в Europe/Moscow."""
    return datetime.now(config.TZ)


def as_moscow(value: datetime) -> datetime:
    """Конвертирует aware datetime из Telegram (UTC) в московское время."""
    if value.tzinfo is None:
        # Для дат Telegram naive-время трактуем как UTC, а не как локальное.
        from datetime import timezone
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(config.TZ)


def iso_moscow(value: datetime) -> str:
    """Форматирует дату ISO 8601 с офсетом +03:00 без микросекунд."""
    return as_moscow(value).replace(microsecond=0).isoformat()


def read_json(path: Path, default: Any) -> Any:
    """Читает JSON; при отсутствии или повреждении файла возвращает default."""
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as stream:
            return json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        logger.error("Не удалось прочитать %s: %s", path.name, exc)
        return default


def atomic_write_json(path: Path, data: Any) -> None:
    """Атомарно записывает JSON через временный файл и os.replace."""
    temp_path = path.with_name(path.name + ".tmp")
    try:
        with temp_path.open("w", encoding="utf-8") as stream:
            json.dump(data, stream, ensure_ascii=False, indent=config.JSON_INDENT)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_path, path)
    finally:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                logger.warning("Не удалось удалить временный файл %s.", temp_path)

 
def load_state() -> dict[str, Any]:
    """Загружает last_id по каналам; повреждённое состояние не останавливает запуск."""
    value = read_json(STATE_PATH, {})
    return value if isinstance(value, dict) else {}


def load_cache() -> dict[str, Any]:
    """Загружает кеш сущностей каналов."""
    value = read_json(CACHE_PATH, {})
    return value if isinstance(value, dict) else {}


async def resolve_channel(
    client: TelegramClient,
    username: str,
    cache: dict[str, Any],
) -> tuple[Any, str]:
    """Берёт сущность из кеша по id/access_hash или разрешает username заново."""
    key = username.casefold()
    cached = cache.get(key)
    if isinstance(cached, dict):
        try:
            entity_id = int(cached["id"])
            access_hash = int(cached["access_hash"])
            entity = await client.get_entity(InputPeerChannel(entity_id, access_hash))
            if not getattr(entity, "broadcast", False):
                raise ValueError(f"@{username} — группа или пользователь, а не канал.")
            return entity, str(getattr(entity, "title", username))
        except Exception as exc:
            logger.warning("Кеш @%s непригоден (%s); разрешаю username заново.", username, exc)

    entity = await client.get_entity(username)
    if not getattr(entity, "broadcast", False):
        raise ValueError(f"@{username} — группа или пользователь, а не broadcast-канал.")
    access_hash = getattr(entity, "access_hash", None)
    if access_hash is not None:
        cache[key] = {
            "id": int(entity.id),
            "access_hash": int(access_hash),
            "title": str(getattr(entity, "title", username)),
            "username": username,
        }
    return entity, str(getattr(entity, "title", username))


# ---------------------------------------------------------------------------
# Извлечение текста, реклама, хеширование и форматирование
# ---------------------------------------------------------------------------
def poll_text(message: Any) -> str | None:
    """Преобразует опрос в текст вопроса и вариантов ответа."""
    media = getattr(message, "media", None)
    if not isinstance(media, MessageMediaPoll):
        return None
    poll = media.poll
    question = getattr(poll, "question", None)
    if not question:
        return None
    lines = [f"[Опрос] {question}"]
    lines.extend(
        f"- {answer.text}"
        for answer in getattr(poll, "answers", [])
        if getattr(answer, "text", None)
    )
    return "\n".join(lines)


def message_text(message: Any) -> tuple[str, bool]:
    """Возвращает форматированный message.text и признак опроса."""
    text = getattr(message, "text", None) or ""
    if text.strip():
        return text.strip(), False
    value = poll_text(message)
    return (value.strip(), True) if value else ("", False)


def is_ad(text: str) -> bool:
    """Ищет рекламные маркеры без учёта регистра."""
    if not config.SKIP_ADS:
        return False
    lowered = text.casefold()
    return any(marker.casefold() in lowered for marker in config.AD_MARKERS)


URL_RE = re.compile(r"https?://\S+|t\.me/\S+", re.IGNORECASE)


def normalized_for_hash(text: str) -> str:
    """Убирает ссылки, пробелы, пунктуацию и символы/emoji перед дедупликацией."""
    text = URL_RE.sub("", text.casefold())
    return "".join(
        char for char in text
        if not char.isspace() and unicode_category(char)[0] not in {"P", "S", "Z"}
    )


def text_hash(text: str) -> str:
    """Считает SHA-256 нормализованного текста публикации."""
    normalized = normalized_for_hash(text)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest() if normalized else ""


def truncate_text(text: str) -> tuple[str, bool]:
    """Обрезает длинный текст по абзацу или слову, не по произвольному символу."""
    if len(text) <= config.MAX_TEXT_LEN:
        return text, False
    fragment = text[:config.MAX_TEXT_LEN]
    paragraph = fragment.rfind("\n\n")
    if paragraph >= config.MAX_TEXT_LEN // 2:
        fragment = fragment[:paragraph]
    else:
        word = fragment.rfind(" ")
        if word >= config.MAX_TEXT_LEN // 2:
            fragment = fragment[:word]
    return fragment.rstrip() + "\n[Текст обрезан]", True


def forwarded_source(message: Any) -> str | None:
    """Возвращает имя источника пересылки, если Telegram его предоставляет."""
    forward = getattr(message, "forward", None)
    if not forward:
        return None
    chat = getattr(forward, "chat", None)
    if chat:
        return getattr(chat, "title", None) or getattr(chat, "username", None)
    sender = getattr(forward, "sender", None)
    if sender:
        title = getattr(sender, "title", None)
        if title:
            return title
        parts = [getattr(sender, "first_name", None), getattr(sender, "last_name", None)]
        name = " ".join(part for part in parts if part)
        return name or getattr(sender, "username", None)
    return getattr(getattr(message, "fwd_from", None), "from_name", None)


def post_object(
    first_message: Any,
    media_message: Any,
    title: str,
    username: str,
    text: str,
    truncated: bool,
    has_media: bool,
) -> dict[str, Any]:
    """Создаёт объект с фиксированным набором ключей для нейросетевой обработки."""
    return {
        "channel": title,
        "username": f"@{username}",
        "date": iso_moscow(first_message.date),
        "link": f"https://t.me/{username}/{first_message.id}",
        "text": text,
        "views": int(getattr(first_message, "views", 0))
        if getattr(first_message, "views", None) is not None else None,
        "forwarded_from": forwarded_source(media_message),
        "has_media": has_media,
        "truncated": truncated,
    }


# ---------------------------------------------------------------------------
# Сбор одного канала
# ---------------------------------------------------------------------------
def fresh_stats(username: str) -> dict[str, Any]:
    """Создаёт счётчики, чтобы сбой одного канала не влиял на остальные."""
    return {
        "username": username, "title": username, "scanned": 0, "posts": [],
        "no_text": 0, "ads": 0, "duplicates": 0, "short": 0,
        "limit_dropped": 0, "max_scanned_id": 0, "success": False,
        "error": None, "flood_wait": None,
    }


def window_start(mode: str, now: datetime, last_id: int) -> datetime:
    """Вычисляет нижнюю границу окна, не позволяя читать глубже 48 часов."""
    ceiling = now - timedelta(hours=config.MAX_LOOKBACK_HOURS)
    if mode == "today":
        requested = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elif mode in {"24h", "test"}:
        requested = now - timedelta(hours=24)
    elif last_id:
        requested = ceiling
    else:
        requested = now - timedelta(hours=config.FIRST_RUN_WINDOW_HOURS)
    return max(requested, ceiling)


async def collect_channel(
    client: TelegramClient,
    username: str,
    mode: str,
    state: dict[str, Any],
    cache: dict[str, Any],
    seen_hashes: set[str],
    now: datetime,
) -> dict[str, Any]:
    """Последовательно собирает один канал, возвращая посты, статистику и ошибку."""
    stats = fresh_stats(username)
    try:
        entity, title = await resolve_channel(client, username, cache)
        stats["title"] = title
        saved = state.get(username.casefold(), {})
        try:
            last_id = int(saved.get("last_id", 0)) if mode == "default" else 0
        except (TypeError, ValueError, AttributeError):
            last_id = 0
        start = window_start(mode, now, last_id)

        async def scan() -> list[Any]:
            """Читает новые сообщения; включает в max_id даже граничное старое сообщение."""
            messages: list[Any] = []
            async for message in client.iter_messages(
                entity,
                limit=config.MAX_SCAN_PER_CHANNEL,
                min_id=last_id or None,
            ):
                stats["scanned"] += 1
                stats["max_scanned_id"] = max(stats["max_scanned_id"], int(message.id or 0))
                if message.date and as_moscow(message.date) < start:
                    break
                messages.append(message)
            return messages

        try:
            messages = await scan()
        except FloodWaitError as exc:
            stats["flood_wait"] = int(exc.seconds)
            if exc.seconds > config.FLOOD_WAIT_MAX_AUTO:
                raise RuntimeError(
                    f"FloodWait {exc.seconds} сек. превышает автоматический предел."
                ) from exc
            await progress_note(
                client,
                f"⏳ @{username}: FloodWait {exc.seconds} сек.; жду и повторяю один раз.",
            )
            logger.warning("@%s: FloodWait %s сек.; одна повторная попытка.", username, exc.seconds)
            await asyncio.sleep(exc.seconds + 1)
            messages = await scan()

        # Альбомы становятся одной логической публикацией. Ссылка и дата
        # берутся у сообщения с минимальным ID, подпись — у элемента с текстом.
        grouped: dict[str, list[Any]] = defaultdict(list)
        for message in messages:
            if getattr(message, "action", None):
                continue
            grouped[
                f"album:{message.grouped_id}" if getattr(message, "grouped_id", None)
                else f"message:{message.id}"
            ].append(message)

        candidates: list[tuple[Any, Any, str, bool, bool]] = []
        # Обход в порядке получения (новые альбомы раньше старых) сохраняет
        # первое обнаруженное вхождение при дедупликации между каналами.
        for members in grouped.values():
            members.sort(key=lambda item: item.id)
            first = members[0]
            text = ""
            text_message = first
            is_poll = False
            for member in members:
                candidate, candidate_is_poll = message_text(member)
                if candidate:
                    text, text_message, is_poll = candidate, member, candidate_is_poll
                    break
            if not text:
                stats["no_text"] += 1
                continue
            if is_ad(text):
                stats["ads"] += 1
                continue
            if len(text.strip()) < config.MIN_TEXT_LEN and not is_poll:
                stats["short"] += 1
                continue
            digest = text_hash(text)
            if digest and digest in seen_hashes:
                stats["duplicates"] += 1
                continue
            if digest:
                seen_hashes.add(digest)
            shortened, truncated = truncate_text(text)
            candidates.append((first, text_message, shortened, truncated, any(m.media for m in members)))

        # Ограничение оставляет самые новые публикации канала.
        candidates.sort(key=lambda item: item[0].date, reverse=True)
        if len(candidates) > config.MAX_POSTS_PER_CHANNEL:
            stats["limit_dropped"] = len(candidates) - config.MAX_POSTS_PER_CHANNEL
            candidates = candidates[:config.MAX_POSTS_PER_CHANNEL]
        candidates.sort(key=lambda item: item[0].date)

        for first, text_message, text, truncated, has_media in candidates:
            stats["posts"].append(
                post_object(first, text_message, title, username, text, truncated, has_media)
            )
        stats["success"] = True
        logger.info(
            "@%s | просмотрено=%d | сохранено=%d | без текста=%d | реклама=%d | дубли=%d",
            username, stats["scanned"], len(stats["posts"]),
            stats["no_text"], stats["ads"], stats["duplicates"],
        )
    except (
        UsernameNotOccupiedError, UsernameInvalidError,
        ChannelPrivateError, ChannelInvalidError, ValueError,
    ) as exc:
        stats["error"] = f"{type(exc).__name__}: {exc}"
        logger.error("@%s: %s", username, stats["error"])
    except Exception as exc:
        stats["error"] = f"{type(exc).__name__}: {exc}"
        logger.error("@%s: %s\n%s", username, exc, traceback.format_exc())
    return stats


# ---------------------------------------------------------------------------
# Разбиение и отправка JSON
# ---------------------------------------------------------------------------
def split_posts(posts: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """Делит JSON на части по границам постов, стараясь не делить канал."""
    by_channel: dict[str, list[dict[str, Any]]] = {}
    for post in posts:
        by_channel.setdefault(post["username"], []).append(post)

    parts: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    chars = 0
    for channel_posts in by_channel.values():
        channel_chars = sum(len(post["text"]) for post in channel_posts)
        if current and chars + channel_chars > config.MAX_CHARS_PER_FILE:
            parts.append(current)
            current, chars = [], 0
        for post in channel_posts:
            size = len(post["text"])
            if current and chars + size > config.MAX_CHARS_PER_FILE:
                parts.append(current)
                current, chars = [], 0
            current.append(post)
            chars += size
    if current:
        parts.append(current)
    return parts


def write_json_files(posts: list[dict[str, Any]], timestamp: datetime) -> list[Path]:
    """Сохраняет строго JSON-массивы с московским временем в именах файлов."""
    parts = split_posts(posts)
    stamp = timestamp.strftime("%Y-%m-%d_%H%M")
    paths: list[Path] = []
    for index, part in enumerate(parts, start=1):
        name = f"digest_{stamp}.json" if len(parts) == 1 else f"digest_{stamp}_part{index}.json"
        path = OUTPUT_DIR / name
        with path.open("w", encoding="utf-8") as stream:
            json.dump(part, stream, ensure_ascii=False, indent=config.JSON_INDENT)
        paths.append(path)
    return paths


async def progress_note(client: TelegramClient, text: str) -> None:
    """Отправляет короткую служебную заметку в Избранное; она не является командой."""
    try:
        await client.send_message("me", text)
    except Exception:
        logger.exception("Не удалось отправить служебное сообщение.")


async def safe_edit(client: TelegramClient, message: Any, text: str) -> None:
    """Редактирует прогресс без риска остановить сбор из-за ошибки Telegram."""
    try:
        await client.edit_message("me", message, text)
    except Exception as exc:
        logger.warning("Не удалось отредактировать сообщение прогресса: %s", exc)


async def send_files(
    client: TelegramClient,
    paths: list[Path],
    mode: str,
    period: str,
    count: int,
) -> tuple[bool, list[str]]:
    """Отправляет файлы в Избранное; удаляет каждый локальный файл только после отправки."""
    labels = {
        "default": "/digest", "today": "/digest today",
        "24h": "/digest 24h", "test": "/digest test",
    }
    errors: list[str] = []
    for index, path in enumerate(paths, start=1):
        caption = (
            f"📊 Telegram Digest\nРежим: {labels[mode]}\n"
            f"Период МСК: {period}\nПубликаций: {count}\n"
            f"Файл: {index}/{len(paths)}"
        )
        try:
            await client.send_file("me", str(path), caption=caption)
            path.unlink()
            logger.info("Отправлен файл %s.", path.name)
        except Exception as exc:
            errors.append(f"{path}: {type(exc).__name__}: {exc}")
            logger.exception("Не удалось отправить %s; файл оставлен на диске.", path)
    return not errors, errors


def report_text(
    mode: str,
    start: datetime,
    end: datetime,
    stats: list[dict[str, Any]],
    post_count: int,
    part_count: int,
    elapsed: float,
) -> str:
    """Собирает отчёт по каналам, фильтрам, ошибкам и длительности."""
    successful = sum(item["success"] for item in stats)
    totals = {
        key: sum(item[key] for item in stats)
        for key in ("no_text", "ads", "duplicates", "short", "limit_dropped")
    }
    names = {"default": "/digest", "today": "/digest today", "24h": "/digest 24h", "test": "/digest test"}
    lines = [
        "📊 ИТОГОВЫЙ ОТЧЁТ TELEGRAM DIGEST", "",
        f"Режим: {names[mode]}",
        f"Период МСК: {iso_moscow(start)} — {iso_moscow(end)}",
        f"Успешных каналов: {successful}/{len(stats)}",
        f"Всего публикаций: {post_count}", "",
        "Пропущено:",
        f"• без текста: {totals['no_text']}",
        f"• реклама: {totals['ads']}",
        f"• дубли: {totals['duplicates']}",
        f"• короткие: {totals['short']}",
        f"• по лимиту MAX_POSTS_PER_CHANNEL: {totals['limit_dropped']}",
        f"Частей JSON: {part_count}",
        f"Были отброшены посты по лимиту: {'да' if totals['limit_dropped'] else 'нет'}",
        f"Время выполнения: {elapsed:.1f} сек.",
    ]
    failures = [item for item in stats if item["error"]]
    if failures:
        lines.extend(["", "Ошибки каналов:"])
        lines.extend(f"• @{item['username']}: {item['error']}" for item in failures)
    else:
        lines.extend(["", "Ошибок каналов нет."])
    waits = [item for item in stats if item["flood_wait"] is not None]
    if waits:
        lines.extend(["", "FloodWait:"])
        lines.extend(f"• @{item['username']}: {item['flood_wait']} сек." for item in waits)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Команда и основной сценарий
# ---------------------------------------------------------------------------
def parse_command(text: str) -> str | None:
    """Преобразует команду в режим; None означает неизвестные аргументы."""
    parts = text.strip().split()
    if not parts or parts[0].casefold() != "/digest":
        return None
    if len(parts) == 1:
        return "default"
    if len(parts) != 2:
        return None
    return {"today": "today", "24h": "24h", "test": "test"}.get(parts[1].casefold())


async def run_digest(client: TelegramClient, mode: str) -> None:
    """Собирает каналы по очереди и фиксирует last_id только после успешной отправки."""
    started = time.monotonic()
    now = now_moscow()
    state = load_state()
    new_state = dict(state)
    cache = load_cache()
    channels = get_channels()
    if mode == "test":
        channels = channels[:config.TEST_CHANNELS_COUNT]
    if not channels:
        await progress_note(client, "❌ Список CHANNELS пуст или содержит некорректные записи.")
        return

    progress = await client.send_message("me", "⏳ Собираю дайджест…")
    all_stats: list[dict[str, Any]] = []
    all_posts: list[dict[str, Any]] = []
    seen_hashes: set[str] = set()
    logger.info("Старт: режим=%s, каналов=%d.", mode, len(channels))

    for index, username in enumerate(channels, start=1):
        item = await collect_channel(client, username, mode, state, cache, seen_hashes, now)
        all_stats.append(item)
        all_posts.extend(item["posts"])

        # Обновляем только локальную копию. На диск запишем её лишь после
        # успешной отправки всех частей JSON; ошибочные каналы не продвигаем.
        if mode == "default" and item["success"]:
            previous = state.get(username.casefold(), {})
            try:
                previous_id = int(previous.get("last_id", 0))
            except (TypeError, ValueError, AttributeError):
                previous_id = 0
            new_state[username.casefold()] = {
                "last_id": max(previous_id, item["max_scanned_id"]),
                "updated_at": iso_moscow(now),
                "title": item["title"],
            }

        if index % 5 == 0 or index == len(channels):
            await safe_edit(
                client, progress,
                f"⏳ Собираю дайджест…\nОбработано {index}/{len(channels)} каналов.\n"
                f"Найдено публикаций: {len(all_posts)}.",
            )

        # Пауза выполняется и после последнего канала, как задано в требованиях.
        await asyncio.sleep(random.uniform(config.PAUSE_MIN, config.PAUSE_MAX))

    atomic_write_json(CACHE_PATH, cache)

    ceiling = now - timedelta(hours=config.MAX_LOOKBACK_HOURS)
    if mode == "today":
        start = max(now.replace(hour=0, minute=0, second=0, microsecond=0), ceiling)
    elif mode in {"24h", "test"}:
        start = max(now - timedelta(hours=24), ceiling)
    else:
        start = ceiling

    paths = write_json_files(all_posts, now) if all_posts else []
    sent = True
    send_errors: list[str] = []
    if paths:
        sent, send_errors = await send_files(
            client, paths, mode, f"{iso_moscow(start)} — {iso_moscow(now)}", len(all_posts)
        )

    # Если все файлы доставлены, состояние продвигается для каждого
    # успешно обработанного канала. Ошибочные каналы остаются на прежнем last_id.
    if mode == "default" and all_posts and sent:
        atomic_write_json(STATE_PATH, new_state)
        logger.info("state.json атомарно обновлён.")
    elif mode == "default":
        logger.info("state.json не обновлён: нет постов или отправка не завершилась успешно.")

    elapsed = time.monotonic() - started
    report = report_text(mode, start, now, all_stats, len(all_posts), len(paths), elapsed)
    if not all_posts:
        report += "\n\nℹ️ Подходящих публикаций нет; JSON не создавался, состояние не изменено."
    if send_errors:
        report += "\n\n⚠️ Ошибки отправки файлов (файлы оставлены на диске):\n"
        report += "\n".join(f"• {error}" for error in send_errors)
    if mode == "default":
        report += "\n\nСостояние: " + ("обновлено." if all_posts and sent else "не изменялось.")
    else:
        report += "\n\nСостояние не изменялось (режим просмотра)."
    await client.send_message("me", report)
    await safe_edit(
        client, progress,
        f"✅ Сбор завершён. Каналов: {len(channels)}. "
        f"Публикаций: {len(all_posts)}. Время: {elapsed:.1f} сек.",
    )
    logger.info("Завершено: постов=%d, ошибок каналов=%d, время=%.1f сек.",
                len(all_posts), sum(bool(x["error"]) for x in all_stats), elapsed)


async def register_handlers(client: TelegramClient) -> None:
    """Слушает команды только в Избранном, отправленные самим аккаунтом."""
    @client.on(events.NewMessage(
        chats="me",
        outgoing=True,
        pattern=r"^/digest(?:\s+.*)?$",
    ))
    async def digest_handler(event: Any) -> None:
        if digest_lock.locked():
            await client.send_message("me", "Сбор уже выполняется, подожди.")
            return
        async with digest_lock:
            mode = parse_command(event.raw_text or "")
            if mode is None:
                await client.send_message(
                    "me",
                    "Неизвестная команда. Доступно:\n"
                    "/digest — новые публикации\n"
                    "/digest today — с полуночи МСК\n"
                    "/digest 24h — последние 24 часа\n"
                    "/digest test — первые 3 канала за 24 часа",
                )
                return
            try:
                await run_digest(client, mode)
            except asyncio.CancelledError:
                logger.warning("Сбор отменён.")
                raise
            except Exception as exc:
                logger.error("Критическая ошибка запуска: %s\n%s", exc, traceback.format_exc())
                await progress_note(client, f"❌ Критическая ошибка: {type(exc).__name__}: {exc}")


def secure_session_file() -> None:
    """Ограничивает права файла сессии до 600; на Windows chmod может быть недоступен."""
    session_path = BASE_DIR / f"{SESSION_NAME}.session"
    if session_path.exists():
        try:
            os.chmod(session_path, 0o600)
        except OSError as exc:
            logger.warning("Не удалось выставить права 600 для сессии: %s", exc)


def render_qr(url: str) -> None:
    """Печатает QR-код входа в терминал."""
    code = qrcode.QRCode(border=1)
    code.add_data(url)
    code.make(fit=True)
    matrix = code.get_matrix()
    print("\n".join(
        "".join("██" if cell else "  " for cell in row)
        for row in matrix
    ))


async def authorize_user(client: TelegramClient) -> None:
    """Вход через QR-код; повторный запрос SMS не требуется."""
    await client.connect()
    if await client.is_user_authorized():
        logger.info("Найдена действующая Telegram-сессия; повторный вход не нужен.")
        return

    print("\nАвторизация через QR-код.")
    print("На телефоне Telegram: Настройки → Устройства → Подключить устройство.")
    print("Отсканируй QR-код с экрана. Не отправляй его другим людям.\n")

    qr = await client.qr_login()
    while True:
        render_qr(qr.url)
        try:
            await qr.wait()
            break
        except asyncio.TimeoutError:
            print("\nQR-код истёк, обновляю…")
            await qr.recreate()
        except SessionPasswordNeededError:
            import getpass
            password = getpass.getpass("Включена двухэтапная аутентификация. Введи пароль 2FA: ")
            await client.sign_in(password=password)
            break

    logger.info("Вход в личный Telegram-аккаунт выполнен через QR.")


async def main() -> None:
    """Авторизуется, показывает способ доставки кода и ждёт команды."""
    setup_logging()
    logger.info("Запуск Telegram Digest.")
    client = TelegramClient(
        str(BASE_DIR / SESSION_NAME),
        API_ID,
        API_HASH,
        connection_retries=5,
        retry_delay=5,
    )
    try:
        # В отличие от client.start(), явный вход позволяет показать,
        # какой тип доставки кода реально вернул Telegram.
        await authorize_user(client)
        secure_session_file()
        await register_handlers(client)
        logger.info("Клиент запущен, жду /digest в Избранном.")
        await client.run_until_disconnected()
    finally:
        if client.is_connected():
            await client.disconnect()
        logger.info("Telegram Digest остановлен.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nПолучен Ctrl+C. Завершение работы.")
