"""Настройки Telegram Digest. Замените примеры каналов своими публичными каналами."""
from zoneinfo import ZoneInfo

# Публичные каналы. Допускаются @username, username и https://t.me/username.
# Примеры ниже — заглушки, замените их реальными каналами.
CHANNELS: list[str] = [
    "@meduza", "@bbcrussian", "@tass_agency", "@rian_ru", "@rt_russian",
    "@fontankaspb", "@thebell_io", "@verstka_media", "@holodmedia",
    "@zona_media", "@novaya_gazeta", "@the_ins_ru", "@ostorozhno_novosti",
    "@breakingmash", "@shot_shot", "@readovkanews", "@rianovosti",
    "@rbc_news", "@kommersant", "@vedomosti", "@forbesrussia",
    "@techsparks", "@habr_com", "@vcnews", "@rozetked",
    "@popmech", "@nplusone", "@postnews",
]

# Все внутренние даты и имена файлов используют московское время.
TZ = ZoneInfo("Europe/Moscow")

# Случайная пауза между каналами снижает частоту запросов.
PAUSE_MIN = 2.0
PAUSE_MAX = 4.0

MAX_POSTS_PER_CHANNEL = 50
MAX_SCAN_PER_CHANNEL = 300
MAX_TEXT_LEN = 4000
MIN_TEXT_LEN = 20

SKIP_ADS = True
AD_MARKERS: list[str] = [
    "#реклама", "реклама.", "erid", "на правах рекламы",
    "партнёрский материал", "партнерский материал",
    "промокод", "спонсорский материал", "advertisement",
    "#ad", "рекламная интеграция",
]

FIRST_RUN_WINDOW_HOURS = 24
MAX_LOOKBACK_HOURS = 48
MAX_CHARS_PER_FILE = 300000

# None создаёт компактный JSON и экономит токены при последующей обработке.
JSON_INDENT: int | None = 4

FLOOD_WAIT_MAX_AUTO = 300
TEST_CHANNELS_COUNT = 3

STATE_FILE = "state.json"
CHANNELS_CACHE_FILE = "channels_cache.json"
OUTPUT_DIR = "output"
LOG_FILE = "logs/digest.log"
LOG_LEVEL = "INFO"
LOG_MAX_BYTES = 5 * 1024 * 1024
LOG_BACKUP_COUNT = 3
