import asyncio
import logging
import os
import time
import uuid
import re
import json
import tempfile
import subprocess
import threading
import sys
import gc
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    FSInputFile, BufferedInputFile, InputMediaPhoto, InputMediaVideo, 
    InlineQueryResultCachedVideo, InlineQueryResultCachedPhoto, InlineQueryResultCachedAudio,
    InlineQueryResultArticle, InputTextMessageContent,
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
)
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.exceptions import TelegramEntityTooLarge, TelegramNetworkError
import aiohttp
import io

from config import BOT_TOKEN, PROXY_URL, ENABLE_CLEANUP, USE_PROXY, ENABLE_API
from database import Database

def unload_heavy_modules():
    """Выгружает тяжёлые модули из памяти после использования.
    БЕЗОПАСНАЯ версия: только удаляет из sys.modules, без очистки __dict__,
    чтобы не ломать параллельные потоки, которые уже держат ссылки на модуль."""
    modules_to_unload = [
        'speech_recognition', 'pydub', 'cv2', 'numpy', 'PIL', 'qrcode',
        'yt_dlp', 'pytubefix', 'yt_dlp.extractor', 'yt_dlp.downloader',
        'yt_dlp.postprocessor', 'yt_dlp.utils', 'pydub.utils',
        'PIL.Image', 'PIL._imaging', 'cv2.cv2'
    ]
    for module_name in modules_to_unload:
        keys_to_remove = [key for key in list(sys.modules.keys()) if key == module_name or key.startswith(module_name + '.')]
        for key in keys_to_remove:
            try:
                del sys.modules[key]
            except KeyError:
                pass

    gc.collect()

def log_resource_usage(context: str):
    """Подробное логирование использования RAM и CPU"""
    try:
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        
        # RAM информация
        memory_info = process.memory_info()
        rss_mb = memory_info.rss / (1024 * 1024)  # Resident Set Size в MB
        vms_mb = memory_info.vms / (1024 * 1024)  # Virtual Memory Size в MB
        
        # CPU информация
        cpu_percent = process.cpu_percent(interval=0.1)
        cpu_times = process.cpu_times()
        
        # Системная память
        system_memory = psutil.virtual_memory()
        system_memory_percent = system_memory.percent
        system_memory_available_mb = system_memory.available / (1024 * 1024)
        
        # Количество потоков
        num_threads = process.num_threads()
        
        # Количество открытых файловых дескрипторов
        try:
            num_fds = process.num_fds() if hasattr(process, 'num_fds') else len(process.open_files())
        except:
            num_fds = 0
        
        logger.info(
            f"[RESOURCES] {context} | "
            f"RAM: {rss_mb:.2f} MB (RSS) / {vms_mb:.2f} MB (VMS) | "
            f"CPU: {cpu_percent:.1f}% (user: {cpu_times.user:.2f}s, system: {cpu_times.system:.2f}s) | "
            f"System RAM: {system_memory_percent:.1f}% used ({system_memory_available_mb:.2f} MB free) | "
            f"Threads: {num_threads} | FDs: {num_fds}"
        )
        sys.stdout.flush()
        
    except ImportError:
        # Если psutil не установлен, логируем только базовую информацию
        logger.info(f"[RESOURCES] {context} | psutil not available for detailed monitoring")
        sys.stdout.flush()
    except Exception as e:
        logger.warning(f"[RESOURCES] Error logging resources for {context}: {e}")
        sys.stdout.flush()

# Ленивая загрузка тяжёлых модулей - импортируем только когда нужно
# НЕ импортируем на уровне модуля:
# - yt_dlp (тяжёлый, только при скачивании)
# - speech_recognition (тяжёлый, только при транскрибации)
# - pydub (тяжёлый, только при обработке аудио)
# - qrcode, PIL (только при генерации QR)
# - cv2, numpy (тяжёлые, только при декодировании QR)
# - Downloader (создаём только при первом использовании)

# Channel info for subscription check
CHANNEL_USERNAME = 'dreamhood'
CHANNEL_ID = -1001929791068

# Transcription and summary settings
# TRANSCRIBED_TEXTS больше не используется - все хранится в базе данных
MAX_MESSAGE_LENGTH = 4096
API_TOKEN = os.environ.get("API_TOKEN", "YOUR_API_TOKEN_HERE")

# Prompt for summary generation (сжатый, оптимизированный)
PROMPT = """Ты эксперт по созданию кратких саммари. Создай пронумерованный список ключевых пунктов из расшифровки голосового сообщения. ВАЖНО: Это расшифровка голосового сообщения, возможны ошибки распознавания речи. Поняй смысл по контексту и молча исправь/переформулируй текст естественно, чтобы он был понятным и логичным. Правила: 1) Начни сразу со списка без вводных фраз 2) Каждый пункт - одна ключевая мысль или факт 3) Определи пол говорящего по контексту и СТРОГО соблюдай его во всех пунктах, сохраняя первое лицо (я, у меня, мой/моя/моё) 4) Включи 5-10 самых важных пунктов 5) Используй только цифры с точкой (1. 2. 3.) 6) Пиши кратко и по существу 7) Сохраняй хронологию событий если она важна 8) Исправляй очевидные ошибки распознавания речи, сохраняя смысл 9) Переформулируй неясные фразы для лучшего понимания 10) Исправляй искаженные слова по смыслу 11) Сохраняй естественность речи и логику повествования 12) Если речь неразборчива или слишком короткая, укажи это в саммари. Расшифровка: {input_text}"""

# Ленивая инициализация Speech Recognition (создаём только при первом использовании)
_recognizer = None

def get_recognizer():
    """Ленивая загрузка Speech Recognition - создаём только когда нужно"""
    global _recognizer
    if _recognizer is None:
        import speech_recognition as sr
        _recognizer = sr.Recognizer()
        _recognizer.energy_threshold = 200
        _recognizer.dynamic_energy_threshold = True
        _recognizer.dynamic_energy_adjustment_damping = 0.1
        _recognizer.dynamic_energy_ratio = 1.2
        _recognizer.non_speaking_duration = 0.2
        _recognizer.pause_threshold = 0.5
        _recognizer.operation_timeout = 10
    return _recognizer

# Batch processing system for multiple voice messages
BATCH_TIMEOUT = 0.5  # Delay to catch rapid messages (0.5 seconds)
BATCH_MAX_SIZE = 50  # Maximum messages in a batch
user_message_batches = {}  # user_id -> list of messages
batch_timers = {}  # user_id -> timer
batch_lock = threading.Lock()
user_last_message_time = {}  # user_id -> timestamp of last message

# Transcription functions
def transcribe_single_segment(audio_path):
    """Transcribe a single audio segment - optimized version"""
    try:
        # Ленивая загрузка speech_recognition только когда нужно
        import speech_recognition as sr
        
        if not os.path.exists(audio_path):
            logger.error(f"Audio file does not exist: {audio_path}")
            return ""
        
        file_size = os.path.getsize(audio_path)
        if file_size == 0:
            logger.error(f"Audio file is empty: {audio_path}")
            return ""
        
        logger.info(f"Transcribing audio file: {audio_path} (size: {file_size} bytes)")
        
        recognizer = get_recognizer()
        with sr.AudioFile(audio_path) as source:
            # Настройка для фонового шума (duration должен быть >= non_speaking_duration)
            try:
                recognizer.adjust_for_ambient_noise(source, duration=0.5)
            except (AssertionError, AttributeError) as e:
                # Если возникает ошибка, пропускаем adjust (динамическая настройка включена)
                logger.debug(f"Skipping ambient noise adjustment: {e}")
            
            audio_data = recognizer.record(source)
            try:
                text = recognizer.recognize_google(audio_data, language='ru-RU', show_all=False)
                if text:
                    logger.info(f"Successfully transcribed {len(text)} characters")
                return text if text else ""
            except sr.UnknownValueError:
                logger.warning(f"Speech could not be understood in file: {audio_path}")
                return ""
            except (sr.RequestError, TimeoutError) as e:
                logger.error(f"Request/timeout error in speech recognition for {audio_path}: {e}")
                # Retry once for timeout errors
                if isinstance(e, TimeoutError):
                    try:
                        logger.info(f"Retrying transcription for {audio_path} after timeout...")
                        with sr.AudioFile(audio_path) as source:
                            recognizer.adjust_for_ambient_noise(source, duration=0.3)
                            audio_data = recognizer.record(source)
                            text = recognizer.recognize_google(audio_data, language='ru-RU', show_all=False)
                            if text:
                                logger.info(f"Successfully transcribed on retry: {len(text)} characters")
                            return text if text else ""
                    except Exception as retry_error:
                        logger.error(f"Retry also failed: {retry_error}")
                return ""
    except FileNotFoundError as e:
        logger.error(f"Audio file not found: {audio_path}, error: {e}", exc_info=True)
        return ""
    except Exception as e:
        logger.error(f"Error in transcribe_single_segment for {audio_path}: {e}", exc_info=True)
        return ""

def transcribe_audio_segments(audio_path, max_segment_duration=30):
    """Transcribe audio by splitting it into segments for better accuracy"""
    try:
        # Ленивая загрузка pydub только когда нужно
        from pydub import AudioSegment
        audio = AudioSegment.from_wav(audio_path)
        total_duration = len(audio) / 1000.0
        
        if total_duration <= max_segment_duration:
            return transcribe_single_segment(audio_path)
        
        segments = []
        segment_length = max_segment_duration * 1000
        
        for i in range(0, len(audio), segment_length):
            segment = audio[i:i + segment_length]
            segment_path = audio_path.replace('.wav', f'_segment_{i//segment_length}.wav')
            segment.export(segment_path, format="wav")
            segments.append(segment_path)
        
        transcribed_texts = []
        for segment_path in segments:
            try:
                segment_text = transcribe_single_segment(segment_path)
                if segment_text and segment_text.strip():
                    transcribed_texts.append(segment_text.strip())
            except Exception as e:
                logger.warning(f"Failed to transcribe segment {segment_path}: {e}")
                continue
            finally:
                if os.path.exists(segment_path):
                    os.remove(segment_path)
        
        return ' '.join(transcribed_texts) if transcribed_texts else ""
    except Exception as e:
        logger.error(f"Error in transcribe_audio_segments: {e}")
        return ""

async def generate_summary(text: str) -> str:
    """Generate summary using API"""
    url = "https://api.intelligence.io.solutions/api/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {API_TOKEN}",
    }
    data = {
        "model": "openai/gpt-oss-120b",
        "messages": [
            {"role": "system", "content": PROMPT.format(input_text=text)},
            {"role": "user", "content": text}
        ],
    }
    
    try:
        logger.info(f"Attempting API call to: {url}")
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=data) as response:
                logger.info(f"API response status: {response.status}")
                if response.status != 200:
                    error = await response.text()
                    logger.error(f"API error response: {error}")
                    return f"❌ Ошибка API: {error}"
                
                response_data = await response.json()
                if 'choices' in response_data and len(response_data['choices']) > 0:
                    bot_response = response_data['choices'][0]['message']['content']
                    bot_response = re.sub(r'<think>.*?</think>', '', bot_response, flags=re.DOTALL)
                    bot_response = re.sub(r'<[^>]+>', '', bot_response)
                    bot_response = re.sub(r'\n\s*\n', '\n', bot_response)
                    bot_response = bot_response.strip()
                    
                    if not bot_response or len(bot_response.strip()) < 10:
                        return "❌ Получен пустой ответ от модели"
                    
                    return bot_response
                else:
                    logger.error(f"Unexpected API response structure: {response_data}")
                    return "❌ Не удалось обработать ответ API"
    except Exception as e:
        logger.error(f"Exception during API call: {str(e)}")
        return f"❌ Произошла ошибка: {str(e)}"

def generate_qr_code(text: str) -> io.BytesIO:
    """Generate QR code image from text - ленивая загрузка qrcode и PIL"""
    # Ленивая загрузка только когда нужно генерировать QR
    import qrcode
    from PIL import Image
    
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_L,
        box_size=10,
        border=4,
    )
    qr.add_data(text)
    qr.make(fit=True)

    img = qr.make_image(fill_color="black", back_color="white")
    
    # Convert to BytesIO
    img_buffer = io.BytesIO()
    img.save(img_buffer, format='PNG')
    img_buffer.seek(0)
    
    # Выгружаем qrcode и PIL из памяти после использования
    unload_heavy_modules()
    
    return img_buffer

def decode_qr_code(image_data: bytes) -> str:
    """Decode QR code from image data using OpenCV - ленивая загрузка cv2 и numpy"""
    try:
        # Ленивая загрузка только когда нужно декодировать QR
        import cv2
        import numpy as np
        
        # Convert bytes to numpy array
        nparr = np.frombuffer(image_data, np.uint8)
        
        # Decode image
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        if img is None:
            return None
        
        # Initialize QR code detector
        detector = cv2.QRCodeDetector()
        
        # Detect and decode QR code
        data, bbox, _ = detector.detectAndDecode(img)
        
        if data:
            result = data
        else:
            result = None
    except Exception as e:
        logger.error(f"Error decoding QR code: {e}")
        result = None
    finally:
        # Выгружаем cv2 и numpy из памяти после использования
        unload_heavy_modules()
    
    return result

# Configure logging with unbuffered output
import sys

class UnbufferedStreamHandler(logging.StreamHandler):
    def emit(self, record):
        super().emit(record)
        self.stream.flush()
        
handler = UnbufferedStreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

handlers_list = [handler]

# Add file handler
file_handler = logging.FileHandler('bot.log', encoding='utf-8')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
handlers_list.append(file_handler)

logging.basicConfig(
    level=logging.INFO,
    handlers=handlers_list,
    force=True
)

logger = logging.getLogger(__name__)

# Ленивая инициализация модулей
_downloader = None

def get_downloader():
    """Ленивая загрузка Downloader - создаём только когда нужно скачивать"""
    global _downloader
    if _downloader is None:
        from downloader import Downloader
        _downloader = Downloader()
    return _downloader

# Ленивая инициализация Database - создаём только при первом использовании
_db = None

def get_db():
    """Ленивая загрузка Database - создаём только когда нужно"""
    global _db
    if _db is None:
        _db = Database()
    return _db

# Для обратной совместимости - создаём объект-прокси
class DatabaseProxy:
    def __getattr__(self, name):
        return getattr(get_db(), name)

db = DatabaseProxy()
# Увеличиваем таймаут для больших файлов (по умолчанию 60 секунд, увеличиваем до 600)
# Создаем сессию с увеличенным таймаутом
session = AiohttpSession()
# Устанавливаем таймаут как число (в секундах), а не ClientTimeout объект
session.timeout = 600  # 10 минут для больших файлов
bot = Bot(token=BOT_TOKEN, session=session)
dp = Dispatcher()

# Global event loop for batch processing
_main_loop = None

def set_main_loop(loop):
    """Set the main event loop for batch processing"""
    global _main_loop
    _main_loop = loop

def get_main_loop():
    """Get the main event loop"""
    global _main_loop
    if _main_loop is None:
        try:
            _main_loop = asyncio.get_running_loop()
        except RuntimeError:
            _main_loop = asyncio.get_event_loop()
    return _main_loop

# Regex patterns
# Catch URLs with or without protocol (http://, https://, or just domain)
URL_PATTERN = r'(https?://\S+|(?:instagram\.com|tiktok\.com|vt\.tiktok\.com|youtube\.com|youtu\.be|soundcloud\.com)/\S+)'

# Поддерживаемые платформы
SUPPORTED_PLATFORMS = [
    'instagram.com',
    'tiktok.com',
    'vt.tiktok.com',
    'youtube.com',
    'youtu.be',
    'soundcloud.com'
]

def is_supported_url(url: str) -> bool:
    """Проверяет, поддерживается ли ссылка ботом"""
    if not url:
        return False
    
    url_lower = url.lower()
    # Проверяем, содержит ли ссылка один из поддерживаемых доменов
    return any(platform in url_lower for platform in SUPPORTED_PLATFORMS)

def normalize_url(url: str) -> str:
    """Нормализует URL для корректного сравнения (убирает пробелы, лишние параметры)"""
    if not url:
        return url
    
    # Убираем пробелы
    url = url.strip()
    
    try:
        # Парсим URL
        from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
        parsed = urlparse(url)
        
        # Для Instagram/Facebook
        if 'instagram.com' in parsed.netloc or 'facebook.com' in parsed.netloc:
            # Оставляем только путь, убираем query parameters (igsh, etc)
            # Но сохраняем img_index если есть (для каруселей)
            query = parse_qs(parsed.query)
            filtered_query = {}
            if 'img_index' in query:
                filtered_query['img_index'] = query['img_index']
            
            # Собираем обратно без лишних параметров
            new_query = urlencode(filtered_query, doseq=True)
            
            # Убираем слэш в конце пути, если он не корень
            path = parsed.path.rstrip('/')
            
            return urlunparse((parsed.scheme, parsed.netloc, path, parsed.params, new_query, ''))
            
        # Для TikTok
        elif 'tiktok.com' in parsed.netloc:
             # Убираем все параметры запроса для TikTok, они обычно трекинговые
             # Убираем слэш в конце пути
             path = parsed.path.rstrip('/')
             return urlunparse((parsed.scheme, parsed.netloc, path, parsed.params, '', ''))
             
        # Для YouTube/Shorts
        elif 'youtube.com' in parsed.netloc or 'youtu.be' in parsed.netloc:
             # Для YouTube видео ID обычно в query 'v' или в пути (для Shorts)
             # Очищаем трекинговые параметры типа feature, si, t (если таймкод не нужен)
             query = parse_qs(parsed.query)
             allowed_params = ['v', 't'] # Оставляем ID видео и таймкод
             filtered_query = {k: v for k, v in query.items() if k in allowed_params}
             new_query = urlencode(filtered_query, doseq=True)
             # Для Shorts путь содержит /shorts/VIDEO_ID, оставляем как есть
             # Для обычных видео путь может быть /watch, оставляем как есть
             path = parsed.path.rstrip('/')
             return urlunparse((parsed.scheme, parsed.netloc, path, parsed.params, new_query, ''))

        # Для SoundCloud
        elif 'soundcloud.com' in parsed.netloc:
             # Убираем трекинговые параметры (обычно query string не нужна для трека)
             path = parsed.path.rstrip('/')
             return urlunparse((parsed.scheme, parsed.netloc, path, parsed.params, '', ''))

    except Exception as e:
        logger.error(f"Error normalizing URL {url}: {e}")
    
    # Fallback если парсинг не удался
    return url.rstrip()

# Helper function to get cache_id from URL
def get_cache_id_for_url(url: str) -> int:
    """Получает cache_id по URL из базы данных"""
    return db.get_cache_id_by_url(url)

# Keyboards
def get_convert_keyboard(cache_id: int = None, bot_username: str = None):
    """Создает клавиатуру с кнопкой конвертировать.
    Если передан cache_id и bot_username, создает ссылку на бота с параметром start."""
    if cache_id and bot_username:
        # Создаем ссылку на бота с параметром start=file_{cache_id}
        url = f"https://t.me/{bot_username}?start=file_{cache_id}"
        builder = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="конвертировать", url=url)]
        ])
    else:
        # Fallback на callback для обратной совместимости
        builder = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="конвертировать", callback_data="convert_menu")]
        ])
    return builder

def get_convert_options_keyboard():
    builder = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="видеокружок", callback_data="convert_note"),
            InlineKeyboardButton(text="голосовое", callback_data="convert_voice")
        ],
        [
            InlineKeyboardButton(text="мп3", callback_data="convert_mp3"),
            InlineKeyboardButton(text="назад", callback_data="convert_back")
        ]
    ])
    return builder

def get_convert_options_keyboard_with_cache_id(cache_id: int):
    """Создает клавиатуру выбора типа конвертации с cache_id в callback_data"""
    builder = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="видеокружок", callback_data=f"conv_note_{cache_id}"),
            InlineKeyboardButton(text="голосовое", callback_data=f"conv_voice_{cache_id}")
        ],
        [
            InlineKeyboardButton(text="мп3", callback_data=f"conv_mp3_{cache_id}"),
            InlineKeyboardButton(text="получить файл", callback_data=f"conv_file_{cache_id}")
        ],
        [
            InlineKeyboardButton(text="расшифровка", callback_data=f"conv_transcription_{cache_id}")
        ]
    ])
    return builder

# Callback Handlers
@dp.callback_query(F.data == "convert_menu")
async def on_convert_menu(callback: CallbackQuery):
    await callback.message.edit_reply_markup(reply_markup=get_convert_options_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "convert_back")
async def on_convert_back(callback: CallbackQuery):
    # Для обратной совместимости со старым форматом меню
    await callback.message.edit_reply_markup(reply_markup=get_convert_keyboard())
    await callback.answer()

@dp.callback_query(F.data.startswith("convert_") | F.data.startswith("conv_"))
async def on_convert_action(callback: CallbackQuery):
    # Парсим callback_data: convert_action (старый) или conv_action_cacheid (новый)
    parts = callback.data.split("_")
    
    # Определяем формат
    if parts[0] == "conv" and len(parts) >= 3:
        # Новый формат: conv_action_cacheid
        action = parts[1]
        try:
            cache_id = int(parts[2])
        except ValueError:
            await callback.answer("❌ Ошибка: неверный ID файла", show_alert=True)
            return
        
        # Получаем file_id из базы по cache_id
        result = db.get_file_by_id(cache_id)
        if not result:
            await callback.answer("❌ Файл не найден", show_alert=True)
            return
        
        file_ids_list, file_type = result
        # Берем первый file_id для конвертации
        file_id = file_ids_list[0] if file_ids_list else None
        if not file_id:
            await callback.answer("❌ Файл не найден", show_alert=True)
            return
    else:
        # Старый формат - берем file_id из сообщения
        action = parts[1]
        if not callback.message.video:
            await callback.answer("Error: No video found in message", show_alert=True)
            return
        file_id = callback.message.video.file_id
    
    action_names = {
        "video": "видео",
        "file": "файл",
        "voice": "голосовое",
        "note": "видеокружок",
        "mp3": "аудиофайл",
        "summary": "саммари",
        "transcription": "расшифровка"
    }
    action_display = action_names.get(action, action)
    
    # Если это просто получить файл - отправляем сразу без конвертации
    if action == "video" or action == "file":
        await callback.answer("📹 Отправляю файл...", show_alert=False)
        try:
            bot_username = await get_bot_username()
            caption = f"@{bot_username}"
            
            # Определяем тип файла из базы данных
            file_type = None
            if parts[0] == "conv" and len(parts) >= 3:
                try:
                    cache_id_for_file = int(parts[2])
                    result = db.get_file_by_id(cache_id_for_file)
                    if result and len(result) >= 2:
                        file_type = result[1]
                except:
                    pass
            
            # Отправляем файл в зависимости от типа
            if file_type == 'video':
                await callback.message.answer_video(file_id, caption=caption, supports_streaming=True)
            elif file_type == 'audio':
                await callback.message.answer_audio(file_id, caption=caption)
            elif file_type == 'photo':
                await callback.message.answer_photo(file_id, caption=caption)
            else:
                # Пробуем разные варианты
                try:
                    await callback.message.answer_video(file_id, caption=caption, supports_streaming=True)
                except:
                    try:
                        await callback.message.answer_audio(file_id, caption=caption)
                    except:
                        await callback.message.answer_document(file_id, caption=caption)
            return
        except Exception as e:
            logger.error(f"Error sending file: {e}", exc_info=True)
            await callback.answer("❌ Ошибка при отправке файла", show_alert=True)
            return
    
    # Исправляем текст для расшифровки
    if action == "transcription":
        action_display_text = "расшифровку"
    else:
        action_display_text = action_display
    
    await callback.answer(f"Начинаю конвертацию в {action_display_text}...", show_alert=False)
    
    # Temporary message
    status_msg = await callback.message.answer(f"⏳ Конвертирую в {action_display_text}...")
    
    try:
        # Download file with retry logic
        file = await bot.get_file(file_id)
        file_path = file.file_path
        
        # Create temp dir
        task_id = str(uuid.uuid4())
        task_dir = os.path.join("downloads", task_id)
        os.makedirs(task_dir, exist_ok=True)
        
        # Download from Telegram
        # Определяем расширение файла из file_path или по типу файла из базы
        file_ext = os.path.splitext(file_path)[1] if file_path else ''
        
        # Если расширение не определено, пытаемся определить по типу из базы
        if not file_ext and 'file_type' in locals():
            if file_type == 'audio':
                file_ext = '.mp3'  # По умолчанию для аудио
            elif file_type == 'video':
                file_ext = '.mp4'  # По умолчанию для видео
            else:
                file_ext = '.mp4'  # По умолчанию
        
        # Если всё ещё не определено, используем .mp4
        if not file_ext:
            file_ext = '.mp4'
        
        local_input_path = os.path.join(task_dir, f"input{file_ext}")
        
        # Скачиваем файл напрямую без retry (Telegram API должен справиться)
        await status_msg.edit_text("⏳ Скачиваю файл...")
        try:
            # Скачиваем файл напрямую (используем стандартный таймаут aiogram)
            await bot.download_file(file_path, local_input_path)
            
            # Проверяем, что файл действительно скачался
            if not os.path.exists(local_input_path):
                raise Exception("Файл не был создан после скачивания")
            
            file_size = os.path.getsize(local_input_path)
            if file_size == 0:
                raise Exception("Скачанный файл пустой")
            
            logger.info(f"Successfully downloaded file: {local_input_path}, size: {file_size} bytes")
        except Exception as download_error:
            logger.error(f"Error downloading file: {download_error}", exc_info=True)
            await status_msg.edit_text(f"❌ Ошибка при скачивании файла: {str(download_error)[:200]}")
            if ENABLE_CLEANUP:
                await asyncio.to_thread(get_downloader().cleanup, task_dir)
            return
        
        output_file = None
        bot_username = await get_bot_username()
        caption = f"@{bot_username}"
        
        if action == "mp3":
            async with conversion_semaphore:
                output_file = await asyncio.to_thread(get_downloader().convert_to_mp3, local_input_path, task_dir)
            if output_file:
                await callback.message.answer_audio(
                    FSInputFile(output_file, filename=f"{bot_username}.mp3"), 
                    caption=caption
                )
                
        elif action == "voice":
            async with conversion_semaphore:
                output_file = await asyncio.to_thread(get_downloader().convert_to_voice, local_input_path, task_dir)
            if output_file:
                await callback.message.answer_voice(FSInputFile(output_file), caption=caption)
                
        elif action == "note":
            async with conversion_semaphore:
                output_file = await asyncio.to_thread(get_downloader().convert_to_video_note, local_input_path, task_dir)
            if output_file:
                await callback.message.answer_video_note(FSInputFile(output_file))
        
        elif action == "transcription":
            # Для расшифровки нужно сначала извлечь аудио, затем расшифровать
            await status_msg.edit_text("⏳ Извлекаю аудио...")
            temp_audio_path = os.path.join(task_dir, "audio.wav")
            
            # Извлекаем аудио из видео (с ограничением параллельных операций)
            async with conversion_semaphore:
                await asyncio.to_thread(
                    subprocess.run,
                    [
                        'ffmpeg', '-i', local_input_path, '-vn', '-acodec', 'pcm_s16le',
                        '-ar', '16000', '-ac', '1', '-y', temp_audio_path
                    ],
                    check=True,
                    capture_output=True
                )
            
            if not os.path.exists(temp_audio_path) or os.path.getsize(temp_audio_path) == 0:
                await status_msg.edit_text("❌ Не удалось извлечь аудио")
                if ENABLE_CLEANUP:
                    await asyncio.to_thread(get_downloader().cleanup, task_dir)
                return
            
            # Расшифровываем аудио
            await status_msg.edit_text("⏳ Расшифровываю аудио...")
            transcribed_text = await asyncio.to_thread(transcribe_audio_segments, temp_audio_path)
            
            if not transcribed_text or not transcribed_text.strip():
                await status_msg.edit_text("❌ Не удалось распознать речь")
                if ENABLE_CLEANUP:
                    await asyncio.to_thread(get_downloader().cleanup, task_dir)
                return
            
            # Сохраняем расшифровку в базу данных для последующего саммари
            # Используем cache_id как уникальный идентификатор для этого файла
            file_unique_id = f"conv_{cache_id}"
            user_id = callback.from_user.id
            db.save_transcription(file_unique_id, user_id, transcribed_text)
            
            # Создаем кнопку "саммари" для этой расшифровки
            summary_button = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="саммари", callback_data=f"summarize:{file_unique_id}")]
            ])
            
            # Отправляем расшифровку с кнопкой саммари
            if len(transcribed_text) > MAX_MESSAGE_LENGTH:
                # Разбиваем на части
                words = transcribed_text.split()
                current_message = ""
                messages = []
                
                for word in words:
                    if len(current_message + " " + word) <= MAX_MESSAGE_LENGTH:
                        current_message += (" " + word) if current_message else word
                    else:
                        if current_message:
                            messages.append(current_message)
                        current_message = word
                
                if current_message:
                    messages.append(current_message)
                
                # Отправляем все части, кнопку добавляем только к последней
                for i, msg_text in enumerate(messages):
                    if i == 0:
                        if len(messages) == 1:
                            # Если только одно сообщение, добавляем кнопку
                            await status_msg.edit_text(f"<b>📝 Расшифровка:</b>\n{msg_text}", parse_mode="HTML", reply_markup=summary_button)
                        else:
                            await status_msg.edit_text(f"<b>📝 Расшифровка:</b>\n{msg_text}", parse_mode="HTML")
                    elif i == len(messages) - 1:
                        # Последнее сообщение получает кнопку
                        await callback.message.answer(f"<b>📝 Расшифровка (продолжение):</b>\n{msg_text}", parse_mode="HTML", reply_markup=summary_button)
                    else:
                        await callback.message.answer(f"<b>📝 Расшифровка (продолжение):</b>\n{msg_text}", parse_mode="HTML")
            else:
                await status_msg.edit_text(f"<b>📝 Расшифровка:</b>\n{transcribed_text}", parse_mode="HTML", reply_markup=summary_button)
            
            if ENABLE_CLEANUP:
                await asyncio.to_thread(get_downloader().cleanup, task_dir)
            return
        
        elif action == "summary":
            # Для саммари нужно сначала извлечь аудио, затем расшифровать и сделать саммари
            await status_msg.edit_text("⏳ Извлекаю аудио...")
            temp_audio_path = os.path.join(task_dir, "audio.wav")
            
            # Извлекаем аудио из видео (с ограничением параллельных операций)
            async with conversion_semaphore:
                await asyncio.to_thread(
                    subprocess.run,
                    [
                        'ffmpeg', '-i', local_input_path, '-vn', '-acodec', 'pcm_s16le',
                        '-ar', '16000', '-ac', '1', '-y', temp_audio_path
                    ],
                    check=True,
                    capture_output=True
                )
            
            if not os.path.exists(temp_audio_path) or os.path.getsize(temp_audio_path) == 0:
                await status_msg.edit_text("❌ Не удалось извлечь аудио")
                if ENABLE_CLEANUP:
                    await asyncio.to_thread(get_downloader().cleanup, task_dir)
                return
            
            # Расшифровываем аудио (с ограничением параллельных операций)
            await status_msg.edit_text("⏳ Расшифровываю аудио...")
            async with transcription_semaphore:
                transcribed_text = await asyncio.to_thread(transcribe_audio_segments, temp_audio_path)
            
            if not transcribed_text or not transcribed_text.strip():
                await status_msg.edit_text("❌ Не удалось распознать речь")
                if ENABLE_CLEANUP:
                    await asyncio.to_thread(get_downloader().cleanup, task_dir)
                return
            
            # Создаем только саммари (без отправки расшифровки)
            await status_msg.edit_text("⏳ Создаю саммари...")
            summary = await generate_summary(transcribed_text)
            
            await status_msg.edit_text(f"📝 <b>Саммари:</b>\n\n{summary}", parse_mode="HTML")
            if ENABLE_CLEANUP:
                await asyncio.to_thread(get_downloader().cleanup, task_dir)
            return
        
        # Cleanup
        if ENABLE_CLEANUP:
            await asyncio.to_thread(get_downloader().cleanup, task_dir)
        await status_msg.delete()
        
    except Exception as e:
        logger.error(f"Conversion error: {e}", exc_info=True)
        await status_msg.edit_text(f"❌ Ошибка конвертации")
        # Cleanup on error
        if ENABLE_CLEANUP and 'task_dir' in locals():
             await asyncio.to_thread(get_downloader().cleanup, task_dir)

# ... rest of the code ...
# Ключ: URL (нормализованный), значение: Future с результатом (file_ids, file_type)
active_downloads = {}

# Track sent links to avoid duplicates (max 10000 entries, then clear)
# Ключ: (normalized_url, user_id)
sent_links = set()
MAX_SENT_LINKS = 10000

# Bot username (cached)
_bot_username = None

async def get_bot_username():
    """Получает username бота (кэшируется)"""
    global _bot_username
    if _bot_username is None:
        bot_info = await bot.get_me()
        _bot_username = bot_info.username
    return _bot_username

def get_cookies_file(url: str) -> str:
    """Определяет правильный файл cookies в зависимости от платформы.
    Файлы читаются каждый раз заново, без кэширования - можно обновлять без перезапуска бота."""
    base_dir = os.path.dirname(__file__)
    
    if 'instagram.com' in url:
        cookies_file = os.path.join(base_dir, 'ig_cookies.txt')
        if os.path.exists(cookies_file):
            # Логируем время модификации для отладки
            try:
                mtime = os.path.getmtime(cookies_file)
                logger.debug(f"Using Instagram cookies file (modified: {time.ctime(mtime)})")
            except:
                pass
            return cookies_file
    elif 'youtube.com' in url or 'youtu.be' in url:
        cookies_file = os.path.join(base_dir, 'yt_cookies.txt')
        if os.path.exists(cookies_file):
            # Логируем время модификации для отладки
            try:
                mtime = os.path.getmtime(cookies_file)
                logger.debug(f"Using YouTube cookies file (modified: {time.ctime(mtime)})")
            except:
                pass
            return cookies_file
    
    # Fallback на общий файл cookies
    cookies_file = os.path.join(base_dir, 'cookies.txt')
    if os.path.exists(cookies_file):
        try:
            mtime = os.path.getmtime(cookies_file)
            logger.debug(f"Using general cookies file (modified: {time.ctime(mtime)})")
        except:
            pass
        return cookies_file
    
    return None

async def expand_short_url(url: str) -> str:
    """Расшифровывает короткие ссылки (vt.tiktok.com и т.д.) в полные URL"""
    try:
        # Проверяем, является ли это короткой ссылкой
        is_short_url = 'vt.tiktok.com' in url and not any(pattern in url for pattern in ['/photo/', '/video/'])
        
        # Если это уже полная ссылка, возвращаем как есть
        if not is_short_url:
            return url
        
        # Используем yt-dlp для получения финального URL (быстро, только метаданные)
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'extract_flat': True,  # Только метаданные, быстрее
            'skip_download': True,
            'no_check_certificate': True,
        }
        
        if USE_PROXY and PROXY_URL:
            ydl_opts['proxy'] = PROXY_URL
        
        cookies_file = get_cookies_file(url)
        if cookies_file:
            ydl_opts['cookiefile'] = cookies_file
        
        # Выполняем в отдельном потоке, так как yt-dlp синхронный
        def _extract_url():
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=False)
                    if info:
                        # Пробуем разные варианты получения финального URL
                        if 'webpage_url' in info:
                            final_url = info['webpage_url']
                        elif 'url' in info:
                            final_url = info['url']
                        elif 'entries' in info and info['entries']:
                            # Для плейлистов/каруселей берем первый элемент
                            first_entry = info['entries'][0]
                            if isinstance(first_entry, dict):
                                final_url = first_entry.get('webpage_url') or first_entry.get('url')
                            else:
                                final_url = None
                        else:
                            final_url = None
                        
                        if final_url and final_url != url:
                            return final_url
            except Exception as e:
                logger.debug(f"yt-dlp extract_info failed for URL expansion: {e}")
            return None
        
        # Используем таймаут 2 секунды для расшифровки
        try:
            expanded_url = await asyncio.wait_for(asyncio.to_thread(_extract_url), timeout=2.0)
            if expanded_url and expanded_url != url:
                logger.info(f"Expanded URL: {url} -> {expanded_url}")
                return expanded_url
        except asyncio.TimeoutError:
            logger.debug(f"URL expansion timeout for {url}, using original")
        
        # Если не получилось расшифровать, возвращаем оригинальную ссылку
        logger.debug(f"Could not expand URL {url}, using original")
        return url
    except Exception as e:
        logger.error(f"Error expanding URL {url}: {e}")
        return url

# Subscription check functions
async def is_subscribed(user_id: int) -> bool:
    """Проверяет, подписан ли пользователь на канал"""
    try:
        member = await bot.get_chat_member(CHANNEL_ID, user_id)
        return member.status in ['member', 'administrator', 'creator']
    except Exception as e:
        logger.error(f"Error checking subscription: {e}")
        return False

def get_subscription_keyboard():
    """Создает клавиатуру с кнопкой подписки"""
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="подписаться", url=f"https://t.me/{CHANNEL_USERNAME}")]
    ])

@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    # Проверяем подписку
    if not await is_subscribed(message.from_user.id):
        await message.answer(
            "👋 привет! для использования бота нужно подписаться на канал:",
            reply_markup=get_subscription_keyboard()
        )
        return
    db.add_user(message.from_user)
    bot_username = await get_bot_username()
    
    # Проверяем, есть ли параметр start с cache_id
    # Параметр может быть в формате: /start file_123 или в ссылке ?start=file_123
    start_param = None
    if message.text:
        # Проверяем, есть ли параметр после /start
        parts = message.text.split()
        if len(parts) > 1:
            start_param = parts[1]
        else:
            # Проверяем, есть ли start= в ссылке
            if '?start=' in message.text or '&start=' in message.text:
                from urllib.parse import urlparse, parse_qs
                try:
                    parsed = urlparse(message.text)
                    query = parse_qs(parsed.query)
                    if 'start' in query:
                        start_param = query['start'][0]
                except:
                    pass
    
    if start_param and start_param.startswith("file_"):
        try:
            # Парсим cache_id - убираем префикс "file_"
            param_value = start_param[5:]
            # Убираем лишние символы, если есть
            param_value = param_value.split('/')[0].split('?')[0].split('&')[0].strip()
            cache_id = int(param_value)
            
            # Получаем file_ids и media_type из базы по cache_id
            result = db.get_file_by_id(cache_id)
            if not result:
                await message.answer("❌ Файл не найден или ссылка устарела.")
                return
            
            # get_file_by_id возвращает (file_ids, media_type)
            file_ids, media_type = result
            
            # Убеждаемся что file_ids это список
            if not isinstance(file_ids, list):
                file_ids = [file_ids]
            
            logger.info(f"[START] Found file by cache_id {cache_id}: {len(file_ids)} file(s), type: {media_type}")
            sys.stdout.flush()
            
            # Если один файл - показываем меню конвертации (как было раньше)
            if len(file_ids) == 1:
                # Отправляем кнопки выбора типа конвертации
                await message.answer(
                    "🎬 Выбери тип конвертации:",
                    reply_markup=get_convert_options_keyboard_with_cache_id(cache_id)
                )
                return
            else:
                # Карусель - отправляем файлы сразу БЕЗ меню конвертации
                # Получаем URL для подписи
                try:
                    db.cursor.execute("SELECT url FROM file_cache WHERE id = ?", (cache_id,))
                    url_result = db.cursor.fetchone()
                    file_url = url_result[0] if url_result else None
                except:
                    file_url = None
                
                caption = f"⚡ @{bot_username}\n🔗 {file_url}" if file_url else f"⚡ @{bot_username}"
                # Карусель - отправляем через media_group БЕЗ меню конвертации
                logger.info(f"[START] Sending carousel: {len(file_ids)} files, type: {media_type}")
                sys.stdout.flush()
                
                media_group = []
                for i, file_id in enumerate(file_ids):
                    media_caption = caption if i == 0 else None
                    # Определяем тип медиа для каждого файла
                    # Если media_type = 'carousel', пробуем определить по первому файлу
                    # Но обычно карусели Instagram - это фото
                    if media_type == 'video':
                        media_group.append(InputMediaVideo(media=file_id, caption=media_caption))
                    elif media_type == 'audio':
                        from aiogram.types import InputMediaAudio
                        media_group.append(InputMediaAudio(media=file_id, caption=media_caption))
                    elif media_type == 'carousel':
                        # carousel обычно состоит из фото, отправляем как фото
                        media_group.append(InputMediaPhoto(media=file_id, caption=media_caption))
                    else:
                        # photo - отправляем как фото
                        media_group.append(InputMediaPhoto(media=file_id, caption=media_caption))
                
                # Отправляем карусель chunks по 10 файлов
                chunk_size = 10
                sent_successfully = False
                for i in range(0, len(media_group), chunk_size):
                    chunk = media_group[i:i + chunk_size]
                    try:
                        await message.answer_media_group(chunk)
                        logger.info(f"[START] ✅ Sent carousel chunk {i//chunk_size + 1}/{len(range(0, len(media_group), chunk_size))}")
                        sys.stdout.flush()
                        sent_successfully = True
                    except Exception as e:
                        logger.error(f"[START] ❌ Media group chunk failed: {e}", exc_info=True)
                        sys.stdout.flush()
                
                if sent_successfully:
                    logger.info(f"[START] ✅ Successfully sent carousel with {len(file_ids)} files from cache_id {cache_id}")
                else:
                    logger.error(f"[START] ❌ Failed to send carousel")
                    await message.answer("❌ Ошибка при отправке карусели.")
                sys.stdout.flush()
                return
        except ValueError:
            logger.debug(f"Invalid cache_id in start parameter: {start_param}")
            # Продолжаем показывать приветственное сообщение
        except Exception as e:
            logger.error(f"Error handling start parameter: {e}", exc_info=True)
            # Продолжаем показывать приветственное сообщение
    
    # Обычное приветственное сообщение
    await message.answer(
        "👋 привет! отправь мне ссылки на Instagram, TikTok, YouTube или SoundCloud и я их скачаю!\n"
        "можно отправить несколько ссылок в одном сообщении!\n\n"
        "🎬 также я умею конвертировать:\n"
        "   + видео/аудио в видеокружок\n"
        "   + видео/аудио в голосовое сообщение\n"
        "   + видео в MP3\n"
        "   + голосовые/видеокружки в текст (расшифровка)\n"
        "   + видео/аудио в текст (расшифровка)\n"
        "   + создавать краткое содержание (саммари)\n\n"
        "📱 создаю qr-коды - используй команду /qr (текст)\n"
        "📷 расшифровываю qr-коды - отправь фото с qr-кодом\n\n"
        f"🔎 или используй в любом чате: @{bot_username} ссылка\n"
        f"🌐 веб-версия: https://downloader.dreampartners.online"
    )

@dp.message(Command("qr"))
async def cmd_qr(message: types.Message):
    """Handle /qr command to generate QR codes"""
    # Проверяем подписку
    if not await is_subscribed(message.from_user.id):
        await message.answer(
            "👋 для использования бота нужно подписаться на канал:",
            reply_markup=get_subscription_keyboard()
        )
        return
    
    try:
        # Extract text after /qr command
        command_text = message.text[4:].strip() if message.text else ""  # Remove '/qr ' prefix
        
        if not command_text:
            await message.answer("❌ укажите текст для qr-кода\n\nпример: /qr https://example.com")
            return
        
        if len(command_text) > 2000:
            await message.answer("❌ текст слишком длинный для qr-кода (максимум 2000 символов)")
            return
        
        # Generate QR code
        qr_buffer = generate_qr_code(command_text)
        
        # Send QR code as photo
        qr_file = BufferedInputFile(qr_buffer.getvalue(), filename="qr_code.png")
        await message.answer_photo(
            qr_file,
            caption=f"📱 qr-код для: {command_text[:100]}{'...' if len(command_text) > 100 else ''}"
        )
        
    except Exception as e:
        logger.error(f"Error in cmd_qr: {e}")
        await message.answer(f"❌ ошибка при создании qr-кода: {str(e)}")

async def send_link_to_user(user_id: int, url: str, normalized_url: str = None):
    """Отправляет ссылку пользователю в ЛС (только один раз для комбинации URL+user)"""
    global sent_links
    # Нормализуем URL для проверки дубликатов
    if normalized_url is None:
        normalized_url = normalize_url(url)
    
    # Очищаем кэш, если он слишком большой
    if len(sent_links) > MAX_SENT_LINKS:
        sent_links.clear()
        logger.info("Cleared sent_links cache")
    
    # Проверяем, не отправлялась ли уже ссылка этому пользователю
    link_key = (normalized_url, user_id)
    if link_key in sent_links:
        logger.debug(f"Link already sent to user {user_id} for {normalized_url}, skipping")
        return
    
    try:
        await bot.send_message(
            chat_id=user_id,
            text=f"🔗 Ссылка для скачивания:\n{url}",
            disable_notification=True
        )
        # Помечаем, что ссылка отправлена
        sent_links.add(link_key)
        logger.info(f"Sent link to user {user_id}: {url}")
    except Exception as e:
        logger.error(f"Error sending link to user {user_id}: {e}")


async def download_and_cache_inline(url: str, user_id: int, expanded_url: str = None):
    """Скачивает, загружает в TG и кэширует файл. Возвращает list of file_ids и тип."""
    # Используем переданный expanded_url (уже расшифрован) или url
    url_to_use = expanded_url if expanded_url else url
    # Убираем обратные слэши, если они есть
    if url_to_use:
        url_to_use = url_to_use.rstrip('\\')
    
    # Нормализуем URL для кэша (используем url_to_use, который уже может быть расшифрован)
    normalized_url = normalize_url(url_to_use)
    
    # Получаем username бота для подписи (ссылка будет добавлена позже)
    bot_username = await get_bot_username()
    
    # Проверяем, не скачивается ли уже
    if normalized_url in active_downloads:
        logger.info(f"Download already in progress for {normalized_url}, waiting for completion...")
        # Присоединяемся к существующей загрузке
        future = active_downloads[normalized_url]
        try:
            file_ids, file_type = await future
            return file_ids, file_type
        except Exception as e:
            logger.error(f"Error waiting for existing download: {e}")
            return None, None
    
    # Создаем Future для этой загрузки
    future = asyncio.Future()
    active_downloads[normalized_url] = future
    
    # Обновляем время последней активности
    global _last_activity_time
    _last_activity_time = time.time()
    
    # Initialize variables to prevent UnboundLocalError
    files = None
    task_dir = None
    
    try:
        # Скачиваем используя расшифрованную ссылку
        logger.info(f"[STEP 1/7] Starting download for {normalized_url} using URL: {url_to_use}")
        logger.info(f"[DOWNLOAD] Queuing get_downloader().download for user {user_id}")
        sys.stdout.flush()
        
        async with download_semaphore:
            logger.info(f"[DOWNLOAD] Calling get_downloader().download for user {user_id}")
            start_time = time.time()
            try:
                # Максимум 600 секунд на загрузку
                files, task_dir = await asyncio.wait_for(
                    asyncio.to_thread(get_downloader().download, url_to_use),
                    timeout=600.0
                )
                end_time = time.time()
                duration = end_time - start_time
                
                logger.info(f"[STEP 2/7] Download completed! Got {len(files) if files else 0} file(s), task_dir: {task_dir}")
                logger.info(f"[STATS] Download duration: {duration:.2f} seconds")
                sys.stdout.flush()
                
                if files:
                    total_size = sum(os.path.getsize(f) for f in files if os.path.exists(f))
                    size_mb = total_size / (1024 * 1024)
                    avg_speed = size_mb / duration if duration > 0 else 0
                    logger.info(f"[STATS] Total size: {size_mb:.2f} MB, Avg Speed: {avg_speed:.2f} MB/s")
                    
                    logger.info(f"[DOWNLOAD] File list: {files[:3]}..." if len(files) > 3 else f"[DOWNLOAD] File list: {files}")
                    sys.stdout.flush()
            except Exception as download_error:
                # Специальная обработка для _ProgressState ошибки
                if isinstance(download_error, NameError) and '_ProgressState' in str(download_error):
                    logger.warning(f"[WARNING] yt-dlp _ProgressState error caught and ignored for {normalized_url}: {download_error}")
                    # Продолжаем выполнение как будто все нормально
                    pass
                else:
                    logger.error(f"[ERROR] Exception during download: {download_error}", exc_info=True)
                    sys.stdout.flush()
                    raise
        
        if not files:
            logger.error(f"[ERROR] No files downloaded for {normalized_url}")
            future.set_result((None, None))
            return None, None
        
        files.sort()
        logger.info(f"[STEP 3/7] Downloaded {len(files)} file(s) for {normalized_url}, starting upload to user {user_id}")
        sys.stdout.flush()  # Принудительный flush после завершения скачивания
        
        # Фильтруем медиа файлы
        logger.info(f"[STEP 4/7] Filtering media files from {len(files)} downloaded files...")
        media_files = []
        # Для SoundCloud фильтруем - оставляем только аудио, обложки не отправляем отдельно
        is_soundcloud = 'soundcloud.com' in normalized_url
        
        for file_path in files:
            ext = os.path.splitext(file_path)[1].lower()
            # Для SoundCloud пропускаем обложки (они будут использоваться как thumbnail)
            if is_soundcloud and ext in ['.jpg', '.jpeg', '.png', '.webp']:
                logger.info(f"[MEDIA] Skipping thumbnail for SoundCloud: {file_path}")
                continue
            # Пропускаем только явно не медиа файлы, если нужно. 
            # Но yt-dlp обычно скачивает то, что нужно.
            # Поддерживаем популярные форматы
            if ext in ['.jpg', '.jpeg', '.png', '.webp', '.mp4', '.mov', '.avi', '.mkv', '.mp3', '.m4a', '.aac', '.ogg', '.wav', '.opus', '.flac']:
                media_files.append(file_path)
                logger.info(f"[MEDIA] Added media file: {file_path}")
        
        if not media_files:
            logger.error(f"[ERROR] No media files found after filtering for {normalized_url}")
            future.set_result((None, None))
            if ENABLE_CLEANUP:
                await asyncio.to_thread(get_downloader().cleanup, task_dir)
            return None, None
        
        logger.info(f"[STEP 5/7] Processing {len(media_files)} media file(s) for {normalized_url}")
        
        file_ids = []
        file_type = None
        
        # Определяем тип по первому файлу
        if len(media_files) == 1:
            ext = os.path.splitext(media_files[0])[1].lower()
            if ext in ['.jpg', '.jpeg', '.png', '.webp']:
                file_type = 'photo'
            elif ext in ['.mp3', '.m4a', '.aac', '.ogg', '.wav', '.opus', '.flac']:
                file_type = 'audio'
            else:
                file_type = 'video'
        else:
            # Карусель - отправляем массивом
            ext = os.path.splitext(media_files[0])[1].lower()
            if ext in ['.jpg', '.jpeg', '.png', '.webp']:
                file_type = 'photo'
            elif ext in ['.mp3', '.m4a', '.aac', '.ogg', '.wav', '.opus', '.flac']:
                file_type = 'audio'
            else:
                file_type = 'video'
        
        # Создаем подпись со ссылкой
        logger.info(f"[STEP 6/7] Preparing to send files to user {user_id}...")
        bot_username = await get_bot_username()
        # Используем нормализованную ссылку в подписи для чистоты
        # Молния только для кэша, здесь свежая загрузка
        caption = f"@{bot_username}\n🔗 {normalized_url}"
        
        # Отправляем как media_group для карусели или одиночным файлом
        video_sent_msg = None  # Сохраняем сообщение с видео для добавления кнопки
        if len(media_files) == 1:
            # Один файл
            file_path = media_files[0]
            logger.info(f"[UPLOAD] Single file mode: {file_type}, file: {file_path}")
            sent_msg = None
            
            try:
                if file_type == 'photo':
                    logger.info(f"[UPLOAD] Sending photo to user {user_id}: {file_path}")
                    sent_msg = await bot.send_photo(chat_id=user_id, photo=FSInputFile(file_path), caption=caption, disable_notification=True)
                    logger.info(f"[UPLOAD] Photo send API call completed, response: {sent_msg}")
                    if sent_msg and sent_msg.photo:
                        file_ids.append(sent_msg.photo[-1].file_id)
                        logger.info(f"[SUCCESS] Photo sent successfully to user {user_id}, file_id: {sent_msg.photo[-1].file_id}")
                        # Умная очистка: удаляем файл после получения file_id
                        try:
                            if os.path.exists(file_path):
                                os.remove(file_path)
                                logger.info(f"[CLEANUP] 🗑️ Cleaned up photo file after Telegram upload: {os.path.basename(file_path)}")
                        except Exception as cleanup_error:
                            logger.warning(f"[CLEANUP] Failed to cleanup photo file {file_path}: {cleanup_error}")
                    else:
                        logger.error(f"[ERROR] Photo send returned invalid response: {sent_msg}")
                elif file_type == 'audio':
                    logger.info(f"[UPLOAD] Sending audio to user {user_id}: {file_path}")
                    # Для SoundCloud пытаемся загрузить метаданные и обложку
                    metadata = None
                    thumbnail_path = None
                    task_dir = os.path.dirname(file_path)
                    metadata_file = os.path.join(task_dir, 'metadata.json')
                    if os.path.exists(metadata_file):
                        try:
                            with open(metadata_file, 'r', encoding='utf-8') as f:
                                metadata = json.load(f)
                            # Ищем обложку в папке
                            for thumb_file in os.listdir(task_dir):
                                if thumb_file.endswith(('.jpg', '.jpeg', '.png', '.webp')) and thumb_file != os.path.basename(file_path):
                                    thumbnail_path = os.path.join(task_dir, thumb_file)
                                    break
                        except Exception as e:
                            logger.warning(f"Failed to load metadata: {e}")
                    
                    # Для SoundCloud отправляем обложку отдельным сообщением перед аудио
                    cover_file_id = None
                    if is_soundcloud and thumbnail_path:
                        try:
                            logger.info(f"[UPLOAD] Sending SoundCloud cover art to user {user_id}: {thumbnail_path}")
                            cover_msg = await bot.send_photo(chat_id=user_id, photo=FSInputFile(thumbnail_path), caption=caption, disable_notification=True)
                            if cover_msg and cover_msg.photo:
                                cover_file_id = cover_msg.photo[-1].file_id
                                logger.info(f"[SUCCESS] Cover art sent successfully to user {user_id}")
                                # НЕ добавляем обложку в file_ids - она не нужна в кэше для инлайна
                        except Exception as e:
                            logger.warning(f"Failed to send cover art: {e}")
                    
                    audio_kwargs = {'caption': caption, 'disable_notification': True}
                    if metadata:
                        audio_kwargs['title'] = metadata.get('title', 'Track')
                        audio_kwargs['performer'] = metadata.get('uploader', 'Unknown')
                    if thumbnail_path:
                        audio_kwargs['thumbnail'] = FSInputFile(thumbnail_path)
                    
                    sent_msg = await bot.send_audio(chat_id=user_id, audio=FSInputFile(file_path), **audio_kwargs)
                    if sent_msg and sent_msg.audio:
                         # Добавляем только аудио file_id в кэш (обложка отправляется отдельно, но не кэшируется)
                         file_ids.append(sent_msg.audio.file_id)
                         # Умная очистка: удаляем файл после получения file_id
                         try:
                             if os.path.exists(file_path):
                                 os.remove(file_path)
                                 logger.info(f"[CLEANUP] 🗑️ Cleaned up audio file after Telegram upload: {os.path.basename(file_path)}")
                         except Exception as cleanup_error:
                             logger.warning(f"[CLEANUP] Failed to cleanup audio file {file_path}: {cleanup_error}")
                else:
                    # Получаем размер файла для логирования
                    file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
                    file_size_mb = file_size / (1024 * 1024)
                    logger.info(f"[UPLOAD] Sending video to user {user_id}: {file_path}")
                    logger.info(f"[UPLOAD] Video file size: {file_size_mb:.2f} MB ({file_size} bytes)")
                    logger.info(f"[UPLOAD] Calling bot.send_video() API...")
                    sys.stdout.flush()  # Принудительный flush
                    
                    # Проверяем, нужно ли оптимизировать видео для Telegram (как в обычных сообщениях)
                    needs_opt, opt_reason = await asyncio.to_thread(get_downloader().needs_telegram_optimization, file_path)
                    optimized_path = file_path
                    
                    if needs_opt:
                        logger.info(f"[UPLOAD] Video needs optimization: {opt_reason}")
                        logger.info(f"[UPLOAD] Optimizing video for Telegram...")
                        sys.stdout.flush()
                        
                        # Оптимизируем видео с ограничением параллельных операций (только 1 одновременно)
                        async with optimization_semaphore:
                            optimized_path = await asyncio.to_thread(
                                get_downloader().optimize_for_telegram, 
                                file_path, 
                                task_dir,
                                fast_mode=True
                            )
                        
                        if optimized_path and os.path.exists(optimized_path):
                            logger.info(f"[UPLOAD] ✅ Video optimized: {os.path.basename(optimized_path)}")
                            file_path = optimized_path
                        else:
                            logger.warning(f"[UPLOAD] ⚠️ Optimization failed, using original")
                    
                    # Генерируем thumbnail для ВСЕХ видео (гарантируем превью)
                    thumbnail_path = None
                    final_file_size = os.path.getsize(file_path)
                    logger.info(f"[UPLOAD] Generating thumbnail for video ({final_file_size/1024/1024:.2f}MB)...")
                    try:
                        # Генерируем обложку на 1-й секунде (или 0.0, если видео короткое)
                        thumbnail_path = await asyncio.to_thread(
                            get_downloader().generate_thumbnail,
                            file_path,
                            task_dir,
                            1.0  # time_offset
                        )
                        if thumbnail_path:
                            logger.info(f"[UPLOAD] ✅ Thumbnail generated: {os.path.basename(thumbnail_path)}")
                        else:
                            logger.warning(f"[UPLOAD] ⚠️ Thumbnail generation returned None")
                    except Exception as thumb_error:
                        logger.warning(f"[UPLOAD] Failed to generate thumbnail: {thumb_error}")
                        thumbnail_path = None
                    
                    try:
                        # ПОЛУЧАЕМ МЕТАДАННЫЕ ВИДЕО (как в обычных сообщениях)
                        video_info = await asyncio.to_thread(get_downloader().get_video_info, file_path)
                        
                        # Сначала отправляем без кнопки, потом получим cache_id и добавим кнопку
                        video_kwargs = {
                            'video': FSInputFile(file_path), 
                            'caption': caption, 
                            'disable_notification': True, 
                            'supports_streaming': True,
                            # Передаем точные размеры и длительность (как в обычных сообщениях)!
                            'width': video_info['width'] if video_info else None,
                            'height': video_info['height'] if video_info else None,
                            'duration': video_info['duration'] if video_info else None
                        }
                        if thumbnail_path and os.path.exists(thumbnail_path):
                            video_kwargs['thumbnail'] = FSInputFile(thumbnail_path)
                            logger.info(f"[UPLOAD] Sending video with thumbnail: {os.path.basename(thumbnail_path)}")
                        sent_msg = await bot.send_video(chat_id=user_id, **video_kwargs)
                        sys.stdout.flush()  # Принудительный flush после отправки
                        logger.info(f"[UPLOAD] Video send API call completed, response received: {sent_msg is not None}")
                        if sent_msg:
                            logger.info(f"[UPLOAD] Response type: {type(sent_msg)}, has video: {hasattr(sent_msg, 'video')}")
                        sys.stdout.flush()
                        
                        if sent_msg and sent_msg.video:
                            file_id = sent_msg.video.file_id
                            file_ids.append(file_id)
                            video_sent_msg = sent_msg  # Сохраняем для добавления кнопки после сохранения в кэш
                            logger.info(f"[SUCCESS] Video sent successfully to user {user_id}, file_id: {sent_msg.video.file_id}")
                            sys.stdout.flush()
                            # Умная очистка: удаляем файл после получения file_id
                            try:
                                if os.path.exists(file_path):
                                    os.remove(file_path)
                                    logger.info(f"[CLEANUP] 🗑️ Cleaned up file after Telegram upload: {os.path.basename(file_path)}")
                            except Exception as cleanup_error:
                                logger.warning(f"[CLEANUP] Failed to cleanup file {file_path}: {cleanup_error}")
                        else:
                            logger.error(f"[ERROR] Video send returned invalid response: {sent_msg}")
                            sys.stdout.flush()
                    except Exception as send_error:
                        logger.error(f"[ERROR] Exception during video send: {send_error}", exc_info=True)
                        sys.stdout.flush()
                        raise
            except Exception as e:
                logger.error(f"[ERROR] Exception uploading single file {file_path} to user {user_id}: {e}", exc_info=True)
                import traceback
                logger.error(f"[ERROR] Traceback: {traceback.format_exc()}")
        else:
            # Карусель - отправляем массивом
            logger.info(f"[UPLOAD] Carousel mode: {len(media_files)} files, type: {file_type}")
            logger.info(f"[UPLOAD] Preparing media group with {len(media_files)} files for user {user_id}")
            # Создаем подпись со ссылкой для первого файла
            bot_username = await get_bot_username()
            # Молния только для кэша
            caption_with_link = f"@{bot_username}\n🔗 {normalized_url}"
            
            media_group = []
            for i, file_path in enumerate(media_files):
                ext = os.path.splitext(file_path)[1].lower()
                # Подпись только к первому файлу
                media_caption = caption_with_link if i == 0 else None
                if ext in ['.jpg', '.jpeg', '.png', '.webp']:
                    media_group.append(InputMediaPhoto(media=FSInputFile(file_path), caption=media_caption))
                    logger.info(f"[MEDIA_GROUP] Added photo {i+1}/{len(media_files)}: {file_path}")
                elif ext in ['.mp4', '.mov', '.avi', '.mkv']:
                    media_group.append(InputMediaVideo(media=FSInputFile(file_path), caption=media_caption))
                    logger.info(f"[MEDIA_GROUP] Added video {i+1}/{len(media_files)}: {file_path}")
            
            if not media_group:
                logger.error(f"[ERROR] Media group is empty after preparing for {normalized_url}")
                future.set_result((None, None))
                if ENABLE_CLEANUP:
                    await asyncio.to_thread(get_downloader().cleanup, task_dir)
                return None, None
            
            logger.info(f"[UPLOAD] Starting to send {len(media_group)} media items to user {user_id}")
            
            # Отправляем chunks по 10
            sent_messages_all = []
            chunk_size = 10
            total_chunks = (len(media_group) + chunk_size - 1) // chunk_size
            for i in range(0, len(media_group), chunk_size):
                chunk = media_group[i:i + chunk_size]
                chunk_num = i//chunk_size + 1
                try:
                    logger.info(f"[UPLOAD] Sending media group chunk {chunk_num}/{total_chunks} ({len(chunk)} items) to user {user_id}")
                    sent_messages_chunk = await bot.send_media_group(chat_id=user_id, media=chunk, disable_notification=True)
                    logger.info(f"[UPLOAD] Media group chunk {chunk_num} send API call completed, got {len(sent_messages_chunk) if sent_messages_chunk else 0} messages")
                    sent_messages_all.extend(sent_messages_chunk)
                    logger.info(f"[SUCCESS] Media group chunk {chunk_num}/{total_chunks} sent successfully, {len(sent_messages_chunk)} messages")
                except Exception as e:
                    logger.error(f"[ERROR] Exception sending media group chunk {chunk_num} to user {user_id}: {e}", exc_info=True)
                    import traceback
                    logger.error(f"[ERROR] Traceback: {traceback.format_exc()}")
            
            logger.info(f"[EXTRACT] Extracting file_ids from {len(sent_messages_all)} sent messages")
            
            # Извлекаем file_id из всех сообщений и очищаем файлы после получения file_id
            for idx, sent_msg in enumerate(sent_messages_all):
                if sent_msg.photo:
                    file_ids.append(sent_msg.photo[-1].file_id)
                    logger.info(f"[EXTRACT] Extracted photo file_id {idx+1}/{len(sent_messages_all)}: {sent_msg.photo[-1].file_id}")
                    # Умная очистка: удаляем файл после получения file_id
                    if idx < len(media_files):
                        file_path = media_files[idx]
                        try:
                            if os.path.exists(file_path):
                                os.remove(file_path)
                                logger.info(f"[CLEANUP] 🗑️ Cleaned up carousel photo after Telegram upload: {os.path.basename(file_path)}")
                        except Exception as cleanup_error:
                            logger.warning(f"[CLEANUP] Failed to cleanup carousel photo {file_path}: {cleanup_error}")
                elif sent_msg.video:
                    file_ids.append(sent_msg.video.file_id)
                    logger.info(f"[EXTRACT] Extracted video file_id {idx+1}/{len(sent_messages_all)}: {sent_msg.video.file_id}")
                    # Умная очистка: удаляем файл после получения file_id
                    if idx < len(media_files):
                        file_path = media_files[idx]
                        try:
                            if os.path.exists(file_path):
                                os.remove(file_path)
                                logger.info(f"[CLEANUP] 🗑️ Cleaned up carousel video after Telegram upload: {os.path.basename(file_path)}")
                        except Exception as cleanup_error:
                            logger.warning(f"[CLEANUP] Failed to cleanup carousel video {file_path}: {cleanup_error}")
                else:
                    logger.warning(f"[WARNING] Message {idx+1} has no photo or video: {sent_msg}")
            
            logger.info(f"[SUCCESS] Extracted {len(file_ids)} file_id(s) from {len(sent_messages_all)} sent messages")
        
        # Сохраняем в кэш (один file_id или массив)
        logger.info(f"[STEP 7/7] Saving to cache: {len(file_ids)} file_id(s), type: {file_type}")
        # Используем нормализованный URL для кэша
        cache_id = None
        if file_ids:
            cache_id = db.save_file_to_cache(normalized_url, file_ids, file_type, user_id)
            logger.info(f"[CACHE] Cached {len(file_ids)} file(s) ({file_type}) for {normalized_url}, cache_id: {cache_id}")
            
            # Добавляем кнопку конвертировать для видео и аудио после сохранения в кэш
            if cache_id and video_sent_msg and (file_type == 'video' or file_type == 'audio'):
                log_resource_usage(f"Adding convert button to {file_type} message (cache_id={cache_id})")
                try:
                    await bot.edit_message_reply_markup(
                        chat_id=user_id,
                        message_id=video_sent_msg.message_id,
                        reply_markup=get_convert_keyboard(cache_id=cache_id, bot_username=bot_username)
                    )
                    logger.info(f"[BUTTON] ✅ Added convert button with cache_id={cache_id} to {file_type} message")
                except Exception as e:
                    logger.error(f"[BUTTON] ❌ Failed to add button to {file_type} message: {e}")
                log_resource_usage(f"After adding convert button to {file_type} message")
                # Очистка памяти после добавления кнопки
                unload_heavy_modules()
        else:
            logger.warning(f"[WARNING] No file_ids to cache for {normalized_url} - files were NOT sent to user!")
        
        # Агрессивная очистка памяти после завершения inline обработки
        unload_heavy_modules()
        
        # Устанавливаем результат в Future СРАЗУ (для других ожидающих запросов)
        # Это позволяет другим запросам получить результат как можно быстрее
        result = (file_ids, file_type)
        if not future.done():
            future.set_result(result)
            logger.info(f"[FUTURE] Future completed for {normalized_url}, files sent to user {user_id}")
        else:
            logger.warning(f"[WARNING] Future already done for {normalized_url}")
        
        logger.info(f"[COMPLETE] Successfully completed download and caching for {normalized_url}: {len(file_ids) if file_ids else 0} file(s) sent to user {user_id}")
        sys.stdout.flush()  # Важный flush перед завершением функции
        
        # Очистка файлов в фоне после всего (не блокируем отправку)
        if ENABLE_CLEANUP:
            async def _cleanup_background():
                try:
                    logger.info(f"[CLEANUP] Starting cleanup for task_dir: {task_dir}")
                    await asyncio.to_thread(get_downloader().cleanup, task_dir)
                    logger.info(f"[CLEANUP] Cleanup completed for task_dir: {task_dir}")
                except Exception as e:
                    logger.error(f"[ERROR] Error during cleanup: {e}", exc_info=True)
            
            # Запускаем очистку в фоне, не ждем
            cleanup_task = asyncio.create_task(_cleanup_background())
            logger.info(f"[CLEANUP] Cleanup task created (will run in background)")
        
        logger.info(f"[RETURN] Returning from download_and_cache_inline: {len(file_ids) if file_ids else 0} file_ids")
        sys.stdout.flush()  # Финальный flush перед возвратом
        return file_ids, file_type
        
    except asyncio.CancelledError:
        logger.warning(f"[CANCEL] Download cancelled for {url_to_use}")
        if not future.done():
            future.cancel()
        
        # Очистка при отмене
        if ENABLE_CLEANUP:
            try:
                if 'task_dir' in locals() and task_dir:
                    logger.info(f"[CLEANUP] Cleaning up task_dir after cancellation: {task_dir}")
                    await asyncio.to_thread(get_downloader().cleanup, task_dir)
            except Exception as cleanup_error:
                logger.error(f"[ERROR] Cleanup error: {cleanup_error}")
        raise
        
    except Exception as e:
        logger.error(f"[ERROR] Download and cache error for {url_to_use}: {e}", exc_info=True)
        import traceback
        logger.error(f"[ERROR] Full traceback: {traceback.format_exc()}")
        result = (None, None)
        if not future.done():
            future.set_result(result)
            logger.info(f"[FUTURE] Future completed with error for {normalized_url}")
        else:
            logger.warning(f"[WARNING] Future already done (error case) for {normalized_url}")
        
        # Очистка при ошибке
        if ENABLE_CLEANUP:
            try:
                if 'task_dir' in locals() and task_dir:
                    logger.info(f"[CLEANUP] Cleaning up task_dir: {task_dir}")
                    await asyncio.to_thread(get_downloader().cleanup, task_dir)
            except Exception as cleanup_error:
                logger.error(f"[ERROR] Cleanup error: {cleanup_error}")
        
        return None, None
    finally:
        # Удаляем Future из активных загрузок после завершения
        if normalized_url in active_downloads:
            if future.done():
                active_downloads.pop(normalized_url, None)
                logger.info(f"Removed Future from active_downloads for {normalized_url}")


@dp.inline_query()
async def inline_handler(query: types.InlineQuery):
    # Проверяем подписку
    if not await is_subscribed(query.from_user.id):
        from aiogram.types import InlineQueryResultArticle, InputTextMessageContent
        results = [
            InlineQueryResultArticle(
                id=str(uuid.uuid4()),
                title='Подпишитесь на канал',
                description='Для использования бота необходимо подписаться на канал',
                input_message_content=InputTextMessageContent(
                    message_text=f'Подпишитесь на канал @{CHANNEL_USERNAME} для использования бота'
                )
            )
        ]
        await query.answer(results, cache_time=1, is_personal=True)
        return
    # Регистрация юзера (даже через инлайн)
    db.add_user(query.from_user)
    
    text = query.query.strip()
    
    # Check if it's a QR code request
    if text.lower().startswith('qr '):
        qr_text = text[3:].strip()  # Remove 'qr ' prefix
        
        if qr_text and len(qr_text) <= 2000:
            try:
                # Generate QR code
                qr_buffer = generate_qr_code(qr_text)
                
                # Send QR code to user first to get file_id
                qr_file = BufferedInputFile(qr_buffer.getvalue(), filename="qr_code.png")
                sent_photo = await bot.send_photo(query.from_user.id, qr_file)
                
                # Create inline result with cached photo
                results = [
                    InlineQueryResultCachedPhoto(
                        id='qr_result',
                        photo_file_id=sent_photo.photo[-1].file_id,
                        title='📱 qr-код',
                        description=f'qr-код для: {qr_text[:50]}{"..." if len(qr_text) > 50 else ""}',
                        caption=f'📱 qr-код для: {qr_text}'
                    )
                ]
                
                await query.answer(results, cache_time=1, is_personal=True)
                return
                
            except Exception as e:
                logger.error(f"Error generating QR code in inline: {e}")
                results = [
                    InlineQueryResultArticle(
                        id='qr_error',
                        title='❌ ошибка qr-кода',
                        description=f'не удалось создать qr-код: {str(e)}',
                        input_message_content=InputTextMessageContent(
                            message_text=f'❌ ошибка при создании qr-кода: {str(e)}'
                        )
                    )
                ]
                await query.answer(results, cache_time=1, is_personal=True)
                return
        else:
            results = [
                InlineQueryResultArticle(
                    id='qr_invalid',
                    title='❌ неверный запрос qr',
                    description='укажите текст для qr-кода после "qr "',
                    input_message_content=InputTextMessageContent(
                        message_text='❌ укажите текст для qr-кода\nпример: @bot_username qr https://example.com'
                    )
                )
            ]
            await query.answer(results, cache_time=1, is_personal=True)
            return
    
    # Проверяем, является ли текст file_id
    if len(text) > 20 and (text.startswith('BAAC') or text.startswith('CAA') or 
                           text.startswith('AgAC') or text.startswith('BQAC') or
                           text.startswith('AwAC') or '_' in text or '-' in text):
        try:
            # Пробуем получить файл по file_id
            file = await bot.get_file(text)
            if file:
                # Это валидный file_id, показываем файл
                file_info = await bot.get_file(text)
                
                # Определяем тип файла по file_id префиксу или расширению
                file_id = text
                results = []
                result_id = str(uuid.uuid4())
                bot_username = await get_bot_username()
                caption = f"📥 Файл из Telegram\n⚡ @{bot_username}"
                
                # Пробуем определить тип по префиксу file_id
                if file_id.startswith('BAAC') or file_id.startswith('CAA'):
                    # Видео
                    results.append(InlineQueryResultCachedVideo(
                        id=result_id,
                        video_file_id=file_id,
                        title="Видео из Telegram",
                        description=caption
                    ))
                elif file_id.startswith('AwAC'):
                    # Аудио
                    results.append(InlineQueryResultCachedAudio(
                        id=result_id,
                        audio_file_id=file_id,
                        caption=caption
                    ))
                elif file_id.startswith('AgAC') or file_id.startswith('BQAC'):
                    # Фото
                    results.append(InlineQueryResultCachedPhoto(
                        id=result_id,
                        photo_file_id=file_id,
                        title="Фото из Telegram",
                        description=caption
                    ))
                else:
                    # Пробуем как документ или показываем как есть
                    results.append(InlineQueryResultArticle(
                        id=result_id,
                        title="Файл из Telegram",
                        description=f"File ID: {file_id[:50]}...",
                        input_message_content=InputTextMessageContent(
                            message_text=f"📥 Файл из Telegram\n\nFile ID: `{file_id}`\n\nИспользуйте этот file_id для отправки файла.",
                            parse_mode="Markdown"
                        )
                    ))
                
                await query.answer(results, cache_time=1)
                return
        except Exception as e:
            # Если не получилось - это не file_id, продолжаем обработку как обычного текста
            logger.debug(f"Text is not a valid file_id in inline: {e}")
    
    urls = re.findall(URL_PATTERN, text)
    
    if not urls:
        # Пустой результат, если нет ссылок
        return

    url = urls[0]
    
    # Проверяем, поддерживается ли ссылка
    if not is_supported_url(url):
        # Не показываем результат для неподдерживаемых ссылок
        return
    # Расшифровываем короткую ссылку
    expanded_url = await expand_short_url(url)
    # Убираем обратные слэши, если они есть
    expanded_url = expanded_url.rstrip('\\')
    # Используем расшифрованную ссылку для кэша
    normalized_url = normalize_url(expanded_url)
    result_id = str(uuid.uuid4())
    bot_username = await get_bot_username()
    
    # 1. Проверяем кэш (используем нормализованный расшифрованный URL)
    cached = db.get_cached_file(normalized_url)
    
    # Если не нашли в кэше, пробуем поискать по оригинальной ссылке (для совместимости со старым кэшем)
    if not cached:
        original_normalized = normalize_url(url)
        if original_normalized != normalized_url:
            cached = db.get_cached_file(original_normalized)
            if cached:
                # Нашли в кэше по старому ключу, обновляем на новый
                logger.info(f"Found in cache by old key, updating to new key")
                file_ids_str, media_type = cached
                db.save_file_to_cache(normalized_url, file_ids_str if isinstance(file_ids_str, list) else json.loads(file_ids_str) if file_ids_str.startswith('[') else [file_ids_str], media_type, query.from_user.id)
    
    if cached:
        file_ids_str, media_type = cached
        # Парсим file_ids (может быть строка или JSON)
        if isinstance(file_ids_str, list):
            file_ids = file_ids_str
        else:
            file_ids = json.loads(file_ids_str) if file_ids_str.startswith('[') else [file_ids_str]
        
        caption = f"⚡ @{bot_username}"
        results = []
        
        # Получаем cache_id по URL для файлов из кэша
        cache_id = get_cache_id_for_url(normalized_url)
        
        # Если один файл
        if len(file_ids) == 1:
            file_id = file_ids[0]
            if media_type == 'video':
                results.append(InlineQueryResultCachedVideo(
                    id=result_id,
                    video_file_id=file_id,
                    title="Видео",
                    description=caption,
                    reply_markup=get_convert_keyboard(cache_id=cache_id, bot_username=bot_username) if cache_id else None
                ))
            elif media_type == 'audio':
                results.append(InlineQueryResultCachedAudio(
                    id=result_id,
                    audio_file_id=file_id,
                    caption=caption
                ))
            else:
                results.append(InlineQueryResultCachedPhoto(
                    id=result_id,
                    photo_file_id=file_id,
                    title="Фото",
                    description=caption
                ))
        else:
            # Карусель - возвращаем все фотки/видео
            for i, file_id in enumerate(file_ids):
                if media_type == 'video':
                    # Для видео-карусели используем CachedVideo
                    results.append(InlineQueryResultCachedVideo(
                        id=f"{result_id}_{i}",
                        video_file_id=file_id,
                        title=f"Видео {i+1}",
                        description=caption if i == 0 else None,
                        reply_markup=get_convert_keyboard(cache_id=cache_id, bot_username=bot_username) if cache_id and i == 0 else None
                    ))
                elif media_type == 'audio':
                    results.append(InlineQueryResultCachedAudio(
                        id=f"{result_id}_{i}",
                        audio_file_id=file_id,
                        caption=caption if i == 0 else None
                    ))
                else:
                    # Для фото-карусели используем CachedPhoto
                    results.append(InlineQueryResultCachedPhoto(
                        id=f"{result_id}_{i}",
                        photo_file_id=file_id,
                        title=f"Фото {i+1}",
                        description=caption if i == 0 else None
                    ))
        
        await query.answer(results, cache_time=300, is_personal=False)
        return

    # 2. Проверяем, не идет ли уже загрузка
    if normalized_url in active_downloads:
        future = active_downloads[normalized_url]
        logger.info(f"Download in progress for {normalized_url}, waiting for completion...")
        # Ждем завершения загрузки с таймаутом 8 секунд (из 10 доступных)
        try:
            file_ids, file_type = await asyncio.wait_for(future, timeout=8.0)
            logger.info(f"Download completed for {normalized_url}, got {len(file_ids) if file_ids else 0} file(s)")
        except asyncio.TimeoutError:
            logger.info(f"Timeout waiting for download, responding with empty result")
            # Отвечаем сразу, загрузка продолжится в фоне
            await query.answer([], cache_time=1, is_personal=True)
            return
        except Exception as e:
            logger.error(f"Error waiting for download: {e}")
            file_ids, file_type = None, None
        
        # Проверяем кэш после завершения загрузки
        if file_ids:
            cached = db.get_cached_file(normalized_url)
            if cached:
                file_ids_str, media_type = cached
                if isinstance(file_ids_str, list):
                    file_ids = file_ids_str
                else:
                    file_ids = json.loads(file_ids_str) if file_ids_str.startswith('[') else [file_ids_str]
                
                caption = f"@{bot_username}"
                results = []
                
                # Получаем cache_id по URL для файлов из кэша
                cache_id = get_cache_id_for_url(normalized_url)
                
                if len(file_ids) == 1:
                    file_id = file_ids[0]
                    if media_type == 'video':
                        results.append(InlineQueryResultCachedVideo(
                            id=result_id,
                            video_file_id=file_id,
                            title="Видео",
                            description=caption,
                            reply_markup=get_convert_keyboard(cache_id=cache_id, bot_username=bot_username) if cache_id else None
                        ))
                    else:
                        results.append(InlineQueryResultCachedPhoto(
                            id=result_id,
                            photo_file_id=file_id,
                            title="Фото",
                            description=caption
                        ))
                else:
                    for i, file_id in enumerate(file_ids):
                        if media_type == 'video':
                            results.append(InlineQueryResultCachedVideo(
                                id=f"{result_id}_{i}",
                                video_file_id=file_id,
                                title=f"Видео {i+1}",
                                description=caption if i == 0 else None,
                                reply_markup=get_convert_keyboard(cache_id=cache_id, bot_username=bot_username) if cache_id and i == 0 else None
                            ))
                        else:
                            results.append(InlineQueryResultCachedPhoto(
                                id=f"{result_id}_{i}",
                                photo_file_id=file_id,
                                title=f"Фото {i+1}",
                                description=caption if i == 0 else None
                            ))
                
                await query.answer(results, cache_time=300, is_personal=False)
                return
    
    # 3. Если нет в кэше и не идет загрузка - пытаемся скачать быстро
    logger.info(f"[INLINE] Starting download for {normalized_url}, user {query.from_user.id}")
    
    # Запускаем скачивание и ждем до 8 секунд
    download_task = asyncio.create_task(download_and_cache_inline(expanded_url, query.from_user.id, expanded_url=expanded_url))
    logger.info(f"[INLINE] Download task created, waiting up to 8 seconds...")
    
    # Добавляем callback для логирования завершения задачи даже после таймаута
    def _task_done_callback(task):
        try:
            logger.info(f"[BACKGROUND_TASK] Task callback triggered for user {query.from_user.id}")
            sys.stdout.flush()
            
            if task.cancelled():
                logger.warning(f"[BACKGROUND_TASK] Task was cancelled for user {query.from_user.id}")
                sys.stdout.flush()
                return
            
            if task.exception():
                exc = task.exception()
                logger.error(f"[BACKGROUND_TASK] Background download task failed: {exc}", exc_info=exc)
                sys.stdout.flush()
            else:
                result = task.result()
                file_ids, file_type = result if result else (None, None)
                logger.info(f"[BACKGROUND_TASK] Task result received: {len(file_ids) if file_ids else 0} file_ids, type: {file_type}")
                sys.stdout.flush()
                
                if file_ids:
                    logger.info(f"[BACKGROUND_TASK] ✅ Background download completed successfully: {len(file_ids)} file(s) ({file_type}) sent to user {query.from_user.id}")
                else:
                    logger.warning(f"[BACKGROUND_TASK] ⚠️ Background download completed but no files were sent to user {query.from_user.id}")
                sys.stdout.flush()
        except Exception as e:
            logger.error(f"[BACKGROUND_TASK] Error in task callback: {e}", exc_info=True)
            sys.stdout.flush()
    
    download_task.add_done_callback(_task_done_callback)
    
    try:
        # Ждем завершения скачивания с таймаутом 10 секунд (как требует inline)
        logger.info(f"[INLINE] Waiting for download task with 10 second timeout...")
        # Используем shield, чтобы таймаут не отменял задачу скачивания
        file_ids, file_type = await asyncio.wait_for(asyncio.shield(download_task), timeout=10.0)
        logger.info(f"[INLINE] Download task completed within timeout: {len(file_ids) if file_ids else 0} file(s)")
        
        if file_ids and file_type:
                # Файлы скачались! Проверяем кэш и возвращаем результат
                cached = db.get_cached_file(normalized_url)
                if cached:
                    file_ids_str, media_type = cached
                    if isinstance(file_ids_str, list):
                        file_ids = file_ids_str
                    else:
                        file_ids = json.loads(file_ids_str) if file_ids_str.startswith('[') else [file_ids_str]
                    
                caption = f"@{bot_username}"
                results = []
                
                # Получаем cache_id по URL для файлов из кэша
                cache_id = get_cache_id_for_url(normalized_url)
                
                if len(file_ids) == 1:
                    file_id = file_ids[0]
                    if media_type == 'video':
                        results.append(InlineQueryResultCachedVideo(
                            id=result_id,
                            video_file_id=file_id,
                            title="Видео",
                            description=caption,
                            reply_markup=get_convert_keyboard(cache_id=cache_id, bot_username=bot_username) if cache_id else None
                        ))
                    elif media_type == 'audio':
                        results.append(InlineQueryResultCachedAudio(
                            id=result_id,
                            audio_file_id=file_id,
                            caption=caption
                        ))
                    else:
                        results.append(InlineQueryResultCachedPhoto(
                            id=result_id,
                            photo_file_id=file_id,
                            title="Фото",
                            description=caption
                        ))
                else:
                    for i, file_id in enumerate(file_ids):
                        if media_type == 'video':
                            results.append(InlineQueryResultCachedVideo(
                                id=f"{result_id}_{i}",
                                video_file_id=file_id,
                                title=f"Видео {i+1}",
                                description=caption if i == 0 else None,
                                reply_markup=get_convert_keyboard(cache_id=cache_id, bot_username=bot_username) if cache_id and i == 0 else None
                            ))
                        elif media_type == 'audio':
                            results.append(InlineQueryResultCachedAudio(
                                id=f"{result_id}_{i}",
                                audio_file_id=file_id,
                                caption=caption if i == 0 else None
                            ))
                        else:
                            results.append(InlineQueryResultCachedPhoto(
                                id=f"{result_id}_{i}",
                                photo_file_id=file_id,
                                title=f"Фото {i+1}",
                                description=caption if i == 0 else None
                            ))
                
                await query.answer(results, cache_time=300, is_personal=False)
                return
    except asyncio.TimeoutError:
        logger.info(f"[TIMEOUT] Download timeout for {normalized_url} after 10 seconds")
        logger.info(f"[TIMEOUT] Task will continue in background - file will be sent to user {query.from_user.id} when ready")
        # Отвечаем пустым результатом, скачивание продолжится в фоне
        # Задача download_task продолжит выполняться и отправит файлы пользователю
        # Callback _task_done_callback залогирует результат когда задача завершится
        try:
            await query.answer([], cache_time=1, is_personal=True)
            logger.info(f"[TIMEOUT] Inline query answered with empty result, background task continues")
        except Exception as answer_error:
            logger.warning(f"[TIMEOUT] Failed to answer inline query (query may be too old): {answer_error}")
        # Не отменяем задачу - пусть продолжает работать в фоне
        return
    except Exception as e:
        logger.error(f"Error during download: {e}", exc_info=True)
        try:
            await query.answer([], cache_time=1, is_personal=True)
        except Exception as answer_error:
            logger.warning(f"[ERROR] Failed to answer inline query (query may be too old): {answer_error}")
        return


# Semaphores to limit concurrent operations and prevent VPS overload
MAX_CONCURRENT_DOWNLOADS = 10
MAX_CONCURRENT_CONVERSIONS = 8  # Ограничение на конвертацию (mp3, voice, video_note)
MAX_CONCURRENT_OPTIMIZATIONS = 4  # Оптимизация видео очень тяжелая - только 1 параллельно
MAX_CONCURRENT_TRANSCRIPTIONS = 8  # Расшифровка аудио

download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
conversion_semaphore = asyncio.Semaphore(MAX_CONCURRENT_CONVERSIONS)
optimization_semaphore = asyncio.Semaphore(MAX_CONCURRENT_OPTIMIZATIONS)
transcription_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TRANSCRIPTIONS)

async def delete_status_message_safe(status_message: types.Message, deleted_flag: dict):
    """Безопасно удаляет статусное сообщение только один раз"""
    if status_message and not deleted_flag.get('deleted', False):
        try:
            await status_message.delete()
            deleted_flag['deleted'] = True
        except Exception as e:
            logger.debug(f"Could not delete status message: {e}")

async def send_file_with_retry(message: types.Message, file_path: str, file_type: str, caption: str, max_retries: int = 3, thumbnail_path: str = None):
    """
    Отправляет файл с повторными попытками при таймаутах и сетевых ошибках.
    
    Args:
        message: Сообщение для ответа
        file_path: Путь к файлу
        file_type: Тип файла ('photo', 'video', 'audio')
        caption: Подпись к файлу
        max_retries: Максимальное количество попыток
        thumbnail_path: Путь к миниатюре (JPEG) для видео (опционально)
    
    Returns:
        Отправленное сообщение или None при ошибке
    """
    for attempt in range(max_retries):
        try:
            if file_type == 'photo':
                sent_msg = await message.answer_photo(FSInputFile(file_path), caption=caption)
            elif file_type == 'video':
                # ПОЛУЧАЕМ МЕТАДАННЫЕ ВИДЕО
                video_info = await asyncio.to_thread(get_downloader().get_video_info, file_path)
                
                video_kwargs = {
                    'video': FSInputFile(file_path), 
                    'caption': caption, 
                    'supports_streaming': True,
                    # Передаем точные размеры и длительность!
                    'width': video_info['width'] if video_info else None,
                    'height': video_info['height'] if video_info else None,
                    'duration': video_info['duration'] if video_info else None
                }
                
                # Добавляем Thumbnail
                if thumbnail_path and os.path.exists(thumbnail_path):
                    video_kwargs['thumbnail'] = FSInputFile(thumbnail_path)
                    logger.info(f"[MSG] Sending video with thumbnail: {os.path.basename(thumbnail_path)}")
                
                sent_msg = await message.answer_video(**video_kwargs)
            elif file_type == 'audio':
                sent_msg = await message.answer_audio(FSInputFile(file_path), caption=caption)
            else:
                # Fallback to video
                # ПОЛУЧАЕМ МЕТАДАННЫЕ ВИДЕО
                video_info = await asyncio.to_thread(get_downloader().get_video_info, file_path)
                
                video_kwargs = {
                    'video': FSInputFile(file_path), 
                    'caption': caption, 
                    'supports_streaming': True,
                    # Передаем точные размеры и длительность!
                    'width': video_info['width'] if video_info else None,
                    'height': video_info['height'] if video_info else None,
                    'duration': video_info['duration'] if video_info else None
                }
                
                if thumbnail_path and os.path.exists(thumbnail_path):
                    video_kwargs['thumbnail'] = FSInputFile(thumbnail_path)
                sent_msg = await message.answer_video(**video_kwargs)
            
            logger.info(f"[MSG] File sent successfully on attempt {attempt + 1}")
            return sent_msg
            
        except TelegramNetworkError as e:
            error_msg = str(e).lower()
            is_timeout = 'timeout' in error_msg or 'timed out' in error_msg
            
            if is_timeout and attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5  # 5, 10, 15 секунд
                logger.warning(f"[MSG] Timeout on attempt {attempt + 1}/{max_retries}, retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
                continue
            else:
                logger.error(f"[MSG] Network error after {attempt + 1} attempts: {e}")
                raise
                
        except Exception as e:
            # Для других ошибок не повторяем
            logger.error(f"[MSG] Error sending file: {e}")
            raise
    
    return None

async def process_single_url(message: types.Message, url: str, status_message: types.Message = None, status_deleted_flag: dict = None):
    """Асинхронная обработка одной ссылки"""
    # ... (rest of the function)
    bot_username = await get_bot_username()
    
    # Проверяем, поддерживается ли ссылка
    if not is_supported_url(url):
        logger.info(f"[MSG] Unsupported URL, skipping: {url}")
        if status_deleted_flag is not None:
            await delete_status_message_safe(status_message, status_deleted_flag)
        return
    
    # Расшифровываем короткую ссылку
    logger.info(f"[MSG] Expanding URL: {url}")
    sys.stdout.flush()
    expanded_url = await expand_short_url(url)
    # Убираем обратные слэши, если они есть
    if expanded_url:
        expanded_url = expanded_url.rstrip('\\')
    url_to_use = expanded_url if expanded_url else url
    
    # Проверяем еще раз после расшифровки
    if not is_supported_url(url_to_use):
        logger.info(f"[MSG] Unsupported URL after expansion, skipping: {url_to_use}")
        if status_deleted_flag is not None:
            await delete_status_message_safe(status_message, status_deleted_flag)
        return
    
    # Нормализуем URL
    normalized_url = normalize_url(url_to_use)
    logger.info(f"[MSG] Processing URL: {normalized_url}")
    sys.stdout.flush()
    
    # 1. Проверяем кэш (используем нормализованный URL)
    cached = db.get_cached_file(normalized_url)
    if cached:
        # get_cached_file возвращает (file_ids_list, media_type)
        # где file_ids_list уже список
        if isinstance(cached, tuple) and len(cached) == 2:
            file_ids, media_type = cached
        else:
            # Fallback для старого формата
            file_ids_str, media_type = cached
            if isinstance(file_ids_str, list):
                file_ids = file_ids_str
            else:
                try:
                    file_ids = json.loads(file_ids_str) if file_ids_str.startswith('[') else [file_ids_str]
                except:
                    file_ids = [file_ids_str]
        
        # Убеждаемся что file_ids это список
        if not isinstance(file_ids, list):
            file_ids = [file_ids]
        
        caption = f"⚡ @{bot_username}\n🔗 {normalized_url}"
        logger.info(f"[MSG] Found in cache: {len(file_ids)} file(s)")
        sys.stdout.flush()
        
        # Получаем cache_id по URL для файлов из кэша
        cache_id = get_cache_id_for_url(normalized_url)
        
        # Если один файл
        if len(file_ids) == 1:
            file_id = file_ids[0]
            try:
                if media_type == 'video':
                    sent_msg = await message.answer_video(file_id, caption=caption, supports_streaming=True)
                    # Добавляем кнопку конвертировать с cache_id
                    if sent_msg and cache_id:
                        await bot.edit_message_reply_markup(
                            chat_id=message.chat.id,
                            message_id=sent_msg.message_id,
                            reply_markup=get_convert_keyboard(cache_id=cache_id, bot_username=bot_username)
                        )
                elif media_type == 'audio':
                    await message.answer_audio(file_id, caption=caption)
                else:
                    await message.answer_photo(file_id, caption=caption)
                logger.info(f"[MSG] Sent cached file to {message.chat.id}")
                sys.stdout.flush()
                # Удаляем статусное сообщение после успешной отправки
                if status_deleted_flag is not None:
                    await delete_status_message_safe(status_message, status_deleted_flag)
            except Exception as e:
                logger.error(f"[MSG] Error sending cached file: {e}")
                sys.stdout.flush()
                # Удаляем статусное сообщение при ошибке
                if status_deleted_flag is not None:
                    await delete_status_message_safe(status_message, status_deleted_flag)
        else:
            # Карусель - отправляем массивом через media_group БЕЗ меню конвертации
            logger.info(f"[MSG] Sending cached carousel: {len(file_ids)} files, type: {media_type}")
            sys.stdout.flush()
            
            media_group = []
            for i, file_id in enumerate(file_ids):
                media_caption = caption if i == 0 else None
                # Используем file_id напрямую (строка, не файл)
                if media_type == 'video':
                    media_group.append(InputMediaVideo(media=file_id, caption=media_caption))
                elif media_type == 'audio':
                    from aiogram.types import InputMediaAudio
                    media_group.append(InputMediaAudio(media=file_id, caption=media_caption))
                else:
                    # Для фото карусели
                    media_group.append(InputMediaPhoto(media=file_id, caption=media_caption))
            
            # Отправляем карусель chunks по 10 файлов (лимит Telegram)
            chunk_size = 10
            sent_successfully = False
            for i in range(0, len(media_group), chunk_size):
                chunk = media_group[i:i + chunk_size]
                try:
                    await message.answer_media_group(chunk)
                    logger.info(f"[MSG] ✅ Sent cached carousel chunk {i//chunk_size + 1}/{len(range(0, len(media_group), chunk_size))}")
                    sys.stdout.flush()
                    sent_successfully = True
                    # Удаляем статусное сообщение после первой успешной отправки
                    if i == 0 and status_deleted_flag is not None:
                        await delete_status_message_safe(status_message, status_deleted_flag)
                except Exception as e:
                    logger.error(f"[MSG] ❌ Media group chunk failed: {e}", exc_info=True)
                    sys.stdout.flush()
                    # Удаляем статусное сообщение при ошибке (если еще не удалено)
                    if status_deleted_flag is not None:
                        await delete_status_message_safe(status_message, status_deleted_flag)
            
            if sent_successfully:
                logger.info(f"[MSG] ✅ Successfully sent cached carousel with {len(file_ids)} files")
            else:
                logger.error(f"[MSG] ❌ Failed to send cached carousel")
            sys.stdout.flush()
        return

    # 2. Проверяем, не идет ли уже загрузка
    if normalized_url in active_downloads:
        logger.info(f"[MSG] Download already in progress for {normalized_url}, waiting...")
        sys.stdout.flush()
        future = active_downloads[normalized_url]
        try:
            # Increase timeout significantly to handle bulk queues
            file_ids, file_type = await asyncio.wait_for(future, timeout=300.0) 
            
            if file_ids:
                caption = f"@{bot_username}\n🔗 {normalized_url}"
                # Получаем cache_id по URL для файлов из кэша
                cache_id = get_cache_id_for_url(normalized_url)
                
                if len(file_ids) == 1:
                    file_id = file_ids[0]
                    if file_type == 'video':
                        sent_msg = await message.answer_video(file_id, caption=caption, supports_streaming=True)
                        # Добавляем кнопку конвертировать с cache_id
                        if sent_msg and cache_id:
                            await bot.edit_message_reply_markup(
                                chat_id=message.chat.id,
                                message_id=sent_msg.message_id,
                                reply_markup=get_convert_keyboard(cache_id=cache_id, bot_username=bot_username)
                            )
                    elif file_type == 'audio':
                        await message.answer_audio(file_id, caption=caption)
                    else:
                        await message.answer_photo(file_id, caption=caption)
                    # Удаляем статусное сообщение после успешной отправки
                    if status_deleted_flag is not None:
                        await delete_status_message_safe(status_message, status_deleted_flag)
                else:
                    media_group = []
                    for i, file_id in enumerate(file_ids):
                        media_caption = caption if i == 0 else None
                        if file_type == 'video':
                            media_group.append(InputMediaVideo(media=file_id, caption=media_caption))
                        elif file_type == 'audio':
                             # Аудио в media_group поддерживается как Audio, но InputMediaAudio
                             # Однако aiogram может требовать InputMediaAudio
                             from aiogram.types import InputMediaAudio
                             media_group.append(InputMediaAudio(media=file_id, caption=media_caption))
                        else:
                            media_group.append(InputMediaPhoto(media=file_id, caption=media_caption))
                    
                    chunk_size = 10
                    for i in range(0, len(media_group), chunk_size):
                        chunk = media_group[i:i + chunk_size]
                        await message.answer_media_group(chunk)
                        # Удаляем статусное сообщение после первой успешной отправки
                        if i == 0 and status_deleted_flag is not None:
                            await delete_status_message_safe(status_message, status_deleted_flag)
                logger.info(f"[MSG] Sent files from parallel download to {message.chat.id}")
                sys.stdout.flush()
                # Удаляем статусное сообщение, если еще не удалено
                if status_deleted_flag is not None:
                    await delete_status_message_safe(status_message, status_deleted_flag)
                return
        except Exception as e:
            logger.error(f"[MSG] Error waiting for download: {e}")
            sys.stdout.flush()
            # Удаляем статусное сообщение при ошибке
            if status_deleted_flag is not None:
                await delete_status_message_safe(status_message, status_deleted_flag)

    # 3. Проверяем, есть ли уже скачанный файл на диске
    downloaded_file_info = db.get_downloaded_file(normalized_url)
    if downloaded_file_info:
        logger.info(f"[MSG] Found downloaded file on disk: {downloaded_file_info['file_path']}")
        sys.stdout.flush()
        
        file_path = downloaded_file_info['file_path']
        file_type = downloaded_file_info.get('file_type', 'video')
        media_type = downloaded_file_info.get('media_type', 'video')
        
        # Проверяем, что файл действительно существует
        if os.path.exists(file_path):
            try:
                # Отправляем файл напрямую
                logger.info(f"[MSG] Sending existing file: {file_path}, size: {os.path.getsize(file_path)/1024/1024:.2f} MB")
                sys.stdout.flush()
                
                ext = os.path.splitext(file_path)[1].lower()
                uploaded_file_ids = []
                
                if ext in ['.jpg', '.jpeg', '.png', '.webp']:
                    sent_msg = await send_file_with_retry(message, file_path, 'photo', caption)
                    if sent_msg and sent_msg.photo:
                        uploaded_file_ids.append(sent_msg.photo[-1].file_id)
                        file_type = 'photo'
                elif ext in ['.mp3', '.m4a', '.aac', '.ogg', '.wav', '.opus', '.flac']:
                    sent_msg = await send_file_with_retry(message, file_path, 'audio', caption)
                    if sent_msg and sent_msg.audio:
                        uploaded_file_ids.append(sent_msg.audio.file_id)
                        file_type = 'audio'
                else:
                    # Проверяем, нужно ли оптимизировать видео для Telegram
                    needs_opt, opt_reason = await asyncio.to_thread(get_downloader().needs_telegram_optimization, file_path)
                    optimized_path = file_path
                    
                    if needs_opt:
                        logger.info(f"[MSG] Video needs optimization: {opt_reason}")
                        logger.info(f"[MSG] Optimizing video for Telegram...")
                        sys.stdout.flush()
                        
                        # Оптимизируем видео с ограничением параллельных операций (только 1 одновременно)
                        async with optimization_semaphore:
                            optimized_path = await asyncio.to_thread(
                                get_downloader().optimize_for_telegram, 
                                file_path, 
                                os.path.dirname(file_path),
                                fast_mode=True
                            )
                        
                        if optimized_path and os.path.exists(optimized_path):
                            logger.info(f"[MSG] ✅ Video optimized: {os.path.basename(optimized_path)}")
                            file_path = optimized_path
                        else:
                            logger.warning(f"[MSG] ⚠️ Optimization failed, using original")
                    
                    # Генерируем thumbnail для ВСЕХ видео (гарантируем превью)
                    thumbnail_path = None
                    final_file_size = os.path.getsize(file_path)
                    logger.info(f"[MSG] Generating thumbnail for video ({final_file_size/1024/1024:.2f}MB)...")
                    try:
                        # Генерируем обложку на 1-й секунде (или 0.0, если видео короткое)
                        thumbnail_path = await asyncio.to_thread(
                            get_downloader().generate_thumbnail,
                            file_path,
                            os.path.dirname(file_path),
                            1.0  # time_offset
                        )
                        if thumbnail_path:
                            logger.info(f"[MSG] ✅ Thumbnail generated: {os.path.basename(thumbnail_path)}")
                        else:
                            logger.warning(f"[MSG] ⚠️ Thumbnail generation returned None")
                    except Exception as thumb_error:
                        logger.warning(f"[MSG] Failed to generate thumbnail: {thumb_error}")
                        thumbnail_path = None
                    
                    sent_msg = await send_file_with_retry(message, file_path, 'video', caption, thumbnail_path=thumbnail_path)
                    if sent_msg and sent_msg.video:
                        uploaded_file_ids.append(sent_msg.video.file_id)
                        file_type = 'video'
                
                if uploaded_file_ids:
                    # Сохраняем в кэш и обновляем cache_id в downloaded_files
                    cache_id = db.save_file_to_cache(normalized_url, uploaded_file_ids, file_type, message.from_user.id)
                    if cache_id and downloaded_file_info.get('cache_id') != cache_id:
                        # Обновляем cache_id в downloaded_files
                        db.cursor.execute("""
                            UPDATE downloaded_files 
                            SET cache_id = ? 
                            WHERE url = ?
                        """, (cache_id, normalized_url))
                        db.connection.commit()
                    
                    # Добавляем кнопку конвертировать для видео
                    if file_type == 'video' and sent_msg and cache_id:
                        await bot.edit_message_reply_markup(
                            chat_id=message.chat.id,
                            message_id=sent_msg.message_id,
                            reply_markup=get_convert_keyboard(cache_id=cache_id, bot_username=bot_username)
                        )
                    
                    logger.info(f"[MSG] Successfully sent existing file")
                    sys.stdout.flush()
                    
                    # Удаляем статусное сообщение
                    if status_deleted_flag is not None:
                        await delete_status_message_safe(status_message, status_deleted_flag)
                    
                    return
                    
            except TelegramEntityTooLarge as e:
                error_msg = f"❌ Файл слишком большой для отправки в Telegram.\n\nОшибка: {str(e)}"
                logger.error(f"[MSG] File too large: {e}", exc_info=True)
                await message.answer(error_msg)
                if status_deleted_flag is not None:
                    await delete_status_message_safe(status_message, status_deleted_flag)
                return
            except TelegramNetworkError as e:
                error_msg = f"❌ Ошибка сети при отправке файла.\n\nОшибка: {str(e)}"
                logger.error(f"[MSG] Network error: {e}", exc_info=True)
                await message.answer(error_msg)
                if status_deleted_flag is not None:
                    await delete_status_message_safe(status_message, status_deleted_flag)
                return
            except Exception as e:
                error_msg = f"❌ Произошла ошибка при отправке файла.\n\nОшибка: {str(e)}"
                logger.error(f"[MSG] Error sending existing file: {e}", exc_info=True)
                await message.answer(error_msg)
                if status_deleted_flag is not None:
                    await delete_status_message_safe(status_message, status_deleted_flag)
                return
        else:
            # Файл удален, удаляем запись из БД
            logger.warning(f"[MSG] File from DB no longer exists: {file_path}")
            db.delete_downloaded_file(normalized_url)
    
    # 4. Если нет в кэше, нет на диске и не идет загрузка - создаем Future и скачиваем
    # Создаем Future для этой загрузки
    future = asyncio.Future()
    active_downloads[normalized_url] = future
    
    # Обновляем время последней активности
    global _last_activity_time
    _last_activity_time = time.time()
    
    try:
        logger.info(f"[MSG] Queuing download for {normalized_url}")
        sys.stdout.flush()
        
        async with download_semaphore:
            logger.info(f"[MSG] Starting download for {normalized_url}")
            log_resource_usage(f"Before download: {normalized_url}")
            sys.stdout.flush()
            
            start_time = time.time()
            try:
                # Максимум 600 секунд на загрузку
                files, task_dir = await asyncio.wait_for(
                    asyncio.to_thread(get_downloader().download, url_to_use),
                    timeout=600.0
                )
                end_time = time.time()
                duration = end_time - start_time
                log_resource_usage(f"After download: {normalized_url}, files_count={len(files) if files else 0}, duration={duration:.2f}s")
                # Выгружаем yt_dlp и pytubefix из памяти сразу после скачивания
                unload_heavy_modules()
            except asyncio.TimeoutError:
                logger.error(f"[ERROR] Download timeout after 600 seconds for {normalized_url}")
                await message.answer(f"❌ Таймаут: файл не был скачан за 600 секунд")
                result = ([], None)
                future.set_result(result)
                if status_deleted_flag is not None:
                    await delete_status_message_safe(status_message, status_deleted_flag)
                return
            except NameError as e:
                if '_ProgressState' in str(e):
                    logger.warning(f"[WARNING] yt-dlp _ProgressState error caught and ignored for {normalized_url}: {e}")
                    # Продолжаем выполнение - это не критическая ошибка
                    pass
                else:
                    raise e
        
        if not files:
            await message.answer(f"❌ Не удалось скачать: {url}")
            result = ([], None)
            future.set_result(result)
            # Удаляем статусное сообщение при ошибке
            if status_deleted_flag is not None:
                await delete_status_message_safe(status_message, status_deleted_flag)
            # Очистка папки при ошибке
            if ENABLE_CLEANUP and 'task_dir' in locals() and task_dir:
                await asyncio.to_thread(get_downloader().cleanup, task_dir)
            return

        files.sort()
        
        total_size = sum(os.path.getsize(f) for f in files if os.path.exists(f))
        size_mb = total_size / (1024 * 1024)
        avg_speed = size_mb / duration if duration > 0 else 0
        
        logger.info(f"[MSG] Downloaded {len(files)} file(s), duration: {duration:.2f}s, size: {size_mb:.2f} MB, speed: {avg_speed:.2f} MB/s")
        logger.info(f"[MSG] Starting upload")
        sys.stdout.flush()
        
        caption = f"@{bot_username}\n🔗 {normalized_url}"
        uploaded_file_ids = []
        file_type = 'video'

        if len(files) == 1:
                file_path = files[0]
                ext = os.path.splitext(file_path)[1].lower()
                
                try:
                    sent_msg = None
                    # Log file size
                    file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
                    logger.info(f"[MSG] Uploading file: {file_path}, size: {file_size/1024/1024:.2f} MB")
                    sys.stdout.flush()

                    if ext in ['.jpg', '.jpeg', '.png', '.webp']:
                        sent_msg = await send_file_with_retry(message, file_path, 'photo', caption)
                        if sent_msg and sent_msg.photo:
                            uploaded_file_ids.append(sent_msg.photo[-1].file_id)
                            file_type = 'photo'
                    elif ext in ['.mp3', '.m4a', '.aac', '.ogg', '.wav', '.opus', '.flac']:
                        # Для SoundCloud пытаемся загрузить метаданные и обложку
                        metadata = None
                        thumbnail_path = None
                        task_dir = os.path.dirname(file_path)
                        metadata_file = os.path.join(task_dir, 'metadata.json')
                        
                        # Ищем обложку в папке (даже если нет metadata.json)
                        logger.info(f"[MSG] Looking for cover art in: {task_dir}")
                        if os.path.exists(task_dir):
                            try:
                                all_files = os.listdir(task_dir)
                                logger.info(f"[MSG] Files in task_dir: {all_files}")
                                for thumb_file in all_files:
                                    if thumb_file.endswith(('.jpg', '.jpeg', '.png', '.webp')) and thumb_file != os.path.basename(file_path) and thumb_file != 'metadata.json':
                                        thumbnail_path = os.path.join(task_dir, thumb_file)
                                        logger.info(f"[MSG] Found cover art: {thumbnail_path}")
                                        break
                            except Exception as e:
                                logger.warning(f"Failed to list files in task_dir: {e}")
                        
                        if os.path.exists(metadata_file):
                            try:
                                with open(metadata_file, 'r', encoding='utf-8') as f:
                                    metadata = json.load(f)
                            except Exception as e:
                                logger.warning(f"Failed to load metadata: {e}")
                        
                        # Для SoundCloud отправляем обложку отдельным сообщением перед аудио
                        is_soundcloud = 'soundcloud.com' in normalized_url
                        cover_file_id = None
                        if is_soundcloud and thumbnail_path and os.path.exists(thumbnail_path):
                            try:
                                logger.info(f"[MSG] Sending SoundCloud cover art: {thumbnail_path}")
                                cover_msg = await message.answer_photo(FSInputFile(thumbnail_path), caption=caption)
                                if cover_msg and cover_msg.photo:
                                    cover_file_id = cover_msg.photo[-1].file_id
                                    logger.info(f"[MSG] Cover art sent successfully")
                                    # НЕ добавляем обложку в uploaded_file_ids - она не нужна в кэше для инлайна
                            except Exception as e:
                                logger.warning(f"Failed to send cover art: {e}")
                        
                        # Формируем параметры для отправки аудио
                        audio_kwargs = {'caption': caption}
                        if metadata:
                            audio_kwargs['title'] = metadata.get('title', 'Track')
                            audio_kwargs['performer'] = metadata.get('uploader', 'Unknown')
                        if thumbnail_path:
                            audio_kwargs['thumbnail'] = FSInputFile(thumbnail_path)
                        
                        # Для аудио с метаданными используем прямой вызов, но с обработкой ошибок
                        try:
                            sent_msg = await message.answer_audio(FSInputFile(file_path), **audio_kwargs)
                        except TelegramNetworkError as e:
                            error_msg = str(e).lower()
                            is_timeout = 'timeout' in error_msg or 'timed out' in error_msg
                            if is_timeout:
                                logger.warning(f"[MSG] Timeout sending audio, retrying...")
                                await asyncio.sleep(5)
                                sent_msg = await message.answer_audio(FSInputFile(file_path), **audio_kwargs)
                            else:
                                raise
                        
                        if sent_msg and sent_msg.audio:
                            # Добавляем только аудио file_id в кэш (обложка отправляется отдельно, но не кэшируется)
                            uploaded_file_ids.append(sent_msg.audio.file_id)
                            file_type = 'audio'
                            log_resource_usage(f"Audio file uploaded, file_id={sent_msg.audio.file_id}")
                            
                            # Сохраняем в кэш сразу для аудио и добавляем кнопку
                            if uploaded_file_ids:
                                log_resource_usage(f"Before saving audio to cache")
                                cache_id_audio = db.save_file_to_cache(normalized_url, uploaded_file_ids, file_type, message.from_user.id)
                                log_resource_usage(f"After saving audio to cache: cache_id={cache_id_audio}")
                                if cache_id_audio and sent_msg:
                                    log_resource_usage(f"Adding convert button for audio (cache_id={cache_id_audio})")
                                    try:
                                        await bot.edit_message_reply_markup(
                                            chat_id=message.chat.id,
                                            message_id=sent_msg.message_id,
                                            reply_markup=get_convert_keyboard(cache_id=cache_id_audio, bot_username=bot_username)
                                        )
                                        logger.info(f"[BUTTON] ✅ Added convert button for audio (cache_id={cache_id_audio})")
                                    except Exception as e:
                                        logger.error(f"[BUTTON] ❌ Failed to add convert button for audio: {e}")
                                    log_resource_usage(f"After adding convert button for audio")
                            
                            # Умная очистка: удаляем файл после получения file_id
                            if ENABLE_CLEANUP:
                                try:
                                    if os.path.exists(file_path):
                                        os.remove(file_path)
                                        logger.info(f"[CLEANUP] 🗑️ Cleaned up audio file after Telegram upload: {os.path.basename(file_path)}")
                                except Exception as cleanup_error:
                                    logger.warning(f"[CLEANUP] Failed to cleanup audio file {file_path}: {cleanup_error}")
                    else:
                        # Default to video for mp4 and others
                        # Проверяем размер файла (50MB лимит для ботов)
                        file_size = os.path.getsize(file_path)
                        needs_compression = file_size > 48 * 1024 * 1024
                        
                        optimized_path = file_path
                        
                        if needs_compression:
                            logger.info(f"[MSG] Video too large ({file_size/1024/1024:.2f}MB), compressing...")
                            # Не обновляем статусное сообщение, так как оно может быть удалено или изменено в другом месте
                            # Но если оно есть, было бы неплохо (но тут status_msg не доступен напрямую в этом блоке if/else без проверки)
                            # status_message передается в функцию, так что можно попробовать
                            if status_message:
                                try:
                                    await status_message.edit_text(f"⏳ Видео больше 50МБ, сжимаю...")
                                except:
                                    pass
                            
                            async with optimization_semaphore:
                                optimized_path = await asyncio.to_thread(
                                    get_downloader().compress_video,
                                    file_path,
                                    task_dir
                                )
                            
                            if optimized_path and os.path.exists(optimized_path):
                                logger.info(f"[MSG] ✅ Video compressed: {os.path.basename(optimized_path)}")
                                file_path_to_send = optimized_path
                            else:
                                logger.warning(f"[MSG] ⚠️ Compression failed, using original")
                                file_path_to_send = file_path
                        else:
                            # ВСЕГДА проверяем и оптимизируем видео для Telegram (гарантируем H.264 + AAC)
                            needs_opt, opt_reason = await asyncio.to_thread(get_downloader().needs_telegram_optimization, file_path)
                            
                            if needs_opt:
                                logger.info(f"[MSG] Video needs optimization: {opt_reason}")
                                logger.info(f"[MSG] Optimizing video for Telegram (H.264 + AAC)...")
                                sys.stdout.flush()
                                
                                # Оптимизируем видео с ограничением параллельных операций (только 1 одновременно)
                                async with optimization_semaphore:
                                    optimized_path = await asyncio.to_thread(
                                        get_downloader().optimize_for_telegram, 
                                        file_path, 
                                        task_dir,
                                        fast_mode=True
                                    )
                                
                                if optimized_path and os.path.exists(optimized_path):
                                    logger.info(f"[MSG] ✅ Video optimized: {os.path.basename(optimized_path)}")
                                    # Используем оптимизированную версию
                                    file_path_to_send = optimized_path
                                else:
                                    logger.warning(f"[MSG] ⚠️ Optimization failed, using original")
                                    file_path_to_send = file_path
                            else:
                                # Даже если не нужно оптимизировать, убеждаемся что формат правильный
                                # Для маленьких видео с правильным кодеком отправляем как есть
                                file_path_to_send = file_path
                                logger.info(f"[MSG] Video format OK, sending as-is")
                        
                        # Генерируем thumbnail для ВСЕХ видео (гарантируем превью)
                        thumbnail_path = None
                        final_file_size = os.path.getsize(file_path_to_send)
                        logger.info(f"[MSG] Generating thumbnail for video ({final_file_size/1024/1024:.2f}MB)...")
                        try:
                            # Генерируем обложку на 1-й секунде (или 0.0, если видео короткое)
                            thumbnail_path = await asyncio.to_thread(
                                get_downloader().generate_thumbnail,
                                file_path_to_send,
                                task_dir,
                                1.0  # time_offset
                            )
                            if thumbnail_path:
                                logger.info(f"[MSG] ✅ Thumbnail generated: {os.path.basename(thumbnail_path)}")
                            else:
                                logger.warning(f"[MSG] ⚠️ Thumbnail generation returned None")
                        except Exception as thumb_error:
                            logger.warning(f"[MSG] Failed to generate thumbnail: {thumb_error}")
                            thumbnail_path = None
                        
                        sent_msg = await send_file_with_retry(message, file_path_to_send, 'video', caption, thumbnail_path=thumbnail_path)
                        if sent_msg and sent_msg.video:
                            file_id = sent_msg.video.file_id
                            uploaded_file_ids.append(file_id)
                            file_type = 'video'
                            # Умная очистка: удаляем файл после получения file_id
                            if ENABLE_CLEANUP:
                                try:
                                    # Удаляем отправленный файл (может быть оптимизированным)
                                    if os.path.exists(file_path_to_send):
                                        os.remove(file_path_to_send)
                                        logger.info(f"[CLEANUP] 🗑️ Cleaned up video file after Telegram upload: {os.path.basename(file_path_to_send)}")
                                    # Удаляем оригинальный файл если он отличается от отправленного
                                    if file_path != file_path_to_send and os.path.exists(file_path):
                                        os.remove(file_path)
                                        logger.info(f"[CLEANUP] 🗑️ Cleaned up original video: {os.path.basename(file_path)}")
                                except Exception as cleanup_error:
                                    logger.warning(f"[CLEANUP] Failed to cleanup video file: {cleanup_error}")
                    
                    logger.info(f"[MSG] Upload successful")
                    sys.stdout.flush()

                    # Сохраняем в кэш и получаем cache_id
                    cache_id = None
                    if uploaded_file_ids:
                        cache_id = db.save_file_to_cache(normalized_url, uploaded_file_ids, file_type, message.from_user.id)
                        
                        # Сохраняем информацию о скачанном файле в БД (если cleanup отключен)
                        if not ENABLE_CLEANUP and 'task_dir' in locals() and task_dir and len(files) == 1:
                            file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
                            ext = os.path.splitext(file_path)[1].lower()
                            # Определяем media_type на основе расширения
                            if ext in ['.jpg', '.jpeg', '.png', '.webp']:
                                media_type = 'photo'
                            elif ext in ['.mp3', '.m4a', '.aac', '.ogg', '.wav', '.opus', '.flac']:
                                media_type = 'audio'
                            else:
                                media_type = 'video'
                            
                            db.save_downloaded_file(
                                normalized_url, 
                                file_path, 
                                file_size, 
                                ext, 
                                media_type, 
                                task_dir, 
                                cache_id,
                                expires_hours=24  # Файл будет доступен 24 часа
                            )
                            logger.info(f"[MSG] Saved downloaded file info to DB: {file_path} (type: {media_type})")
                        
                        # Добавляем кнопку конвертировать с cache_id для видео и аудио
                        if sent_msg and cache_id and (file_type == 'video' or file_type == 'audio'):
                            log_resource_usage(f"Adding convert button for {file_type} after download (cache_id={cache_id})")
                            try:
                                await bot.edit_message_reply_markup(
                                    chat_id=message.chat.id,
                                    message_id=sent_msg.message_id,
                                    reply_markup=get_convert_keyboard(cache_id=cache_id, bot_username=bot_username)
                                )
                                logger.info(f"[BUTTON] ✅ Added convert button for {file_type} (cache_id={cache_id})")
                            except Exception as e:
                                logger.error(f"[BUTTON] ❌ Failed to add convert button for {file_type}: {e}")
                            log_resource_usage(f"After adding convert button for {file_type}")
                            # Очистка памяти после добавления кнопки
                            unload_heavy_modules()
                        # Удаляем статусное сообщение после успешной отправки
                        if status_deleted_flag is not None:
                            await delete_status_message_safe(status_message, status_deleted_flag)
                        # Очистка папки сразу после успешной отправки
                        if ENABLE_CLEANUP and 'task_dir' in locals() and task_dir:
                            asyncio.create_task(asyncio.to_thread(get_downloader().cleanup, task_dir))
                        
                        # Агрессивная очистка памяти после завершения операции
                        unload_heavy_modules()
                        log_resource_usage(f"After complete processing: {file_type}")
                        
                except TelegramEntityTooLarge as e:
                    error_msg = f"❌ Файл слишком большой для отправки в Telegram.\n\nОшибка: {str(e)}\n\nРазмер файла превышает лимит Telegram (обычно 50 МБ для видео)."
                    logger.error(f"[MSG] File too large: {e}", exc_info=True)
                    await message.answer(error_msg)
                    # Удаляем статусное сообщение при ошибке
                    if status_deleted_flag is not None:
                        await delete_status_message_safe(status_message, status_deleted_flag)
                    # Очистка папки при ошибке отправки (если включена)
                    if ENABLE_CLEANUP and 'task_dir' in locals() and task_dir:
                        await asyncio.to_thread(get_downloader().cleanup, task_dir)
                except TelegramNetworkError as e:
                    error_msg = f"❌ Ошибка сети при отправке файла.\n\nОшибка: {str(e)}\n\nПопробуйте позже или проверьте соединение с интернетом."
                    logger.error(f"[MSG] Network error sending file: {e}", exc_info=True)
                    await message.answer(error_msg)
                    # Удаляем статусное сообщение при ошибке
                    if status_deleted_flag is not None:
                        await delete_status_message_safe(status_message, status_deleted_flag)
                    # Очистка папки при ошибке отправки (если включена)
                    if ENABLE_CLEANUP and 'task_dir' in locals() and task_dir:
                        await asyncio.to_thread(get_downloader().cleanup, task_dir)
                except Exception as e:
                    error_msg = f"❌ Произошла ошибка при отправке файла.\n\nОшибка: {str(e)}"
                    logger.error(f"[MSG] Error sending single file: {e}", exc_info=True)
                    await message.answer(error_msg)
                    # Удаляем статусное сообщение при ошибке
                    if status_deleted_flag is not None:
                        await delete_status_message_safe(status_message, status_deleted_flag)
                    # Очистка папки при ошибке отправки (если включена)
                    if ENABLE_CLEANUP and 'task_dir' in locals() and task_dir:
                        await asyncio.to_thread(get_downloader().cleanup, task_dir)

        else:
            # Альбом (карусель) - отправляем массивом и сохраняем все в кэш
                # Для SoundCloud фильтруем файлы - отправляем только аудио, обложки не отправляем отдельно
                if 'soundcloud.com' in normalized_url:
                    # Для SoundCloud должен быть только один аудио файл
                    audio_files = [f for f in files if os.path.splitext(f)[1].lower() in ['.mp3', '.m4a', '.aac', '.ogg', '.wav', '.opus', '.flac']]
                    if audio_files:
                        # Отправляем как одиночный аудио файл с обложкой
                        file_path = audio_files[0]
                        metadata = None
                        thumbnail_path = None
                        task_dir = os.path.dirname(file_path)
                        metadata_file = os.path.join(task_dir, 'metadata.json')
                        if os.path.exists(metadata_file):
                            try:
                                with open(metadata_file, 'r', encoding='utf-8') as f:
                                    metadata = json.load(f)
                                # Ищем обложку в папке
                                for thumb_file in os.listdir(task_dir):
                                    if thumb_file.endswith(('.jpg', '.jpeg', '.png', '.webp')) and thumb_file != os.path.basename(file_path):
                                        thumbnail_path = os.path.join(task_dir, thumb_file)
                                        break
                            except Exception as e:
                                logger.warning(f"Failed to load metadata: {e}")
                        
                        audio_kwargs = {'caption': caption}
                        if metadata:
                            audio_kwargs['title'] = metadata.get('title', 'Track')
                            audio_kwargs['performer'] = metadata.get('uploader', 'Unknown')
                        if thumbnail_path:
                            audio_kwargs['thumbnail'] = FSInputFile(thumbnail_path)
                        
                        try:
                            sent_msg = await message.answer_audio(FSInputFile(file_path), **audio_kwargs)
                            if sent_msg and sent_msg.audio:
                                uploaded_file_ids.append(sent_msg.audio.file_id)
                                file_type = 'audio'
                                # Сохраняем в кэш
                                if uploaded_file_ids:
                                    db.save_file_to_cache(normalized_url, uploaded_file_ids, file_type, message.from_user.id)
                                # Удаляем статусное сообщение
                                if status_deleted_flag is not None:
                                    await delete_status_message_safe(status_message, status_deleted_flag)
                                # Очистка папки после успешной отправки
                                if ENABLE_CLEANUP and 'task_dir' in locals() and task_dir:
                                    asyncio.create_task(asyncio.to_thread(get_downloader().cleanup, task_dir))
                                return
                        except TelegramEntityTooLarge as e:
                            error_msg = f"❌ Файл слишком большой для отправки в Telegram.\n\nОшибка: {str(e)}\n\nРазмер файла превышает лимит Telegram (обычно 50 МБ для видео)."
                            logger.error(f"[MSG] File too large: {e}", exc_info=True)
                            await message.answer(error_msg)
                            if status_deleted_flag is not None:
                                await delete_status_message_safe(status_message, status_deleted_flag)
                            if ENABLE_CLEANUP and 'task_dir' in locals() and task_dir:
                                await asyncio.to_thread(get_downloader().cleanup, task_dir)
                            return
                        except TelegramNetworkError as e:
                            error_msg = f"❌ Ошибка сети при отправке файла.\n\nОшибка: {str(e)}\n\nПопробуйте позже или проверьте соединение с интернетом."
                            logger.error(f"[MSG] Network error sending SoundCloud audio: {e}", exc_info=True)
                            await message.answer(error_msg)
                            if status_deleted_flag is not None:
                                await delete_status_message_safe(status_message, status_deleted_flag)
                            if ENABLE_CLEANUP and 'task_dir' in locals() and task_dir:
                                await asyncio.to_thread(get_downloader().cleanup, task_dir)
                            return
                        except Exception as e:
                            error_msg = f"❌ Произошла ошибка при отправке файла.\n\nОшибка: {str(e)}"
                            logger.error(f"[MSG] Error sending SoundCloud audio: {e}", exc_info=True)
                            await message.answer(error_msg)
                            if status_deleted_flag is not None:
                                await delete_status_message_safe(status_message, status_deleted_flag)
                            if ENABLE_CLEANUP and 'task_dir' in locals() and task_dir:
                                await asyncio.to_thread(get_downloader().cleanup, task_dir)
                            return
                
                carousel_type = 'photo'
                logger.info(f"[MSG] Uploading carousel with {len(files)} files")
                sys.stdout.flush()
                
                # Сначала собираем все файлы для media_group
                # Оптимизируем видео файлы если нужно
                optimized_files_map = {}  # Индекс -> оптимизированный путь
                for i, file_path in enumerate(files):
                    ext = os.path.splitext(file_path)[1].lower()
                    
                    if ext in ['.mp4', '.mov', '.avi', '.mkv']:
                        # Проверяем, нужно ли оптимизировать видео
                        needs_opt, opt_reason = await asyncio.to_thread(get_downloader().needs_telegram_optimization, file_path)
                        
                        if needs_opt:
                            logger.info(f"[MSG] Carousel video {i+1}/{len(files)} needs optimization: {opt_reason}")
                            # Оптимизируем видео с ограничением параллельных операций (только 1 одновременно)
                            async with optimization_semaphore:
                                optimized_path = await asyncio.to_thread(
                                    get_downloader().optimize_for_telegram, 
                                    file_path, 
                                    task_dir,
                                    fast_mode=True
                                )
                            
                            if optimized_path and os.path.exists(optimized_path):
                                optimized_files_map[i] = optimized_path
                                logger.info(f"[MSG] ✅ Carousel video {i+1} optimized")
                
                media_group = []
                for i, file_path in enumerate(files):
                    ext = os.path.splitext(file_path)[1].lower()
                    media_caption = caption if i == 0 else None
                    
                    # Используем оптимизированную версию если есть
                    if i in optimized_files_map:
                        file_path = optimized_files_map[i]
                    
                    if ext in ['.jpg', '.jpeg', '.png', '.webp']:
                        media_group.append(InputMediaPhoto(media=FSInputFile(file_path), caption=media_caption))
                        carousel_type = 'photo'
                    elif ext in ['.mp4', '.mov', '.avi', '.mkv']:
                        media_group.append(InputMediaVideo(media=FSInputFile(file_path), caption=media_caption))
                        carousel_type = 'video'
                
                # Отправляем media_group и получаем file_id из ответа
                chunk_size = 10
                for i in range(0, len(media_group), chunk_size):
                    chunk = media_group[i:i + chunk_size]
                    try:
                        logger.info(f"[MSG] Sending chunk {i//chunk_size + 1}")
                        sys.stdout.flush()
                        sent_messages = await message.answer_media_group(chunk)
                        # Извлекаем file_id из отправленных сообщений и очищаем файлы
                        chunk_files = files[i:i + chunk_size]
                        for msg_idx, sent_msg in enumerate(sent_messages):
                            file_idx = i + msg_idx
                            if sent_msg.photo:
                                uploaded_file_ids.append(sent_msg.photo[-1].file_id)
                                # Умная очистка: удаляем файл после получения file_id
                                if ENABLE_CLEANUP and file_idx < len(files):
                                    file_path = files[file_idx]
                                    try:
                                        if os.path.exists(file_path):
                                            os.remove(file_path)
                                            logger.info(f"[CLEANUP] 🗑️ Cleaned up carousel photo after Telegram upload: {os.path.basename(file_path)}")
                                    except Exception as cleanup_error:
                                        logger.warning(f"[CLEANUP] Failed to cleanup carousel photo {file_path}: {cleanup_error}")
                            elif sent_msg.video:
                                uploaded_file_ids.append(sent_msg.video.file_id)
                                # Умная очистка: удаляем файл после получения file_id
                                if ENABLE_CLEANUP and file_idx < len(files):
                                    original_file_path = files[file_idx]
                                    # Удаляем оптимизированный файл если он был создан
                                    if file_idx in optimized_files_map:
                                        optimized_file_path = optimized_files_map[file_idx]
                                        try:
                                            if os.path.exists(optimized_file_path):
                                                os.remove(optimized_file_path)
                                                logger.info(f"[CLEANUP] 🗑️ Cleaned up optimized carousel video: {os.path.basename(optimized_file_path)}")
                                        except Exception as cleanup_error:
                                            logger.warning(f"[CLEANUP] Failed to cleanup optimized carousel video {optimized_file_path}: {cleanup_error}")
                                    # Удаляем оригинальный файл
                                    try:
                                        if os.path.exists(original_file_path):
                                            os.remove(original_file_path)
                                            logger.info(f"[CLEANUP] 🗑️ Cleaned up carousel video after Telegram upload: {os.path.basename(original_file_path)}")
                                    except Exception as cleanup_error:
                                        logger.warning(f"[CLEANUP] Failed to cleanup carousel video {original_file_path}: {cleanup_error}")
                        # Удаляем статусное сообщение после первой успешной отправки
                        if i == 0 and status_deleted_flag is not None:
                            await delete_status_message_safe(status_message, status_deleted_flag)
                    except TelegramEntityTooLarge as e:
                        error_msg = f"❌ Файл слишком большой для отправки в Telegram.\n\nОшибка: {str(e)}\n\nРазмер файла превышает лимит Telegram (обычно 50 МБ для видео)."
                        logger.error(f"[MSG] File too large in carousel: {e}", exc_info=True)
                        await message.answer(error_msg)
                        sys.stdout.flush()
                        # Удаляем статусное сообщение при ошибке (если еще не удалено)
                        if status_deleted_flag is not None:
                            await delete_status_message_safe(status_message, status_deleted_flag)
                        # Очистка папки при ошибке отправки
                        if ENABLE_CLEANUP and 'task_dir' in locals() and task_dir:
                            await asyncio.to_thread(get_downloader().cleanup, task_dir)
                    except TelegramNetworkError as e:
                        error_msg = str(e).lower()
                        is_timeout = 'timeout' in error_msg or 'timed out' in error_msg
                        
                        if is_timeout:
                            logger.warning(f"[MSG] Timeout sending carousel chunk, retrying...")
                            await asyncio.sleep(5)
                            try:
                                sent_messages = await message.answer_media_group(chunk)
                                # Продолжаем обработку успешной отправки
                                chunk_files = files[i:i + chunk_size]
                                for msg_idx, sent_msg in enumerate(sent_messages):
                                    file_idx = i + msg_idx
                                    if sent_msg.photo:
                                        uploaded_file_ids.append(sent_msg.photo[-1].file_id)
                                        if ENABLE_CLEANUP and file_idx < len(files):
                                            file_path = files[file_idx]
                                            try:
                                                if os.path.exists(file_path):
                                                    os.remove(file_path)
                                                    logger.info(f"[CLEANUP] 🗑️ Cleaned up carousel photo after Telegram upload: {os.path.basename(file_path)}")
                                            except Exception as cleanup_error:
                                                logger.warning(f"[CLEANUP] Failed to cleanup carousel photo {file_path}: {cleanup_error}")
                                    elif sent_msg.video:
                                        uploaded_file_ids.append(sent_msg.video.file_id)
                                        if ENABLE_CLEANUP and file_idx < len(files):
                                            file_path = files[file_idx]
                                            try:
                                                if os.path.exists(file_path):
                                                    os.remove(file_path)
                                                    logger.info(f"[CLEANUP] 🗑️ Cleaned up carousel video after Telegram upload: {os.path.basename(file_path)}")
                                            except Exception as cleanup_error:
                                                logger.warning(f"[CLEANUP] Failed to cleanup carousel video {file_path}: {cleanup_error}")
                                if i == 0 and status_deleted_flag is not None:
                                    await delete_status_message_safe(status_message, status_deleted_flag)
                                continue  # Успешно отправили после повтора
                            except Exception as retry_error:
                                error_msg = f"❌ Ошибка сети при отправке файла после повтора.\n\nОшибка: {str(retry_error)}\n\nПопробуйте позже или проверьте соединение с интернетом."
                                logger.error(f"[MSG] Network error sending carousel chunk after retry: {retry_error}", exc_info=True)
                                await message.answer(error_msg)
                        else:
                            error_msg = f"❌ Ошибка сети при отправке файла.\n\nОшибка: {str(e)}\n\nПопробуйте позже или проверьте соединение с интернетом."
                            logger.error(f"[MSG] Network error sending carousel chunk: {e}", exc_info=True)
                            await message.answer(error_msg)
                        
                        sys.stdout.flush()
                        # Удаляем статусное сообщение при ошибке (если еще не удалено)
                        if status_deleted_flag is not None:
                            await delete_status_message_safe(status_message, status_deleted_flag)
                        # Очистка папки при ошибке отправки
                        if ENABLE_CLEANUP and 'task_dir' in locals() and task_dir:
                            await asyncio.to_thread(get_downloader().cleanup, task_dir)
                    except Exception as e:
                        error_msg = f"❌ Произошла ошибка при отправке файла.\n\nОшибка: {str(e)}"
                        logger.error(f"[MSG] Error sending carousel chunk: {e}", exc_info=True)
                        await message.answer(error_msg)
                        sys.stdout.flush()
                        # Удаляем статусное сообщение при ошибке (если еще не удалено)
                        if status_deleted_flag is not None:
                            await delete_status_message_safe(status_message, status_deleted_flag)
                        # Очистка папки при ошибке отправки
                        if ENABLE_CLEANUP and 'task_dir' in locals() and task_dir:
                            await asyncio.to_thread(get_downloader().cleanup, task_dir)
                
                # Сохраняем все файлы карусели в кэш
                if uploaded_file_ids:
                    db.save_file_to_cache(normalized_url, uploaded_file_ids, carousel_type, message.from_user.id)
                    file_type = carousel_type
                    # Удаляем статусное сообщение, если еще не удалено
                    if status_deleted_flag is not None:
                        await delete_status_message_safe(status_message, status_deleted_flag)
                    # Очистка папки после успешной отправки всех файлов
                    if ENABLE_CLEANUP and 'task_dir' in locals() and task_dir:
                        # Используем небольшую задержку перед очисткой, чтобы убедиться что файлы отправлены
                        await asyncio.sleep(1)
                        asyncio.create_task(asyncio.to_thread(get_downloader().cleanup, task_dir))
        
        # Устанавливаем результат в Future (для других ожидающих запросов)
        result = (uploaded_file_ids, file_type)
        if not future.done():
            future.set_result(result)
        
        # Cleanup уже вызван сразу после успешной отправки файлов выше
            
    except Exception as e:
        logger.error(f"[MSG] Process error for {url}: {e}", exc_info=True)
        sys.stdout.flush()
        
        # Проверяем, поддерживается ли ссылка перед сообщением об ошибке
        if is_supported_url(url):
            error_msg = f"❌ Произошла ошибка при обработке.\n\nОшибка: {str(e)}\n\nURL: {url}"
            await message.answer(error_msg)
        else:
            # Не отвечаем на неподдерживаемые ссылки
            logger.debug(f"Skipping error message for unsupported URL: {url}")
        
        result = ([], None)
        if not future.done():
            future.set_result(result)
        
        # Удаляем статусное сообщение при ошибке
        if status_deleted_flag is not None:
            await delete_status_message_safe(status_message, status_deleted_flag)
        
        # Очистка при ошибке
        if ENABLE_CLEANUP and 'task_dir' in locals():
             asyncio.create_task(asyncio.to_thread(get_downloader().cleanup, task_dir))
    finally:
        # Гарантируем удаление статусного сообщения, если оно еще не удалено
        if status_deleted_flag is not None:
            await delete_status_message_safe(status_message, status_deleted_flag)
        # Удаляем Future из активных загрузок
        # Не удаляем сразу, чтобы параллельные запросы успели получить результат
        # Но для упрощения пока оставим так, так как future уже имеет результат
        pass


@dp.message(F.video)
async def handle_video_file(message: types.Message):
    """Обработка видео файлов - отправляет сообщение с кнопкой конвертировать"""
    if not await is_subscribed(message.from_user.id):
        await message.answer(
            f"👋 Для использования бота подпишитесь на канал @{CHANNEL_USERNAME}",
            reply_markup=get_subscription_keyboard()
        )
        return
    
    db.add_user(message.from_user)
    
    # Сохраняем видео в кэш
    video_file_id = message.video.file_id
    cache_id = db.save_file_to_cache(
        f"user_video_{message.from_user.id}_{message.message_id}",
        [video_file_id],
        'video',
        message.from_user.id
    )
    
    bot_username = await get_bot_username()
    markup = get_convert_keyboard(cache_id=cache_id, bot_username=bot_username)
    await message.answer("✅ Получил файл", reply_markup=markup)

@dp.message(F.audio | F.document)
async def handle_audio_file(message: types.Message):
    """Обработка аудио файлов - отправляет сообщение с кнопкой конвертировать и саммари"""
    if not await is_subscribed(message.from_user.id):
        await message.answer(
            f"👋 Для использования бота подпишитесь на канал @{CHANNEL_USERNAME}",
            reply_markup=get_subscription_keyboard()
        )
        return
    
    db.add_user(message.from_user)
    
    # Определяем file_id в зависимости от типа
    if message.audio:
        file_id = message.audio.file_id
        file_unique_id = message.audio.file_unique_id
    elif message.document:
        # Проверяем, что это аудио файл
        mime_type = message.document.mime_type or ""
        file_name = message.document.file_name or ""
        
        # Поддерживаемые аудио форматы
        audio_formats = ['mp3', 'wav', 'ogg', 'm4a', 'aac', 'flac', 'opus']
        if not any(fmt in mime_type.lower() or fmt in file_name.lower() for fmt in audio_formats):
            return  # Игнорируем неаудио файлы
        
        file_id = message.document.file_id
        file_unique_id = message.document.file_unique_id
    else:
        return
    
    # Сохраняем аудио в кэш
    cache_id = db.save_file_to_cache(
        f"user_audio_{message.from_user.id}_{message.message_id}",
        [file_id],
        'audio',
        message.from_user.id
    )
    
    bot_username = await get_bot_username()
    markup = get_convert_keyboard(cache_id=cache_id, bot_username=bot_username)
    await message.answer("✅ Получил файл", reply_markup=markup)

def add_message_to_batch(user_id, message):
    """Add message to user's batch and process with delay"""
    current_time = time.time()
    
    with batch_lock:
        if user_id not in user_message_batches:
            user_message_batches[user_id] = []
        
        # Check if this is a rapid succession of messages (within 2 seconds)
        is_rapid = (user_id in user_last_message_time and 
                   current_time - user_last_message_time[user_id] < 2.0)
        
        user_message_batches[user_id].append(message)
        user_last_message_time[user_id] = current_time
        
        # Cancel existing timer if any
        if user_id in batch_timers:
            batch_timers[user_id].cancel()
        
        # Special handling for voice messages - always group them
        if message.content_type in ['voice', 'video_note']:
            # For voice messages, always wait a bit to see if more come
            def run_process_batch():
                try:
                    loop = get_main_loop()
                    if loop and loop.is_running():
                        # Use run_coroutine_threadsafe to schedule in main loop
                        future = asyncio.run_coroutine_threadsafe(process_batch(user_id), loop)
                        future.result()  # Wait for completion
                    else:
                        # Fallback: create new loop if main loop not available
                        new_loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(new_loop)
                        new_loop.run_until_complete(process_batch(user_id))
                        new_loop.close()
                except Exception as e:
                    logger.error(f"Error in run_process_batch: {e}", exc_info=True)
            
            timer = threading.Timer(BATCH_TIMEOUT, run_process_batch)
            batch_timers[user_id] = timer
            timer.start()
        # For other messages, check if rapid or batch is full
        elif is_rapid or len(user_message_batches[user_id]) >= BATCH_MAX_SIZE:
            def run_process_batch():
                try:
                    loop = get_main_loop()
                    if loop and loop.is_running():
                        # Use run_coroutine_threadsafe to schedule in main loop
                        future = asyncio.run_coroutine_threadsafe(process_batch(user_id), loop)
                        future.result()  # Wait for completion
                    else:
                        # Fallback: create new loop if main loop not available
                        new_loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(new_loop)
                        new_loop.run_until_complete(process_batch(user_id))
                        new_loop.close()
                except Exception as e:
                    logger.error(f"Error in run_process_batch: {e}", exc_info=True)
            run_process_batch()
        else:
            # Start minimal timer for single messages
            def run_process_batch():
                try:
                    loop = get_main_loop()
                    if loop and loop.is_running():
                        # Use run_coroutine_threadsafe to schedule in main loop
                        future = asyncio.run_coroutine_threadsafe(process_batch(user_id), loop)
                        future.result()  # Wait for completion
                    else:
                        # Fallback: create new loop if main loop not available
                        new_loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(new_loop)
                        new_loop.run_until_complete(process_batch(user_id))
                        new_loop.close()
                except Exception as e:
                    logger.error(f"Error in run_process_batch: {e}", exc_info=True)
            
            timer = threading.Timer(BATCH_TIMEOUT, run_process_batch)
            batch_timers[user_id] = timer
            timer.start()

async def process_batch(user_id):
    """Process all messages in user's batch"""
    with batch_lock:
        if user_id not in user_message_batches or not user_message_batches[user_id]:
            return
        
        messages = user_message_batches[user_id].copy()
        user_message_batches[user_id] = []
        
        if user_id in batch_timers:
            batch_timers[user_id].cancel()
            del batch_timers[user_id]
    
    if not messages:
        return
    
    # Update last message time
    user_last_message_time[user_id] = time.time()
    
    logger.info(f"Processing batch of {len(messages)} messages for user {user_id}")
    
    # Group messages by type
    voice_messages = []
    text_messages = []
    other_messages = []
    
    for msg in messages:
        if msg.content_type in ['voice', 'video_note']:
            voice_messages.append(msg)
        elif msg.content_type == 'text':
            text_messages.append(msg)
        else:
            other_messages.append(msg)
    
    # Sort voice messages by message_id to ensure stable order
    if voice_messages:
        voice_messages.sort(key=lambda msg: msg.message_id)
        await process_voice_batch(voice_messages)
    
    # Process other messages individually
    for msg in other_messages:
        if msg.content_type == 'video':
            # Handle video files
            pass
        elif msg.content_type in ['audio', 'document']:
            # Handle audio files
            pass

async def process_voice_batch(voice_messages):
    """Process multiple voice/video_note messages in parallel"""
    if not voice_messages:
        return
    
    # Log message order for debugging
    message_ids = [msg.message_id for msg in voice_messages]
    logger.info(f"Processing batch of {len(voice_messages)} voice messages")
    logger.info(f"Message IDs: {message_ids}")
    
    # Send initial status message
    first_message = voice_messages[0]
    status_msg = await first_message.answer(f"🎙️ Обрабатываю {len(voice_messages)} сообщений... [░░░░░░░░░░] 0%")
    
    try:
        # Download all files in parallel
        await status_msg.edit_text(f"🎙️ Скачиваю {len(voice_messages)} файлов... [██░░░░░░░░] 20%")
        
        downloaded_files = []
        file_unique_ids = []
        
        for i, message in enumerate(voice_messages):
            if message.content_type == 'voice':
                file_content = message.voice
                input_extension = 'ogg'
            elif message.content_type == 'video_note':
                file_content = message.video_note
                input_extension = 'mp4'
            else:
                continue
            
            file_info = await bot.get_file(file_content.file_id)
            # Используем индекс сообщения в имени файла, чтобы избежать перезаписи при одинаковых file_unique_id
            # и сохранить порядок сообщений
            temp_input_path = os.path.join(tempfile.gettempdir(), f"{file_content.file_unique_id}_{i}_{message.message_id}.{input_extension}")
            await bot.download_file(file_info.file_path, destination=temp_input_path)
            
            downloaded_files.append(temp_input_path)
            file_unique_ids.append(file_content.file_unique_id)
        
        # Convert all files to optimized audio format
        await status_msg.edit_text(f"🎙️ Оптимизирую аудио... [████░░░░░░] 40%")
        
        # Ленивая загрузка pydub только когда нужно обрабатывать аудио
        from pydub import AudioSegment
        
        audio_files = []
        for temp_input_path in downloaded_files:
            audio = AudioSegment.from_file(temp_input_path, format=temp_input_path.split('.')[-1])
            audio = audio.normalize()
            audio = audio.high_pass_filter(80)
            
            temp_audio_path = temp_input_path.replace(f".{temp_input_path.split('.')[-1]}", ".wav")
            audio.set_frame_rate(16000).set_channels(1).set_sample_width(2).export(
                temp_audio_path,
                format="wav",
                parameters=["-ac", "1", "-ar", "16000", "-acodec", "pcm_s16le"]
            )
            audio_files.append(temp_audio_path)
        
        # Выгружаем pydub из памяти после конвертации (перед транскрибацией загрузится speech_recognition)
        unload_heavy_modules()
        
        # Transcribe all files in parallel
        await status_msg.edit_text(f"🎙️ Расшифровываю {len(voice_messages)} сообщений... [██████░░░░] 60%")
        
        # Create futures with their indices to maintain order
        with ThreadPoolExecutor(max_workers=min(len(audio_files), 16)) as executor:
            future_to_index = {}
            for i, audio_file in enumerate(audio_files):
                future = executor.submit(transcribe_audio_segments, audio_file)
                future_to_index[future] = i
            
            # Initialize results list with correct size
            transcribed_texts = [None] * len(audio_files)
            
            # Collect results in any order, but store them at correct indices
            for future in future_to_index:
                try:
                    text = future.result()
                    index = future_to_index[future]
                    transcribed_texts[index] = text
                except Exception as e:
                    logger.error(f"Transcription error: {e}")
                    index = future_to_index[future]
                    transcribed_texts[index] = ""
        
        # Combine all transcriptions
        await status_msg.edit_text(f"📝 Объединяю результаты... [████████░░] 80%")
        
        combined_text = ""
        valid_transcriptions = 0
        for i, (text, message) in enumerate(zip(transcribed_texts, voice_messages)):
            if text and text.strip():  # Only include non-empty transcriptions
                message_type = "Голосовое" if message.content_type == 'voice' else "Видеосообщение"
                combined_text += f"\n\n--- {message_type} {i+1} ---\n{text}"
                valid_transcriptions += 1
        
        combined_text = combined_text.strip()
        
        # Check if any valid transcriptions were found
        if not combined_text or valid_transcriptions == 0:
            await status_msg.edit_text("❌ Не удалось распознать речь ни в одном из сообщений. Попробуйте записать заново.")
            return
        
        # Store transcriptions in database
        user_id = first_message.from_user.id
        for file_unique_id, text in zip(file_unique_ids, transcribed_texts):
            if text and text.strip():  # Only store non-empty transcriptions
                db.save_transcription(file_unique_id, user_id, text)
                logger.info(f"Saved transcription for file_unique_id: {file_unique_id}, user: {user_id}")
        
        await status_msg.edit_text(f"✅ Обработка завершена! [██████████] 100%")
        
        # Send combined result as reply to first voice message
        try:
            if len(combined_text) > MAX_MESSAGE_LENGTH:
                # Create TXT file if too long
                txt_filename = f"transcription_batch_{int(time.time())}.txt"
                txt_path = os.path.join("downloads", txt_filename)
                os.makedirs("downloads", exist_ok=True)
                
                # Правильное склонение для количества сообщений
                count = len(voice_messages)
                if count == 1:
                    message_count_text = "1 сообщение"
                elif count in [2, 3, 4]:
                    message_count_text = f"{count} сообщения"
                else:
                    message_count_text = f"{count} сообщений"
                
                with open(txt_path, 'w', encoding='utf-8') as f:
                    f.write(f"Расшифровка {message_count_text}\n")
                    f.write(f"Дата: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write("="*50 + "\n")
                    f.write(combined_text)
                
                with open(txt_path, 'rb') as f:
                    await first_message.answer_document(
                        FSInputFile(txt_path, filename=txt_filename),
                        caption=f"📝 Расшифровка {message_count_text}\n\nОбщий размер: {len(combined_text)} символов\nФайл создан из-за ограничений Telegram",
                        reply_to_message_id=first_message.message_id
                    )
                
                os.remove(txt_path)
            else:
                # Send as reply to first voice message with summary button
                # Правильное склонение для количества сообщений
                count = len(voice_messages)
                if count == 1:
                    message_count_text = "1 сообщение"
                elif count in [2, 3, 4]:
                    message_count_text = f"{count} сообщения"
                else:
                    message_count_text = f"{count} сообщений"
                
                # Create markup with button
                # For multiple messages, always show batch summary button
                # For single message, show individual summary button if we have file_unique_id
                markup = None
                try:
                    if len(voice_messages) > 1:
                        # For multiple messages, always use batch summary
                        # Передаем file_unique_ids через запятую в callback_data
                        if file_unique_ids and len(file_unique_ids) > 0:
                            file_ids_str = ",".join(file_unique_ids)
                            markup = InlineKeyboardMarkup(inline_keyboard=[
                                [InlineKeyboardButton(text="общее саммари", callback_data=f"batch_summarize:{file_ids_str}")]
                            ])
                        else:
                            # Fallback: используем количество сообщений
                            markup = InlineKeyboardMarkup(inline_keyboard=[
                                [InlineKeyboardButton(text="общее саммари", callback_data=f"batch_summarize:{len(voice_messages)}")]
                            ])
                        logger.info(f"Created batch summary button for {len(voice_messages)} messages, file_unique_ids count: {len(file_unique_ids)}")
                    elif file_unique_ids and len(file_unique_ids) > 0:
                        # For single voice message, use individual summary
                        markup = InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="саммари", callback_data=f"summarize:{file_unique_ids[0]}")]
                        ])
                        logger.info(f"Created individual summary button for file_unique_id: {file_unique_ids[0]}")
                    else:
                        logger.warning(f"No file_unique_ids available (count: {len(file_unique_ids) if file_unique_ids else 0}), sending without button")
                except Exception as markup_error:
                    logger.error(f"Error creating markup: {markup_error}", exc_info=True)
                    markup = None
                
                # Отправляем сообщение с retry логикой
                # Проверяем длину сообщения и разбиваем если нужно
                full_text = f"<b>📝 Расшифровка {message_count_text}:</b>\n{combined_text}"
                
                if len(full_text) > MAX_MESSAGE_LENGTH:
                    # Разбиваем на части
                    logger.info(f"Message too long ({len(full_text)} chars), splitting into parts...")
                    words = combined_text.split()
                    current_message = f"<b>📝 Расшифровка {message_count_text}:</b>\n"
                    messages = []
                    
                    for word in words:
                        test_message = current_message + (" " + word) if current_message.strip() != f"<b>📝 Расшифровка {message_count_text}:</b>" else word
                        if len(test_message) <= MAX_MESSAGE_LENGTH:
                            current_message = test_message
                        else:
                            if current_message.strip() != f"<b>📝 Расшифровка {message_count_text}:</b>":
                                messages.append(current_message)
                            current_message = f"<b>📝 Расшифровка {message_count_text}:</b>\n{word}"
                    
                    if current_message.strip() != f"<b>📝 Расшифровка {message_count_text}:</b>":
                        messages.append(current_message)
                    
                    # Отправляем все части
                    for i, msg_text in enumerate(messages):
                        try:
                            if i == len(messages) - 1 and markup:
                                # Последнее сообщение получает кнопку
                                await first_message.answer(msg_text, parse_mode="HTML", reply_markup=markup, reply_to_message_id=first_message.message_id if i == 0 else None)
                            else:
                                await first_message.answer(msg_text, parse_mode="HTML", reply_to_message_id=first_message.message_id if i == 0 else None)
                        except Exception as part_error:
                            logger.error(f"Error sending part {i+1}/{len(messages)}: {part_error}")
                            # Пробуем отправить без HTML
                            try:
                                plain_text = msg_text.replace("<b>", "").replace("</b>", "")
                                await first_message.answer(plain_text, reply_to_message_id=first_message.message_id if i == 0 else None)
                            except Exception as final_error:
                                logger.error(f"Failed to send even plain text: {final_error}")
                else:
                    # Отправляем как одно сообщение с retry логикой
                    answer_kwargs = {
                        "text": full_text,
                        "parse_mode": "HTML",
                        "reply_to_message_id": first_message.message_id
                    }
                    if markup:
                        answer_kwargs["reply_markup"] = markup
                    
                    # Retry логика для отправки сообщения
                    max_retries = 3
                    retry_delay = 1
                    for attempt in range(max_retries):
                        try:
                            await first_message.answer(**answer_kwargs)
                            break  # Успешно отправлено
                        except Exception as send_error:
                            if attempt < max_retries - 1:
                                logger.warning(f"Error sending transcription (attempt {attempt + 1}/{max_retries}): {send_error}, retrying in {retry_delay}s...")
                                await asyncio.sleep(retry_delay)
                                retry_delay *= 2  # Увеличиваем задержку
                            else:
                                # Последняя попытка - отправляем без форматирования и кнопок
                                logger.error(f"Failed to send transcription after {max_retries} attempts: {send_error}")
                                try:
                                    # Пробуем отправить простой текст без HTML и кнопок
                                    await first_message.answer(
                                        f"📝 Расшифровка {message_count_text}:\n{combined_text}",
                                        reply_to_message_id=first_message.message_id
                                    )
                                except Exception as final_error:
                                    logger.error(f"Final send attempt also failed: {final_error}")
                                    raise
        except Exception as send_error:
            logger.error(f"Error sending transcription: {send_error}", exc_info=True)
            # Правильное склонение для количества сообщений
            count = len(voice_messages)
            if count == 1:
                message_count_text = "1 сообщение"
            elif count in [2, 3, 4]:
                message_count_text = f"{count} сообщения"
            else:
                message_count_text = f"{count} сообщений"
            try:
                await first_message.answer(
                    f"📝 Расшифровка {message_count_text} готова, но произошла ошибка при отправке полного текста.\n\nОшибка: {str(send_error)[:200]}",
                    reply_to_message_id=first_message.message_id
                )
            except Exception as final_error:
                logger.error(f"Failed to send error message: {final_error}")
        
        # Clean up status message
        try:
            await status_msg.delete()
        except Exception as e:
            logger.warning(f"Could not delete status message: {e}")
        
        # Clean up all temporary files
        for file_path in downloaded_files + audio_files:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception as e:
                    logger.warning(f"Failed to remove temp file {file_path}: {e}")
    
    except Exception as e:
        logger.error(f"Error in process_voice_batch: {e}", exc_info=True)
        error_message = f"❌ Ошибка при обработке батча голосовых сообщений: {str(e)}"
        await status_msg.edit_text(error_message)

@dp.message(F.photo)
async def handle_photo(message: types.Message):
    """Handle photo messages - check for QR codes"""
    # Проверяем подписку
    if not await is_subscribed(message.from_user.id):
        await message.answer(
            "👋 для использования бота нужно подписаться на канал:",
            reply_markup=get_subscription_keyboard()
        )
        return
    
    db.add_user(message.from_user)
    
    try:
        # Get the largest photo (last in the list)
        photo = message.photo[-1]
        
        # Download photo
        file_info = await bot.get_file(photo.file_id)
        downloaded_file = await bot.download_file(file_info.file_path)
        
        # Read photo data - aiogram returns bytes
        if isinstance(downloaded_file, bytes):
            photo_data = downloaded_file
        elif hasattr(downloaded_file, 'read'):
            photo_data = downloaded_file.read()
        else:
            photo_data = bytes(downloaded_file)
        
        # Try to decode QR code
        qr_text = decode_qr_code(photo_data)
        
        if qr_text:
            await message.answer(f"📱 **QR-код расшифрован:**\n\n`{qr_text}`", parse_mode="Markdown")
        else:
            # If no QR code found, check if there's a caption with URL or just ignore
            # (we don't want to interfere with normal photo handling)
            pass
            
    except Exception as e:
        logger.error(f"Error in handle_photo (QR decode): {e}")
        # Don't send error message - just log it, as photo might be for other purposes

@dp.message(F.voice | F.video_note)
async def handle_voice_or_video_note(message: types.Message):
    """Обработка голосовых сообщений и видеокружков - добавляет в батч для обработки"""
    if not await is_subscribed(message.from_user.id):
        await message.answer(
            f"👋 Для использования бота подпишитесь на канал @{CHANNEL_USERNAME}",
            reply_markup=get_subscription_keyboard()
        )
        return
    
    db.add_user(message.from_user)
    
    # Добавляем сообщение в батч для обработки
    add_message_to_batch(message.from_user.id, message)

@dp.callback_query(F.data.startswith("summarize:"))
async def handle_summarize_callback(callback: CallbackQuery):
    """Обработка кнопки саммари для голосовых сообщений - только саммари, без повторной отправки расшифровки"""
    if not await is_subscribed(callback.from_user.id):
        await callback.answer("❌ Подпишитесь на канал для использования бота", show_alert=True)
        return
    
    try:
        file_unique_id = callback.data.split(":")[1]
        user_id = callback.from_user.id
        
        # Получаем расшифровку из базы данных
        transcribed_text = db.get_transcription(file_unique_id, user_id)
        
        if not transcribed_text:
            await callback.answer("❌ Не удалось найти текст для саммари", show_alert=True)
            return
        
        await callback.answer("📝 Создаю саммари...")
        
        # Отправляем только саммари (расшифровка уже была отправлена ранее)
        summary_msg = await callback.message.answer("📝 Делаю саммари... [░░░░░░░░░░] 0%")
        await summary_msg.edit_text("📝 Делаю саммари... [███████░░░] 70%")
        
        summary = await generate_summary(transcribed_text)
        
        if summary.startswith("❌"):
            await summary_msg.edit_text(summary)
        else:
            formatted_summary = f"<b>📝 Саммари:</b>\n\n{summary}"
            await summary_msg.edit_text(formatted_summary, parse_mode="HTML")
            
    except Exception as e:
        logger.error(f"Error in handle_summarize_callback: {e}", exc_info=True)
        await callback.answer(f"❌ Ошибка при создании саммари: {str(e)}", show_alert=True)

@dp.callback_query(F.data.startswith("batch_summarize:"))
async def handle_batch_summarize_callback(callback: CallbackQuery):
    """Обработка кнопки батч-саммари для нескольких голосовых сообщений - только саммари, без повторной отправки расшифровки"""
    if not await is_subscribed(callback.from_user.id):
        await callback.answer("❌ Подпишитесь на канал для использования бота", show_alert=True)
        return
    
    try:
        user_id = callback.from_user.id
        
        # Получаем file_unique_ids из callback_data
        # Формат: batch_summarize:file_id1,file_id2,file_id3
        file_unique_ids_str = callback.data.split(":", 1)[1] if ":" in callback.data else ""
        file_unique_ids = [fid.strip() for fid in file_unique_ids_str.split(",") if fid.strip()] if file_unique_ids_str else []
        
        if not file_unique_ids:
            # Fallback: получаем все расшифровки пользователя из базы данных
            user_transcriptions_dict = db.get_user_transcriptions(user_id)
            if not user_transcriptions_dict:
                await callback.answer("❌ Не найдено текстов для саммари", show_alert=True)
                return
            
            # Собираем все расшифровки
            user_transcriptions = []
            for file_id, text in user_transcriptions_dict.items():
                if text and len(text.strip()) > 10:
                    user_transcriptions.append(text)
        else:
            # Получаем расшифровки только для указанных file_unique_ids
            user_transcriptions = []
            for file_unique_id in file_unique_ids:
                text = db.get_transcription(file_unique_id, user_id)
                if text and len(text.strip()) > 10:
                    user_transcriptions.append(text)
        
        if not user_transcriptions:
            await callback.answer("❌ Не найдено текстов для саммари", show_alert=True)
            return
        
        await callback.answer("📝 Создаю общий саммари...")
        
        # Отправляем только саммари (расшифровки уже были отправлены ранее)
        # Объединяем с разделителями для лучшей структуры (как в process_voice_batch)
        combined_text = ""
        for i, text in enumerate(user_transcriptions):
            if text and text.strip():
                combined_text += f"\n\n--- Сообщение {i+1} ---\n{text}"
        
        combined_text = combined_text.strip()
        if not combined_text:
            await callback.answer("❌ Не удалось объединить тексты для саммари", show_alert=True)
            return
        # Правильное склонение для количества сообщений
        count = len(user_transcriptions)
        if count == 1:
            message_count_text = "1 сообщение"
        elif count in [2, 3, 4]:
            message_count_text = f"{count} сообщения"
        else:
            message_count_text = f"{count} сообщений"
        
        summary_msg = await callback.message.answer(f"📝 Делаю общий саммари из {message_count_text}...")
        
        summary = await generate_summary(combined_text)
        
        if summary.startswith("❌"):
            await summary_msg.edit_text(summary)
        else:
            formatted_summary = f"<b>📝 Общий саммари ({message_count_text}):</b>\n\n{summary}"
            await summary_msg.edit_text(formatted_summary, parse_mode="HTML")
            
    except Exception as e:
        logger.error(f"Error in handle_batch_summarize_callback: {e}", exc_info=True)
        await callback.answer(f"❌ Ошибка при создании общего саммари: {str(e)}", show_alert=True)

@dp.message(F.text)
async def handle_message(message: types.Message):
    # Проверяем подписку
    if not await is_subscribed(message.from_user.id):
        await message.answer(
            "👋 для использования бота нужно подписаться на канал:",
            reply_markup=get_subscription_keyboard()
        )
        return
    
    # Регистрируем юзера
    db.add_user(message.from_user)
    
    text = message.text.strip()
    
    # Проверяем, является ли текст file_id (пробуем скачать файл)
    # File_id обычно длинная строка, может содержать дефисы и подчеркивания
    if len(text) > 20 and (text.startswith('BAAC') or text.startswith('CAA') or 
                           text.startswith('AgAC') or text.startswith('BQAC') or
                           text.startswith('AwAC') or '_' in text or '-' in text):
        try:
            # Пробуем получить файл по file_id
            file = await bot.get_file(text)
            if file:
                # Это валидный file_id, скачиваем файл
                status_msg = await message.answer("📥 Скачиваю файл...")
                log_resource_usage(f"Processing file_id: {text[:20]}...")
                
                # Скачиваем файл
                file_path = f"downloads/temp_{uuid.uuid4()}/{file.file_path.split('/')[-1]}"
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                
                await bot.download_file(file.file_path, file_path)
                log_resource_usage(f"Downloaded file from file_id: {file_path}")
                
                # Определяем тип файла по file_id префиксу и расширению
                file_ext = os.path.splitext(file_path)[1].lower()
                # Голосовые сообщения (file_id начинается с AwACAgI) обычно имеют .oga расширение или без расширения
                is_voice = text.startswith('AwACAgI')
                if is_voice and not file_ext:
                    file_ext = '.oga'  # Голосовые сообщения в формате Ogg Opus
                file_size = os.path.getsize(file_path)
                file_size_mb = file_size / (1024 * 1024)
                
                sent_msg = None
                file_type = None
                file_id_result = None
                
                # Отправляем файл пользователю
                bot_username = await get_bot_username()
                uploaded_file_ids = []
                
                # Обрабатываем голосовые сообщения отдельно
                if is_voice or file_ext in ['.oga']:
                    # Голосовое сообщение - отправляем как voice, но сохраняем как audio для конвертации
                    file_type = 'audio'  # Сохраняем как audio, чтобы можно было конвертировать
                    try:
                        # Пробуем отправить напрямую по file_id как voice
                        sent_msg = await message.answer_voice(text, caption=f"🎤 Голосовое сообщение ({file_size_mb:.2f} МБ)")
                        if sent_msg and sent_msg.voice:
                            uploaded_file_ids.append(sent_msg.voice.file_id)
                            file_id_result = sent_msg.voice.file_id
                            logger.info(f"[VOICE] ✅ Sent voice message via file_id: {file_id_result}")
                    except Exception as voice_error:
                        # Если не получилось отправить как voice, скачиваем и отправляем как audio
                        logger.warning(f"[VOICE] Failed to send as voice, trying as audio: {voice_error}")
                        sent_msg = await message.answer_audio(FSInputFile(file_path), caption=f"🎤 Голосовое сообщение ({file_size_mb:.2f} МБ)")
                        if sent_msg and sent_msg.audio:
                            uploaded_file_ids.append(sent_msg.audio.file_id)
                            file_id_result = sent_msg.audio.file_id
                            logger.info(f"[VOICE] ✅ Sent voice as audio: {file_id_result}")
                elif file_ext in ['.mp4', '.mov', '.avi', '.webm']:
                    file_type = 'video'
                    # Генерируем thumbnail для отправляемого видео
                    thumbnail_path = None
                    try:
                        # Генерируем обложку на 1-й секунде (или 0.0, если видео короткое)
                        thumbnail_path = await asyncio.to_thread(
                            get_downloader().generate_thumbnail,
                            file_path,
                            os.path.dirname(file_path),
                            1.0  # time_offset
                        )
                    except Exception as thumb_error:
                        logger.warning(f"Failed to generate thumbnail: {thumb_error}")
                    
                    video_kwargs = {'video': FSInputFile(file_path), 'caption': f"📹 Видео файл ({file_size_mb:.2f} МБ)", 'supports_streaming': True}
                    if thumbnail_path and os.path.exists(thumbnail_path):
                        video_kwargs['thumbnail'] = FSInputFile(thumbnail_path)
                    sent_msg = await message.answer_video(**video_kwargs)
                    if sent_msg and sent_msg.video:
                        uploaded_file_ids.append(sent_msg.video.file_id)
                        file_id_result = sent_msg.video.file_id
                elif file_ext in ['.mp3', '.wav', '.ogg', '.oga', '.m4a', '.aac', '.opus', '.flac']:
                    file_type = 'audio'
                    sent_msg = await message.answer_audio(FSInputFile(file_path), caption=f"🎵 Аудио файл ({file_size_mb:.2f} МБ)")
                    if sent_msg and sent_msg.audio:
                        uploaded_file_ids.append(sent_msg.audio.file_id)
                        file_id_result = sent_msg.audio.file_id
                elif file_ext in ['.jpg', '.jpeg', '.png', '.webp', '.gif']:
                    file_type = 'photo'
                    sent_msg = await message.answer_photo(FSInputFile(file_path), caption=f"🖼️ Изображение ({file_size / 1024:.2f} КБ)")
                    if sent_msg and sent_msg.photo:
                        uploaded_file_ids.append(sent_msg.photo[-1].file_id)
                        file_id_result = sent_msg.photo[-1].file_id
                else:
                    file_type = 'document'
                    sent_msg = await message.answer_document(FSInputFile(file_path), caption=f"📄 Файл ({file_size_mb:.2f} МБ)")
                    if sent_msg and sent_msg.document:
                        uploaded_file_ids.append(sent_msg.document.file_id)
                        file_id_result = sent_msg.document.file_id
                
                log_resource_usage(f"File sent from file_id: type={file_type}, file_id={file_id_result}, uploaded_count={len(uploaded_file_ids)}")
                
                # Сохраняем в кэш и добавляем кнопку конвертации для видео и аудио
                if uploaded_file_ids and file_type and (file_type == 'video' or file_type == 'audio'):
                    # Используем оригинальный file_id как URL для кэша (уникальный идентификатор)
                    cache_url = f"file_id:{text}"
                    log_resource_usage(f"Before saving to cache: file_type={file_type}, file_ids={len(uploaded_file_ids)}")
                    cache_id = db.save_file_to_cache(cache_url, uploaded_file_ids, file_type, message.from_user.id)
                    log_resource_usage(f"Saved to cache: cache_id={cache_id}, type={file_type}")
                    
                    if cache_id and sent_msg:
                        log_resource_usage(f"Adding convert button for {file_type} (cache_id={cache_id}, message_id={sent_msg.message_id})")
                        try:
                            await bot.edit_message_reply_markup(
                                chat_id=message.chat.id,
                                message_id=sent_msg.message_id,
                                reply_markup=get_convert_keyboard(cache_id=cache_id, bot_username=bot_username)
                            )
                            logger.info(f"[BUTTON] ✅ Added convert button for {file_type} from file_id (cache_id={cache_id}, message_id={sent_msg.message_id})")
                        except Exception as e:
                            logger.error(f"[BUTTON] ❌ Failed to add convert button for {file_type}: {e}", exc_info=True)
                        log_resource_usage(f"After adding convert button for {file_type}")
                    else:
                        logger.warning(f"[BUTTON] ⚠️ Cannot add button: cache_id={cache_id}, sent_msg={sent_msg is not None}, file_type={file_type}")
                else:
                    logger.warning(f"[BUTTON] ⚠️ Skipping button: uploaded_file_ids={len(uploaded_file_ids) if uploaded_file_ids else 0}, file_type={file_type}")
                
                # Удаляем статусное сообщение "📥 Скачиваю файл..."
                try:
                    await status_msg.delete()
                    logger.info(f"[STATUS] ✅ Deleted status message")
                except Exception as e:
                    logger.warning(f"[STATUS] Failed to delete status message: {e}")
                
                # Удаляем временный файл
                try:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    if os.path.exists(os.path.dirname(file_path)):
                        os.rmdir(os.path.dirname(file_path))
                except Exception as cleanup_error:
                    logger.warning(f"[CLEANUP] Failed to cleanup temp file: {cleanup_error}")
                
                log_resource_usage(f"Completed file_id processing: {file_type}")
                return
        except Exception as e:
            # Если не получилось скачать - это не file_id, продолжаем обработку как обычного текста
            logger.debug(f"Text is not a valid file_id: {e}")
    
    # Ищем все ссылки
    urls = re.findall(URL_PATTERN, message.text)
    
    if not urls:
        # Если это не ссылка, просто просим отправить ссылку
        await message.answer("📎 отправь мне ссылку из поддерживаемых мною платформ или file_id файла из Telegram")
        return
    
    # Нормализуем ссылки - добавляем https:// если нет протокола
    normalized_urls = []
    for url in urls:
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        normalized_urls.append(url)
    urls = normalized_urls
    
    # Фильтруем только поддерживаемые ссылки и исключаем ссылки на самого бота
    bot_username = await get_bot_username()
    filtered_urls = []
    unsupported_urls = []
    
    for url in urls:
        # Пропускаем ссылки на самого бота (это команды start, их обрабатывает другой обработчик)
        if f't.me/{bot_username}' in url.lower() or f'telegram.me/{bot_username}' in url.lower():
            continue
        
        # Проверяем, поддерживается ли ссылка
        if is_supported_url(url):
            filtered_urls.append(url)
        else:
            unsupported_urls.append(url)
    
    # Если есть неподдерживаемые ссылки, сообщаем об этом
    if unsupported_urls:
        await message.answer(
            "📎 отправь ссылку из поддерживаемых мною платформ:\n\n"
            "📱 Instagram (посты, reels, stories, tv)\n"
            "🎵 TikTok\n"
            "🎥 YouTube (видео и shorts)\n"
            "🎵 SoundCloud"
        )
        logger.info(f"Unsupported URLs from user {message.from_user.id}: {unsupported_urls}")
        return
    
    # Если нет поддерживаемых ссылок, выходим
    if not filtered_urls:
        return
    
    # Используем только поддерживаемые ссылки
    urls = filtered_urls

    # Дедупликация ссылок (по нормализованному виду)
    unique_urls = []
    seen_normalized = set()
    for url in urls:
        # Грубая нормализация для удаления дублей в одном сообщении
        # Более точная проверка будет внутри process_single_url после раскрытия сокращенных ссылок
        norm = normalize_url(url)
        if norm not in seen_normalized:
            seen_normalized.add(norm)
            unique_urls.append(url)

    if len(urls) != len(unique_urls):
        logger.info(f"Filtered duplicates: {len(urls)} -> {len(unique_urls)} URLs")

    logger.info(f"Found {len(unique_urls)} unique URL(s) in message from user {message.from_user.id}: {unique_urls}")
    
    # Отправляем "Скачиваю..." только в личных чатах
    status_message = None
    status_deleted_flag = {'deleted': False}
    if message.chat.type == 'private':
        try:
            status_message = await message.answer("⏳ скачиваю...")
        except Exception as e:
            logger.error(f"Error sending status message: {e}")
    
    # Запускаем задачи параллельно и ждем их выполнения
    tasks = []
    for url in unique_urls:
        # Создаем задачу для каждой ссылки
        logger.info(f"Starting processing for URL: {url}")
        tasks.append(asyncio.create_task(process_single_url(message, url, status_message, status_deleted_flag)))
    
    # Ждем выполнения всех задач, чтобы обработать все ссылки
    if tasks:
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"Error processing URLs: {e}", exc_info=True)

async def cleanup_expired_files_periodically():
    """Периодически очищает истекшие файлы из БД"""
    while True:
        try:
            await asyncio.sleep(3600)  # Проверяем каждый час
            deleted_count = await asyncio.to_thread(db.cleanup_expired_files)
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} expired file records")
        except Exception as e:
            logger.error(f"Error in cleanup task: {e}")

# Глобальная переменная для отслеживания времени последней активности
_last_activity_time = None
_restart_cooldown = 0  # Время последнего перезапуска (для предотвращения частых перезапусков)

async def cleanup_downloads_when_idle():
    """Периодически очищает папку downloads, если нет активных загрузок"""
    global _last_activity_time, _restart_cooldown
    
    while True:
        try:
            await asyncio.sleep(300)  # Проверяем каждые 5 минут
            
            # Обновляем время последней активности, если есть активные загрузки
            if len(active_downloads) > 0:
                _last_activity_time = time.time()
            
            # Проверяем, есть ли активные загрузки
            if len(active_downloads) == 0:
                # Нет активных загрузок - можно чистить папку downloads
                downloads_dir = "downloads"
                if os.path.exists(downloads_dir):
                    try:
                        # Получаем список всех папок в downloads
                        items = os.listdir(downloads_dir)
                        if items:
                            logger.info(f"[CLEANUP] No active downloads, cleaning {len(items)} item(s) from downloads folder")
                            
                            # Удаляем все папки и файлы в downloads
                            for item in items:
                                item_path = os.path.join(downloads_dir, item)
                                try:
                                    if os.path.isdir(item_path):
                                        import shutil
                                        shutil.rmtree(item_path, ignore_errors=True)
                                        logger.info(f"[CLEANUP] Removed directory: {item}")
                                    else:
                                        os.remove(item_path)
                                        logger.info(f"[CLEANUP] Removed file: {item}")
                                except Exception as e:
                                    logger.warning(f"[CLEANUP] Failed to remove {item}: {e}")
                            
                            logger.info(f"[CLEANUP] ✅ Cleaned downloads folder (no active downloads)")
                    except Exception as e:
                        logger.error(f"[CLEANUP] Error cleaning downloads folder: {e}")
            else:
                logger.debug(f"[CLEANUP] Skipping cleanup - {len(active_downloads)} active download(s)")
        except Exception as e:
            logger.error(f"[CLEANUP] Error in cleanup_downloads_when_idle: {e}")

async def smart_restart_monitor():
    """Умный мониторинг памяти и перезапуск бота при необходимости"""
    global _last_activity_time, _restart_cooldown
    
    try:
        import psutil
    except ImportError:
        logger.warning("[RESTART] psutil not installed, smart restart disabled. Install with: pip install psutil")
        return
    
    while True:
        try:
            await asyncio.sleep(60)  # Проверяем каждую минуту
            
            # Получаем информацию о памяти процесса
            process = psutil.Process()
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)
            
            # Получаем общую доступную память системы
            system_memory = psutil.virtual_memory()
            memory_percent = system_memory.percent
            available_mb = system_memory.available / (1024 * 1024)
            
            logger.debug(f"[RESTART] Memory: {memory_mb:.1f} MB (process), {memory_percent:.1f}% system, {available_mb:.1f} MB available")
            
            # Проверяем условия для перезапуска
            should_restart = False
            restart_reason = ""
            
            current_time = time.time()
            time_since_last_activity = current_time - _last_activity_time if _last_activity_time else float('inf')
            time_since_last_restart = current_time - _restart_cooldown
            
            # Условие 1: Нет активных загрузок И прошло более 10 минут с последней активности
            if len(active_downloads) == 0 and time_since_last_activity > 600:  # 10 минут
                should_restart = True
                restart_reason = f"No active downloads for {int(time_since_last_activity/60)} minutes"
            
            # Условие 2: Память процесса превышает 150 MB И нет активных загрузок
            elif len(active_downloads) == 0 and memory_mb > 150:
                should_restart = True
                restart_reason = f"Process memory {memory_mb:.1f} MB > 150 MB, no active downloads"
            
            # Условие 3: Системная память > 85% И нет активных загрузок
            elif len(active_downloads) == 0 and memory_percent > 85:
                should_restart = True
                restart_reason = f"System memory {memory_percent:.1f}% > 85%, no active downloads"
            
            # Проверяем cooldown (не перезапускаемся чаще чем раз в 30 минут)
            if should_restart and time_since_last_restart < 1800:  # 30 минут
                logger.debug(f"[RESTART] Skipping restart (cooldown): {int((1800 - time_since_last_restart)/60)} min remaining")
                should_restart = False
            
            if should_restart:
                logger.warning(f"[RESTART] 🔄 Initiating smart restart: {restart_reason}")
                logger.warning(f"[RESTART] Process memory: {memory_mb:.1f} MB, System memory: {memory_percent:.1f}%")
                logger.warning(f"[RESTART] Active downloads: {len(active_downloads)}")
                
                # Graceful shutdown: останавливаем polling
                try:
                    await dp.stop_polling()
                    logger.info("[RESTART] Stopped polling gracefully")
                except Exception as e:
                    logger.warning(f"[RESTART] Error stopping polling: {e}")
                
                # Небольшая задержка для завершения текущих операций
                await asyncio.sleep(2)
                
                # Завершаем процесс - systemd перезапустит его
                logger.warning("[RESTART] Exiting process for systemd restart...")
                os._exit(0)  # Немедленное завершение
                
        except Exception as e:
            logger.error(f"[RESTART] Error in smart_restart_monitor: {e}")
            await asyncio.sleep(60)  # Ждем перед следующей попыткой

async def main():
    # Сохраняем основной event loop для batch processing
    set_main_loop(asyncio.get_event_loop())
    
    # Определяем username бота при старте
    await get_bot_username()
    logger.info(f"Bot started with username: @{_bot_username}")
    
    # Инициализируем время последней активности
    global _last_activity_time, _restart_cooldown
    _last_activity_time = time.time()
    _restart_cooldown = time.time()
    
    # ОТЛОЖЕННЫЙ запуск фоновых задач - только через 5 минут после старта
    # Это позволяет боту запуститься с минимальной нагрузкой
    async def delayed_background_tasks():
        await asyncio.sleep(300)  # 5 минут задержка
        logger.info("Starting delayed background tasks...")
        
        # Запускаем задачу периодической очистки истекших файлов
        asyncio.create_task(cleanup_expired_files_periodically())
        logger.info("Started periodic cleanup task for expired files")
        
        # Запускаем задачу очистки папки downloads при отсутствии активных загрузок
        asyncio.create_task(cleanup_downloads_when_idle())
        logger.info("Started periodic cleanup task for downloads folder")
        
        # Запускаем задачу умного мониторинга памяти и перезапуска (ленивая загрузка psutil)
        asyncio.create_task(smart_restart_monitor())
        logger.info("Started smart restart monitor")
        
        # Первоначальная очистка при старте (только через 5 минут)
        try:
            deleted_count = await asyncio.to_thread(get_db().cleanup_expired_files)
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} expired file records")
        except Exception as e:
            logger.error(f"Error cleaning up expired files: {e}")
    
    # Запускаем отложенные задачи
    asyncio.create_task(delayed_background_tasks())
    
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

def run_flask_api():
    """Запуск Flask API в отдельном потоке"""
    import sys
    import os
    import time
    
    # Небольшая задержка, чтобы бот успел инициализироваться
    time.sleep(2)
    
    # Добавляем путь к api.py
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    
    try:
        from api import app
        logger.info("🚀 Starting Flask API on port 5030...")
        sys.stdout.flush()
        # Используем waitress для production или werkzeug для dev
        try:
            from waitress import serve
            serve(app, host='0.0.0.0', port=5030, threads=4)
        except ImportError:
            # Если waitress не установлен, используем стандартный Flask сервер
            app.run(host='0.0.0.0', port=5030, debug=False, use_reloader=False, threaded=True)
    except Exception as e:
        logger.error(f"❌ Failed to start Flask API: {e}", exc_info=True)
        sys.stdout.flush()
        # Продолжаем работу бота даже если API не запустился

if __name__ == "__main__":
    # Запускаем Flask API в отдельном потоке (если включено)
    if ENABLE_API:
        api_thread = threading.Thread(target=run_flask_api, daemon=True)
        api_thread.start()
        logger.info("Flask API thread started")
    else:
        logger.info("Flask API disabled in config (ENABLE_API=False)")
    
    # Запускаем бота
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Bot crashed: {e}", exc_info=True)
