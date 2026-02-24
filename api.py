# Ленивая загрузка Flask и других тяжёлых модулей
# Flask загружается только если ENABLE_API=True
import os
import logging
import uuid
import shutil
import requests
import re
import sys
import json
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from config import BOT_TOKEN, ENABLE_API
from database import Database

# Ленивая загрузка тяжёлых модулей - не импортируем на уровне модуля:
# - Flask, Flask-CORS (тяжёлые, только если ENABLE_API=True)
# - speech_recognition (тяжёлый, только при транскрибации)
# - pydub (тяжёлый, только при обработке аудио)
# - Downloader (создаём только при первом использовании)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Ленивая инициализация Flask - только если ENABLE_API=True
_app = None
def get_app():
    """Ленивая загрузка Flask - создаём только если API включен"""
    global _app
    if _app is None:
        from flask import Flask
        from flask_cors import CORS
        _app = Flask(__name__)
        _app.secret_key = os.urandom(24)  # For sessions
        CORS(_app)
    return _app

# In-memory storage for sessions (in production, use Redis or database)
sessions_data = {}

# Ленивая инициализация Downloader
_downloader = None
def get_downloader():
    """Ленивая загрузка Downloader - создаём только когда нужно скачивать"""
    global _downloader
    if _downloader is None:
        from downloader import Downloader
        _downloader = Downloader()
    return _downloader

# Database лёгкий, можно инициализировать сразу
db = Database()

# Создаём app - Flask загружается лениво внутри get_app()
# Если ENABLE_API=False, создаём заглушку чтобы декораторы @app.route не падали
if ENABLE_API:
    app = get_app()
else:
    # Создаём заглушку app с методом route, который просто возвращает функцию как есть
    class DummyApp:
        def route(self, *args, **kwargs):
            return lambda f: f
    app = DummyApp()

# SSO Configuration
SSO_AUTH_URL = "https://auth.dreampartners.online"
SSO_CLIENT_ID = "down_downloader"
SSO_CLIENT_SECRET = os.environ.get('SSO_CLIENT_SECRET', '')
SSO_REDIRECT_URI = "https://download.dreampartners.online/callback"

# URL normalization function (same as in bot.py)
def normalize_url(url: str) -> str:
    """Нормализует URL для корректного сравнения (убирает пробелы, лишние параметры)"""
    if not url:
        return url
    
    # Убираем пробелы
    url = url.strip()
    
    # Добавляем https:// если нет протокола
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    try:
        # Парсим URL
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

# Constants from bot.py
API_TOKEN = "io-v2-eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJvd25lciI6IjY0ODczZDQ3LWQxYzMtNDA5My1iMDAyLTY4YWFiZmQ5YjJjNSIsImV4cCI6NDkxODQ3NjYwNH0.iAwhAprPStFvgrktcGEmvd5J3W7S2o6QxNwt0H2eVTZQxnV-ZE1FUfA5oQ7QJZAyTozsxUYwnIKTsI7PwkUecA"
PROMPT = """Ты эксперт по созданию кратких саммари. Создай пронумерованный список ключевых пунктов из расшифровки голосового сообщения. ВАЖНО: Это расшифровка голосового сообщения, возможны ошибки распознавания речи. Поняй смысл по контексту и молча исправь/переформулируй текст естественно, чтобы он был понятным и логичным. Правила: 1) Начни сразу со списка без вводных фраз 2) Каждый пункт - одна ключевая мысль или факт 3) Определи пол говорящего по контексту и СТРОГО соблюдай его во всех пунктах, сохраняя первое лицо (я, у меня, мой/моя/моё) 4) Включи 5-10 самых важных пунктов 5) Используй только цифры с точкой (1. 2. 3.) 6) Пиши кратко и по существу 7) Сохраняй хронологию событий если она важна 8) Исправляй очевидные ошибки распознавания речи, сохраняя смысл 9) Переформулируй неясные фразы для лучшего понимания 10) Исправляй искаженные слова по смыслу 11) Сохраняй естественность речи и логику повествования 12) Если речь неразборчива или слишком короткая, укажи это в саммари."""

# Ленивая инициализация Speech Recognition
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

def transcribe_single_segment(audio_path):
    """Transcribe a single audio file using Google Speech Recognition"""
    # Ленивая загрузка speech_recognition только когда нужно
    import speech_recognition as sr
    
    try:
        recognizer = get_recognizer()
        with sr.AudioFile(audio_path) as source:
            # Adjust for ambient noise
            recognizer.adjust_for_ambient_noise(source, duration=0.5)
            # Record audio
            audio_data = recognizer.record(source)
            # Recognize speech
            try:
                text = recognizer.recognize_google(audio_data, language='ru-RU', show_all=False)
                if text:
                    logger.info(f"Successfully transcribed {len(text)} characters")
                    return text
            except sr.UnknownValueError:
                logger.info("Speech not recognized")
                return ""
            except sr.RequestError as e:
                logger.error(f"Request error in speech recognition: {e}")
                return ""
    except Exception as e:
        logger.error(f"Error in transcribe_single_segment for {audio_path}: {e}")
        return ""
    return ""

def transcribe_audio_segments(audio_path, max_segment_duration=30):
    """Transcribe audio by splitting it into segments for better accuracy"""
    # Ленивая загрузка pydub только когда нужно
    from pydub import AudioSegment
    
    try:
        # Convert to wav if needed (AudioFile needs wav/aiff/flac)
        if not audio_path.endswith('.wav'):
            # Use pydub to convert
             # Check if ffmpeg is available
            audio = AudioSegment.from_file(audio_path)
            wav_path = audio_path + ".wav"
            audio.export(wav_path, format="wav")
            audio_path = wav_path
        else:
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

def generate_summary_sync(text):
    """Generate summary using API (Synchronous version)"""
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
        response = requests.post(url, headers=headers, json=data)
        
        if response.status_code != 200:
            logger.error(f"API error response: {response.text}")
            return f"❌ Ошибка API: {response.text}"
        
        response_data = response.json()
        if 'choices' in response_data and len(response_data['choices']) > 0:
            bot_response = response_data['choices'][0]['message']['content']
            # Cleanup response
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

@app.route('/health', methods=['GET'])
def health_check():
    """Проверка работоспособности API"""
    from flask import jsonify
    return jsonify({"status": "ok", "service": "download-api"}), 200

@app.route('/api', methods=['GET'])
def api_info():
    """
    Информация об API и доступных эндпоинтах.
    
    Возвращает:
        JSON с описанием всех эндпоинтов API
    """
    from flask import request, jsonify
    base_url = request.host_url.rstrip('/')
    
    api_docs = {
        "service": "Download API",
        "version": "2.0",
        "description": "API для скачивания медиа из Instagram, TikTok, YouTube, SoundCloud и других платформ",
        "endpoints": {
            "/download": {
                "method": "POST",
                "description": "Скачивает медиа по URL. Если файл в кэше - скачивает из Telegram (быстро). Если нет - скачивает с исходного сервиса.",
                "request": {
                    "url": "string (required) - URL для скачивания",
                    "download": "boolean (optional) - если true, возвращает файл напрямую"
                },
                "response": {
                    "status": "success/error",
                    "cached": "boolean - был ли файл в кэше",
                    "files": "array - список файлов",
                    "is_carousel": "boolean - является ли каруселью",
                    "carousel_count": "number - количество файлов в карусели"
                },
                "example": f"{base_url}/download"
            },
            "/api/download/<file_id>": {
                "method": "GET",
                "description": "Скачивает файл по ID из истории сессии",
                "parameters": {
                    "file_id": "string (required) - ID файла из истории"
                },
                "response": "Файл для скачивания",
                "example": f"{base_url}/api/download/123e4567-e89b-12d3-a456-426614174000"
            },
            "/files/<path:filename>": {
                "method": "GET",
                "description": "Отдает файл для скачивания по пути",
                "parameters": {
                    "filename": "string (required) - путь к файлу"
                },
                "response": "Файл для скачивания",
                "example": f"{base_url}/files/downloads/task_id/video.mp4"
            },
            "/transcribe": {
                "method": "POST",
                "description": "Расшифровка аудио/видео. Можно передать URL или загрузить файл",
                "request": {
                    "url": "string (optional) - URL для скачивания и расшифровки",
                    "file": "file (optional) - загружаемый файл"
                },
                "response": {
                    "status": "success/error",
                    "text": "string - расшифрованный текст"
                }
            },
            "/summary": {
                "method": "POST",
                "description": "Генерация саммари из текста",
                "request": {
                    "text": "string (required) - текст для саммари"
                },
                "response": {
                    "status": "success/error",
                    "summary": "string - саммари текста"
                }
            },
            "/api/process": {
                "method": "POST",
                "description": "Полный цикл: Скачивание -> Расшифровка -> Саммари",
                "request": {
                    "url": "string (required) - URL для обработки"
                },
                "response": {
                    "status": "success/error",
                    "file": "object - информация о файле",
                    "transcription": "string - расшифровка",
                    "summary": "string - саммари"
                }
            },
            "/api/history": {
                "method": "GET",
                "description": "Получить историю скачиваний текущей сессии",
                "response": {
                    "status": "success",
                    "history": "array - список файлов"
                }
            },
            "/api/history/<file_id>": {
                "method": "DELETE",
                "description": "Удалить файл из истории",
                "parameters": {
                    "file_id": "string (required) - ID файла"
                }
            },
            "/api/telegram/upload": {
                "method": "POST",
                "description": "Загрузить файл в Telegram и получить bot link",
                "request": {
                    "file_id": "string (required) - ID файла из истории"
                },
                "response": {
                    "status": "success/error",
                    "bot_link": "string - ссылка на бота",
                    "cache_id": "number - ID в кэше",
                    "telegram_file_id": "string - file_id в Telegram"
                }
            }
        },
        "supported_platforms": [
            "Instagram (посты, reels, stories, IGTV)",
            "TikTok (видео, фото)",
            "YouTube (видео, shorts)",
            "SoundCloud (аудио)"
        ],
        "features": [
            "Кэширование файлов в Telegram для быстрого доступа",
            "Автоматическое скачивание из кэша если файл уже был скачан",
            "Поддержка каруселей (несколько файлов)",
            "Расшифровка аудио/видео",
            "Генерация саммари",
            "История скачиваний"
        ]
    }
    
    return jsonify(api_docs), 200

def get_or_create_session():
    """Get or create session ID"""
    from flask import session
    if 'session_id' not in session:
        session['session_id'] = str(uuid.uuid4())
        sessions_data[session['session_id']] = {
            'history': [],
            'created_at': datetime.now().isoformat()
        }
    return session['session_id']

def get_session_history():
    """Get history for current session"""
    session_id = get_or_create_session()
    return sessions_data.get(session_id, {}).get('history', [])

def add_to_history(file_info):
    """Add file to session history (avoid duplicates by normalized_url)"""
    session_id = get_or_create_session()
    if session_id not in sessions_data:
        sessions_data[session_id] = {'history': []}
    
    normalized_url = file_info.get('normalized_url')
    
    # Check if this URL already exists in history
    # If exists, update it instead of creating duplicate
    if normalized_url:
        existing_index = None
        for idx, item in enumerate(sessions_data[session_id]['history']):
            if item.get('normalized_url') == normalized_url:
                existing_index = idx
                break
        
        if existing_index is not None:
            # Update existing entry
            file_entry = {
                'id': sessions_data[session_id]['history'][existing_index]['id'],  # Keep same ID
                'filename': file_info['filename'],
                'url': file_info['url'],
                'path': file_info.get('path', ''),
                'size': file_info.get('size', 0),
                'added_at': datetime.now().isoformat(),  # Update timestamp
                'telegram_file_id': file_info.get('telegram_file_id'),
                'bot_link': file_info.get('bot_link'),
                'cache_id': file_info.get('cache_id'),
                'normalized_url': normalized_url,
                'media_type': file_info.get('media_type'),
                'is_cached': file_info.get('is_cached', False),
                'is_carousel': file_info.get('is_carousel', False),
                'carousel_count': file_info.get('carousel_count', 1),
                'carousel_files': file_info.get('carousel_files', []),
                'carousel_file_ids': file_info.get('carousel_file_ids', [])
            }
            # Move to beginning (most recent)
            sessions_data[session_id]['history'].pop(existing_index)
            sessions_data[session_id]['history'].insert(0, file_entry)
            return file_entry
    
    # New entry
    file_entry = {
        'id': str(uuid.uuid4()),
        'filename': file_info['filename'],
        'url': file_info['url'],
        'path': file_info.get('path', ''),
        'size': file_info.get('size', 0),
        'added_at': datetime.now().isoformat(),
        'telegram_file_id': file_info.get('telegram_file_id'),  # Store Telegram file_id if available
        'bot_link': file_info.get('bot_link'),  # Store bot link if available
        'cache_id': file_info.get('cache_id'),  # Store cache_id if available
        'normalized_url': normalized_url,  # Store normalized URL
        'media_type': file_info.get('media_type'),  # Store media type
        'is_cached': file_info.get('is_cached', False),  # Mark if cached (True) or downloaded (False)
        'is_carousel': file_info.get('is_carousel', False),  # Mark if carousel
        'carousel_count': file_info.get('carousel_count', 1),  # Number of files in carousel
        'carousel_files': file_info.get('carousel_files', []),  # Store all carousel files
        'carousel_file_ids': file_info.get('carousel_file_ids', [])  # Store all file_ids for carousel
    }
    sessions_data[session_id]['history'].insert(0, file_entry)  # Add to beginning
    return file_entry

def remove_from_history(file_id):
    """Remove file from session history"""
    session_id = get_or_create_session()
    if session_id in sessions_data:
        sessions_data[session_id]['history'] = [
            f for f in sessions_data[session_id]['history'] 
            if f['id'] != file_id
        ]
        return True
    return False

def clear_history():
    """Clear all history for current session"""
    session_id = get_or_create_session()
    if session_id in sessions_data:
        sessions_data[session_id]['history'] = []
        return True
    return False

@app.route('/', methods=['GET'])
def index():
    from flask import render_template
    get_or_create_session()  # Initialize session
    return render_template('index.html')

@app.route('/docs', methods=['GET'])
def docs():
    from flask import render_template
    return render_template('docs.html')

def download_file_from_telegram(file_id, output_path):
    """
    Скачивает файл из Telegram по file_id.
    Это быстрее, чем скачивать заново с исходного сервиса.
    
    Args:
        file_id: Telegram file_id
        output_path: Путь для сохранения файла
    
    Returns:
        True если успешно, False если ошибка
    """
    try:
        # Получаем информацию о файле
        response = requests.get(
            f'https://api.telegram.org/bot{BOT_TOKEN}/getFile',
            params={'file_id': file_id},
            timeout=10
        )
        
        if response.status_code != 200:
            logger.error(f"Failed to get file info: {response.status_code}")
            return False
        
        result = response.json()
        if not result.get('ok'):
            logger.error(f"Telegram API error: {result.get('description', 'Unknown')}")
            return False
        
        file_path_telegram = result['result']['file_path']
        
        # Скачиваем файл
        download_url = f'https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path_telegram}'
        file_response = requests.get(download_url, stream=True, timeout=300)
        
        if file_response.status_code != 200:
            logger.error(f"Failed to download file: {file_response.status_code}")
            return False
        
        # Сохраняем файл
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'wb') as f:
            for chunk in file_response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        logger.info(f"✅ Downloaded file from Telegram: {output_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error downloading file from Telegram: {e}", exc_info=True)
        return False

@app.route('/download', methods=['POST'])
def api_download():
    """
    Скачивает медиа по URL и возвращает файлы.
    
    Логика работы:
    1. Проверяет кэш - если файл есть в кэше (file_id в Telegram), скачивает его из Telegram (быстрее)
    2. Если нет в кэше - скачивает с исходного сервиса (Instagram, TikTok, YouTube и т.д.)
    3. Возвращает файлы для скачивания
    
    Request body:
        {
            "url": "https://www.instagram.com/reel/...",
            "download": true  // опционально, если true - возвращает файл напрямую
        }
    
    Response:
        {
            "status": "success",
            "cached": true/false,
            "files": [
                {
                    "id": "uuid",
                    "filename": "video.mp4",
                    "url": "https://.../files/...",
                    "size": 1234567,
                    "telegram_file_id": "AAQ...",
                    "bot_link": "https://t.me/bot?start=file_123",
                    "cache_id": 123,
                    "media_type": "video"
                }
            ],
            "is_carousel": false,
            "carousel_count": 1
        }
    """
    from flask import request, jsonify, send_file
    data = request.json
    if not data or 'url' not in data:
        return jsonify({"error": "URL is required"}), 400
    
    url = data['url']
    direct_download = data.get('download', False)  # Если true - возвращаем файл напрямую
    
    # Normalize URL (add https:// if missing, clean parameters)
    normalized_url = normalize_url(url)
    logger.info(f"Normalized URL: {url} -> {normalized_url}")
    
    try:
        # Check cache first (like in bot)
        cached = db.get_cached_file(normalized_url)
        if cached:
            file_ids_str, media_type = cached
            logger.info(f"Found in cache: {normalized_url}, type: {media_type}")
            
            # Parse file_ids
            if isinstance(file_ids_str, list):
                file_ids = file_ids_str
            else:
                try:
                    file_ids = json.loads(file_ids_str) if file_ids_str.startswith('[') else [file_ids_str]
                except:
                    file_ids = [file_ids_str]
            
            # Get cache_id for bot link
            cache_id = db.get_cache_id_by_url(normalized_url)
            
            # Get bot username
            try:
                response = requests.get(f'https://api.telegram.org/bot{BOT_TOKEN}/getMe')
                if response.status_code == 200:
                    bot_info = response.json()
                    if bot_info.get('ok'):
                        bot_username = bot_info['result']['username']
                        bot_link = f"https://t.me/{bot_username}?start=file_{cache_id}" if cache_id else None
                    else:
                        bot_link = None
                else:
                    bot_link = None
            except:
                bot_link = None
            
            # Файл в кэше - скачиваем из Telegram (быстрее чем заново с сервиса)
            is_cached_carousel = len(file_ids) > 1
            result_files = []
            
            # Скачиваем файлы из Telegram
            task_id = str(uuid.uuid4())
            task_dir = os.path.join("downloads", task_id)
            os.makedirs(task_dir, exist_ok=True)
            
            for idx, file_id in enumerate(file_ids):
                # Определяем расширение файла по media_type
                if media_type == 'video':
                    ext = '.mp4'
                elif media_type == 'audio':
                    ext = '.mp3'
                elif media_type == 'photo':
                    ext = '.jpg'
                else:
                    ext = '.mp4'
                
                filename = f"file_{idx}{ext}" if is_cached_carousel else f"file{ext}"
                output_path = os.path.join(task_dir, filename)
                
                # Скачиваем файл из Telegram
                if download_file_from_telegram(file_id, output_path):
                    if os.path.exists(output_path):
                        rel_path = os.path.relpath(output_path, os.getcwd())
                        file_url = f"{request.host_url.rstrip('/')}/files/{rel_path.replace(os.sep, '/')}"
                        
                        result_files.append({
                            "path": output_path,
                            "filename": filename,
                            "url": file_url,
                            "size": os.path.getsize(output_path),
                            "telegram_file_id": file_id,
                            "bot_link": bot_link,
                            "cache_id": cache_id,
                            "normalized_url": normalized_url,
                            "media_type": media_type,
                            "is_cached": True
                        })
                else:
                    logger.warning(f"Failed to download file {idx} from Telegram, will try original source")
                    # Если не удалось скачать из Telegram - скачиваем заново
                    is_cached_carousel = False
                    result_files = []
                    break
            
            # Если успешно скачали из кэша
            if result_files:
                # Добавляем в историю
                files_with_ids = []
                if is_cached_carousel and len(result_files) > 1:
                    carousel_entry = {
                        "filename": f"Карусель: {len(result_files)} файлов",
                        "url": result_files[0]['url'] if result_files else '',
                        "path": result_files[0]['path'] if result_files else '',
                        "size": sum(f.get('size', 0) for f in result_files),
                        "telegram_file_id": file_ids[0] if file_ids else None,
                        "bot_link": bot_link,
                        "cache_id": cache_id,
                        "normalized_url": normalized_url,
                        "media_type": media_type,
                        "is_carousel": True,
                        "carousel_files": result_files,
                        "carousel_count": len(result_files),
                        "is_cached": True
                    }
                    try:
                        file_entry = add_to_history(carousel_entry)
                        carousel_entry['id'] = file_entry['id']
                        files_with_ids.append(carousel_entry)
                    except Exception as e:
                        logger.error(f"Failed to add to history: {e}")
                        carousel_entry['id'] = str(uuid.uuid4())
                        files_with_ids.append(carousel_entry)
                else:
                    for file_info in result_files:
                        try:
                            file_entry = add_to_history(file_info)
                            file_info['id'] = file_entry['id']
                            files_with_ids.append(file_info)
                        except Exception as e:
                            logger.error(f"Failed to add to history: {e}")
                            file_info['id'] = str(uuid.uuid4())
                            files_with_ids.append(file_info)
                
                # Если запрошено прямое скачивание - возвращаем файл
                if direct_download and result_files:
                    if is_cached_carousel and len(result_files) > 1:
                        # Для карусели возвращаем первый файл (можно улучшить - сделать ZIP)
                        return send_file(result_files[0]['path'], as_attachment=True)
                    else:
                        return send_file(result_files[0]['path'], as_attachment=True)
                
                return jsonify({
                    "status": "success",
                    "cached": True,
                    "files": files_with_ids,
                    "is_carousel": is_cached_carousel,
                    "carousel_count": len(result_files) if is_cached_carousel else 1,
                    "normalized_url": normalized_url,
                    "cache_id": cache_id,
                    "message": "Файл скачан из кэша Telegram (быстро)"
                })
            
            # Если не удалось скачать из кэша - продолжаем как обычно (скачиваем заново)
            logger.info("Failed to download from cache, downloading from source...")
            
            if is_cached_carousel:
                # Return as one carousel entry
                cached_file_info = {
                    "id": f"cached_{cache_id}",
                    "filename": f"Карусель: {len(file_ids)} файлов",
                    "url": bot_link or "#",
                    "size": 0,
                    "telegram_file_id": file_ids[0] if file_ids else None,
                    "bot_link": bot_link,
                    "cache_id": cache_id,
                    "normalized_url": normalized_url,
                    "media_type": media_type,
                    "is_carousel": True,
                    "carousel_count": len(file_ids),
                    "carousel_file_ids": file_ids,
                    "is_cached": True  # Mark as cached
                }
                cached_files = [cached_file_info]
            else:
                # Single file
                cached_file_info = {
                    "id": f"cached_{cache_id}",
                    "filename": f"Кэшированный {media_type}",
                    "url": bot_link or "#",
                    "size": 0,
                    "telegram_file_id": file_ids[0] if file_ids else None,
                    "bot_link": bot_link,
                    "cache_id": cache_id,
                    "normalized_url": normalized_url,
                    "media_type": media_type,
                    "is_cached": True  # Mark as cached
                }
                cached_files = [cached_file_info]
            
            # Add cached files to session history
            files_with_ids = []
            for cached_file in cached_files:
                try:
                    # Add to history with is_cached flag
                    file_entry = add_to_history(cached_file)
                    cached_file['id'] = file_entry['id']  # Use history ID
                    files_with_ids.append(cached_file)
                except Exception as history_error:
                    logger.error(f"Failed to add cached file to history: {history_error}")
                    # Still return file even if history fails
                    files_with_ids.append(cached_file)
            
            return jsonify({
                "status": "success",
                "cached": True,
                "files": files_with_ids,
                "is_carousel": is_cached_carousel,
                "carousel_count": len(file_ids) if is_cached_carousel else 1,
                "normalized_url": normalized_url,
                "cache_id": cache_id,
                "message": "Файл найден в кэше. Используйте ссылку для открытия в боте."
            })
        
        # Not in cache, download
        logger.info(f"Not in cache, downloading: {normalized_url}")
        files, task_dir = get_downloader().download(url)
        
        # Prepare response and upload to Telegram immediately
        result_files = []
        base_url = request.host_url.rstrip('/')
        SERVICE_GROUP_ID = -4990421216
        
        # Get bot username for bot links
        try:
            bot_response = requests.get(f'https://api.telegram.org/bot{BOT_TOKEN}/getMe')
            bot_username = None
            if bot_response.status_code == 200:
                bot_info = bot_response.json()
                if bot_info.get('ok'):
                    bot_username = bot_info['result']['username']
        except:
            bot_username = None
        
        # Determine if this is a carousel (multiple files of same type)
        is_carousel = len(files) > 1
        carousel_sent = False  # Track if carousel was successfully sent
        
        # Determine media type from first file
        first_file_ext = os.path.splitext(files[0])[1].lower()
        if first_file_ext in ['.mp4', '.mov', '.avi', '.webm']:
            media_type = 'video'
        elif first_file_ext in ['.mp3', '.wav', '.ogg', '.m4a', '.aac']:
            media_type = 'audio'
        elif first_file_ext in ['.jpg', '.jpeg', '.png', '.webp']:
            media_type = 'photo'
        else:
            media_type = 'document'
        
        # If carousel, use sendMediaGroup; otherwise send individually
        file_ids_list = []
        result_files = []
        
        if is_carousel and media_type in ['photo', 'video']:
            # Carousel - send via sendMediaGroup
            logger.info(f"📸 Carousel detected: {len(files)} files, type: {media_type}")
            
            try:
                # Send media group in chunks of 10 (Telegram limit)
                chunk_size = 10
                for chunk_start in range(0, len(files), chunk_size):
                    chunk_files = files[chunk_start:chunk_start + chunk_size]
                    
                    # Prepare multipart form data for sendMediaGroup
                    # Format: media[0][type]=photo&media[0][media]=<file>...
                    form_data = {'chat_id': str(SERVICE_GROUP_ID)}
                    files_data = {}
                    
                    opened_files = []  # Track opened files for cleanup
                    try:
                        for idx, file_path in enumerate(chunk_files):
                            file_ext = os.path.splitext(file_path)[1].lower()
                            
                            # Determine type for this file
                            if file_ext in ['.jpg', '.jpeg', '.png', '.webp']:
                                file_media_type = 'photo'
                            elif file_ext in ['.mp4', '.mov', '.avi', '.webm']:
                                file_media_type = 'video'
                            else:
                                file_media_type = 'document'
                            
                            # Set type in form data
                            form_data[f'media[{idx}][type]'] = file_media_type
                            
                            # Open file for upload
                            file_obj = open(file_path, 'rb')
                            opened_files.append(file_obj)
                            filename = os.path.basename(file_path)
                            
                            # Determine MIME type
                            if file_media_type == 'photo':
                                mime_type = 'image/jpeg'
                            elif file_media_type == 'video':
                                mime_type = 'video/mp4'
                            else:
                                mime_type = 'application/octet-stream'
                            
                            # Add file to files_data
                            files_data[f'media[{idx}][media]'] = (filename, file_obj, mime_type)
                        
                        # Send media group
                        response = requests.post(
                            f'https://api.telegram.org/bot{BOT_TOKEN}/sendMediaGroup',
                            data=form_data,
                            files=files_data,
                            timeout=300
                        )
                    finally:
                        # Close all opened files
                        for file_obj in opened_files:
                            try:
                                file_obj.close()
                            except:
                                pass
                    
                    if response.status_code == 200:
                        result = response.json()
                        if result.get('ok'):
                            messages = result['result']
                            for msg in messages:
                                if 'photo' in msg:
                                    file_ids_list.append(msg['photo'][-1]['file_id'])
                                elif 'video' in msg:
                                    file_ids_list.append(msg['video']['file_id'])
                            logger.info(f"✅ Carousel chunk uploaded: {len(messages)} files")
                            carousel_sent = True
                            # НЕ удаляем файлы - они нужны для скачивания через веб-интерфейс
                        else:
                            logger.error(f"Telegram API error: {result.get('description', 'Unknown error')}")
                    else:
                        logger.error(f"Failed to send carousel chunk: {response.status_code} - {response.text}")
                        
            except Exception as carousel_error:
                logger.error(f"Failed to upload carousel to Telegram: {carousel_error}", exc_info=True)
                # Fallback: try sending individually
                carousel_sent = False
                file_ids_list = []
                logger.info("Falling back to individual file upload")
        
        # If not carousel or carousel failed, send files individually
        if not is_carousel or not carousel_sent or not file_ids_list:
            file_ids_list = []  # Reset if carousel failed
            for file_path in files:
                file_ext = os.path.splitext(file_path)[1].lower()
                
                # Determine media type for this file
                if file_ext in ['.mp4', '.mov', '.avi', '.webm']:
                    file_media_type = 'video'
                elif file_ext in ['.mp3', '.wav', '.ogg', '.m4a', '.aac']:
                    file_media_type = 'audio'
                elif file_ext in ['.jpg', '.jpeg', '.png', '.webp']:
                    file_media_type = 'photo'
                else:
                    file_media_type = 'document'
                
                # Upload to Telegram immediately to get file_id
                telegram_file_id = None
                try:
                    with open(file_path, 'rb') as f:
                        files_data = {}
                        data_form = {'chat_id': SERVICE_GROUP_ID}
                        
                        if file_media_type == 'video':
                            files_data['video'] = f
                            response = requests.post(
                                f'https://api.telegram.org/bot{BOT_TOKEN}/sendVideo',
                                files=files_data,
                                data=data_form,
                                timeout=300
                            )
                        elif file_media_type == 'audio':
                            files_data['audio'] = f
                            response = requests.post(
                                f'https://api.telegram.org/bot{BOT_TOKEN}/sendAudio',
                                files=files_data,
                                data=data_form,
                                timeout=300
                            )
                        elif file_media_type == 'photo':
                            files_data['photo'] = f
                            response = requests.post(
                                f'https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto',
                                files=files_data,
                                data=data_form,
                                timeout=300
                            )
                        else:
                            files_data['document'] = f
                            response = requests.post(
                                f'https://api.telegram.org/bot{BOT_TOKEN}/sendDocument',
                                files=files_data,
                                data=data_form,
                                timeout=300
                            )
                        
                        if response.status_code == 200:
                            result = response.json()
                            if result.get('ok'):
                                msg = result['result']
                                if 'video' in msg:
                                    telegram_file_id = msg['video']['file_id']
                                elif 'audio' in msg:
                                    telegram_file_id = msg['audio']['file_id']
                                elif 'photo' in msg:
                                    telegram_file_id = msg['photo'][-1]['file_id']  # Highest quality
                                elif 'document' in msg:
                                    telegram_file_id = msg['document']['file_id']
                                
                                if telegram_file_id:
                                    file_ids_list.append(telegram_file_id)
                                    logger.info(f"✅ Uploaded to Telegram, file_id: {telegram_file_id}")
                                    # НЕ удаляем файл - он нужен для скачивания через веб-интерфейс
                except Exception as upload_error:
                    logger.error(f"Failed to upload to Telegram: {upload_error}")
                    # Continue anyway - file is downloaded
                
                # Make path relative to downloads folder for serving
                rel_path = os.path.relpath(file_path, os.getcwd())
                file_url = f"{base_url}/files/{rel_path.replace(os.sep, '/')}"
                
                result_files.append({
                    "path": file_path,
                    "filename": os.path.basename(file_path),
                    "url": file_url,
                    "size": os.path.getsize(file_path),
                    "telegram_file_id": telegram_file_id,
                    "normalized_url": normalized_url,
                    "media_type": file_media_type
                })
        elif carousel_sent and file_ids_list:
            # Carousel was sent successfully - prepare result_files
            for i, file_path in enumerate(files):
                rel_path = os.path.relpath(file_path, os.getcwd())
                file_url = f"{base_url}/files/{rel_path.replace(os.sep, '/')}"
                
                telegram_file_id = file_ids_list[i] if i < len(file_ids_list) else None
                
                file_ext = os.path.splitext(file_path)[1].lower()
                if file_ext in ['.mp4', '.mov', '.avi', '.webm']:
                    file_media_type = 'video'
                elif file_ext in ['.mp3', '.wav', '.ogg', '.m4a', '.aac']:
                    file_media_type = 'audio'
                elif file_ext in ['.jpg', '.jpeg', '.png', '.webp']:
                    file_media_type = 'photo'
                else:
                    file_media_type = 'document'
                
                result_files.append({
                    "path": file_path,
                    "filename": os.path.basename(file_path),
                    "url": file_url,
                    "size": os.path.getsize(file_path),
                    "telegram_file_id": telegram_file_id,
                    "normalized_url": normalized_url,
                    "media_type": file_media_type
                })
        
        # Save to cache (like in bot)
        cache_id = None
        if file_ids_list:
            # For carousels, use 'photo' or 'video' as media_type
            cache_media_type = media_type if not is_carousel else ('photo' if media_type == 'photo' else 'video')
            cache_id = db.save_file_to_cache(normalized_url, file_ids_list, cache_media_type, 0)
            logger.info(f"✅ Saved to cache: {normalized_url}, cache_id: {cache_id}, file_ids: {len(file_ids_list)}")
            
            # Generate bot links for all files
            if bot_username and cache_id:
                bot_link = f"https://t.me/{bot_username}?start=file_{cache_id}"
                for file_info in result_files:
                    file_info['bot_link'] = bot_link
                    file_info['cache_id'] = cache_id
        
        # Add to session history and add IDs
        files_with_ids = []
        
        # If carousel, save as one entry in history
        if is_carousel and carousel_sent and len(result_files) > 1:
            # Create one carousel entry
            carousel_entry = {
                "filename": f"Карусель: {len(result_files)} файлов",
                "url": result_files[0]['url'] if result_files else '',
                "path": result_files[0]['path'] if result_files else '',
                "size": sum(f.get('size', 0) for f in result_files),
                "telegram_file_id": file_ids_list[0] if file_ids_list else None,
                "normalized_url": normalized_url,
                "media_type": media_type,
                "is_carousel": True,
                "carousel_files": result_files,  # Store all files
                "carousel_count": len(result_files)
            }
            if bot_username and cache_id:
                carousel_entry['bot_link'] = f"https://t.me/{bot_username}?start=file_{cache_id}"
                carousel_entry['cache_id'] = cache_id
            
            try:
                file_entry = add_to_history(carousel_entry)
                # Add carousel entry to response
                carousel_entry['id'] = file_entry['id']
                files_with_ids.append(carousel_entry)
            except Exception as history_error:
                logger.error(f"Failed to add carousel to history: {history_error}")
                if 'id' not in carousel_entry:
                    carousel_entry['id'] = str(uuid.uuid4())
                files_with_ids.append(carousel_entry)
        else:
            # Single file - save normally
            for file_info in result_files:
                try:
                    file_entry = add_to_history(file_info)
                    file_info['id'] = file_entry['id']  # Add ID to response
                    files_with_ids.append(file_info)
                except Exception as history_error:
                    logger.error(f"Failed to add to history: {history_error}")
                    # Still add file to response even if history fails
                    if 'id' not in file_info:
                        file_info['id'] = str(uuid.uuid4())
                    files_with_ids.append(file_info)
        
        # Если запрошено прямое скачивание - возвращаем файл
        if direct_download and result_files:
            if is_carousel and carousel_sent and len(result_files) > 1:
                # Для карусели возвращаем первый файл
                return send_file(result_files[0]['path'], as_attachment=True)
            elif result_files:
                return send_file(result_files[0]['path'], as_attachment=True)
        
        return jsonify({
            "status": "success",
            "cached": False,
            "files": files_with_ids,
            "task_id": os.path.basename(task_dir),
            "normalized_url": normalized_url,
            "telegram_file_ids": file_ids_list,
            "cache_id": cache_id,
            "is_carousel": is_carousel and carousel_sent,
            "carousel_count": len(files) if (is_carousel and carousel_sent) else 1,
            "message": "Файл скачан с исходного сервиса"
        })
        
    except Exception as e:
        logger.error(f"Download failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/files/<path:filename>', methods=['GET'])
def serve_file(filename):
    """
    Отдает файл для скачивания.
    
    Параметры:
        filename: Путь к файлу относительно корня (например: downloads/task_id/file.mp4)
    
    Возвращает:
        Файл с правильными заголовками для скачивания
    """
    from flask import jsonify, send_from_directory
    try:
        # Проверяем существование файла
        file_path = os.path.join('.', filename)
        if not os.path.exists(file_path):
            return jsonify({"error": "File not found"}), 404
        
        # Отдаем файл
        response = send_from_directory('.', filename)
        
        # Устанавливаем правильные заголовки для скачивания
        filename_only = os.path.basename(filename)
        response.headers['Content-Disposition'] = f'attachment; filename="{filename_only}"'
        response.headers['Content-Type'] = 'application/octet-stream'
        
        return response
    except Exception as e:
        logger.error(f"Error serving file {filename}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/download/<file_id>', methods=['GET'])
def download_file_by_id(file_id):
    """
    Скачивает файл по ID из истории сессии.
    
    Параметры:
        file_id: ID файла из истории
    
    Возвращает:
        Файл для скачивания или JSON с ошибкой
    """
    from flask import jsonify, send_file
    try:
        session_id = get_or_create_session()
        
        # Ищем файл в истории
        file_info = None
        if session_id in sessions_data:
            for item in sessions_data[session_id]['history']:
                if item['id'] == file_id:
                    file_info = item
                    break
        
        if not file_info:
            return jsonify({"error": "File not found in history"}), 404
        
        # Если это карусель - возвращаем первый файл или ZIP
        if file_info.get('is_carousel') and file_info.get('carousel_files'):
            # Для карусели возвращаем первый файл (можно улучшить - сделать ZIP)
            file_path = file_info['carousel_files'][0].get('path')
            if not file_path or not os.path.exists(file_path):
                return jsonify({"error": "Carousel file not found on disk"}), 404
            return send_file(file_path, as_attachment=True)
        
        # Обычный файл
        file_path = file_info.get('path')
        if not file_path:
            # Если нет пути, но есть file_id - скачиваем из Telegram
            telegram_file_id = file_info.get('telegram_file_id')
            if telegram_file_id:
                # Создаем временный файл
                task_id = str(uuid.uuid4())
                task_dir = os.path.join("downloads", "temp_" + task_id)
                os.makedirs(task_dir, exist_ok=True)
                
                # Определяем расширение
                media_type = file_info.get('media_type', 'video')
                ext = '.mp4' if media_type == 'video' else '.mp3' if media_type == 'audio' else '.jpg'
                output_path = os.path.join(task_dir, f"file{ext}")
                
                if download_file_from_telegram(telegram_file_id, output_path):
                    return send_file(output_path, as_attachment=True)
                else:
                    return jsonify({"error": "Failed to download from Telegram"}), 500
            
            return jsonify({"error": "File path not available"}), 404
        
        if not os.path.exists(file_path):
            return jsonify({"error": "File not found on disk"}), 404
        
        return send_file(file_path, as_attachment=True)
        
    except Exception as e:
        logger.error(f"Error downloading file by ID: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/preview/<path:filename>', methods=['GET'])
def get_preview(filename):
    """Generate preview thumbnail for video"""
    from flask import request, jsonify
    try:
        file_path = os.path.join('.', filename)
        if not os.path.exists(file_path):
            return jsonify({"error": "File not found"}), 404
        
        # Check if it's a video
        ext = os.path.splitext(file_path)[1].lower()
        if ext not in ['.mp4', '.mov', '.avi', '.webm']:
            return jsonify({"error": "Not a video file"}), 400
        
        # For now, return the video URL with timestamp parameter
        # In production, you could generate actual thumbnail using ffmpeg
        base_url = request.host_url.rstrip('/')
        return jsonify({
            "preview_url": f"{base_url}/files/{filename}?t=1"
        })
    except Exception as e:
        logger.error(f"Preview generation failed: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/transcribe', methods=['POST'])
def api_transcribe():
    """
    Transcribe audio/video.
    Can provide 'url' (download & transcribe) or upload file.
    """
    from flask import request, jsonify
    # Check if URL provided
    if request.content_type and 'application/json' in request.content_type:
        data = request.json
        if 'url' in data:
            # Download first
            try:
                files, task_dir = get_downloader().download(data['url'])
                # Pick the first media file
                target_file = files[0]
                
                # Transcribe
                text = transcribe_audio_segments(target_file)
                
                # Clean up if desired, or keep it
                # get_downloader().cleanup(task_dir) 
                
                return jsonify({
                    "status": "success",
                    "text": text,
                    "source_file": target_file
                })
            except Exception as e:
                return jsonify({"status": "error", "message": str(e)}), 500

    # Check for file upload
    if 'file' in request.files:
        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "No file selected"}), 400
            
        # Save temp file
        task_id = str(uuid.uuid4())
        temp_dir = os.path.join("downloads", "temp_" + task_id)
        os.makedirs(temp_dir, exist_ok=True)
        file_path = os.path.join(temp_dir, file.filename)
        file.save(file_path)
        
        try:
            text = transcribe_audio_segments(file_path)
            # Cleanup upload
            shutil.rmtree(temp_dir, ignore_errors=True)
            
            return jsonify({
                "status": "success",
                "text": text
            })
        except Exception as e:
            shutil.rmtree(temp_dir, ignore_errors=True)
            return jsonify({"status": "error", "message": str(e)}), 500
            
    return jsonify({"error": "Provide 'url' in JSON or upload 'file'"}), 400

@app.route('/summary', methods=['POST'])
def api_summary():
    """
    Generate summary from text.
    Request body: { "text": "..." }
    """
    from flask import request, jsonify
    data = request.json
    if not data or 'text' not in data:
        return jsonify({"error": "Text is required"}), 400
        
    text = data['text']
    try:
        summary = generate_summary_sync(text)
        return jsonify({
            "status": "success",
            "summary": summary
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/history', methods=['GET'])
def get_history():
    """Get download history for current session (only files from this session)"""
    from flask import jsonify
    history = get_session_history()
    
    # Sort by added_at (newest first)
    history.sort(key=lambda x: x.get('added_at', ''), reverse=True)
    
    return jsonify({
        "status": "success",
        "history": history
    })

@app.route('/api/history/<file_id>', methods=['DELETE'])
def delete_history_item(file_id):
    """Delete file from history"""
    from flask import jsonify
    if remove_from_history(file_id):
        return jsonify({"status": "success"})
    return jsonify({"status": "error", "message": "File not found"}), 404

@app.route('/api/history', methods=['DELETE'])
def clear_all_history():
    """Clear all history for current session"""
    from flask import jsonify
    if clear_history():
        return jsonify({"status": "success"})
    return jsonify({"status": "error"}), 500

@app.route('/api/telegram/upload', methods=['POST'])
def upload_to_telegram():
    """Get Telegram bot link with file_id parameter (doesn't upload to channel)"""
    from flask import request, jsonify
    data = request.json
    if not data or 'file_id' not in data:
        return jsonify({"error": "file_id is required"}), 400
    
    file_id = data['file_id']
    session_id = get_or_create_session()
    
    # Find file in history
    file_info = None
    if session_id in sessions_data:
        for item in sessions_data[session_id]['history']:
            if item['id'] == file_id:
                file_info = item
                break
    
    if not file_info:
        return jsonify({"status": "error", "message": "Файл не найден в истории"}), 404
    
    # Check if file already has telegram_file_id
    if file_info.get('telegram_file_id') and file_info.get('bot_link'):
        # Already uploaded to Telegram
        return jsonify({
            "status": "success",
            "bot_link": file_info['bot_link'],
            "cache_id": file_info.get('cache_id'),
            "telegram_file_id": file_info['telegram_file_id'],
            "message": "Файл уже загружен в Telegram"
        })
    
    try:
        # Get bot username from Telegram API
        response = requests.get(f'https://api.telegram.org/bot{BOT_TOKEN}/getMe')
        if response.status_code == 200:
            bot_info = response.json()
            if bot_info.get('ok'):
                bot_username = bot_info['result']['username']
                
                # Upload file to Telegram to get file_id
                file_path = file_info['path']
                if not os.path.exists(file_path):
                    return jsonify({"status": "error", "message": "Файл не найден на сервере"}), 404
                
                # Determine file type and upload to get file_id
                file_ext = os.path.splitext(file_path)[1].lower()
                telegram_file_id = None
                
                # Upload to service group to get file_id
                # Group ID: -4990421216
                SERVICE_GROUP_ID = -4990421216
                
                try:
                    # Upload file to service group to get file_id
                    with open(file_path, 'rb') as f:
                        files = {}
                        data_form = {'chat_id': SERVICE_GROUP_ID}  # Send to service group
                        
                        if file_ext in ['.mp4', '.mov', '.avi', '.webm']:
                            files['video'] = f
                            response = requests.post(
                                f'https://api.telegram.org/bot{BOT_TOKEN}/sendVideo',
                                files=files,
                                data=data_form
                            )
                            response.raise_for_status()
                            result = response.json()
                            if result.get('ok'):
                                telegram_file_id = result['result'].get('video', {}).get('file_id')
                                logger.info(f"Video uploaded successfully, file_id: {telegram_file_id}")
                            else:
                                logger.error(f"Telegram API error: {result.get('description', 'Unknown error')}")
                                return jsonify({"status": "error", "message": f"Telegram API: {result.get('description', 'Unknown error')}"}), 500
                        elif file_ext in ['.mp3', '.wav', '.ogg', '.m4a', '.aac']:
                            files['audio'] = f
                            response = requests.post(
                                f'https://api.telegram.org/bot{BOT_TOKEN}/sendAudio',
                                files=files,
                                data=data_form
                            )
                            response.raise_for_status()
                            result = response.json()
                            if result.get('ok'):
                                telegram_file_id = result['result'].get('audio', {}).get('file_id')
                                logger.info(f"Audio uploaded successfully, file_id: {telegram_file_id}")
                            else:
                                logger.error(f"Telegram API error: {result.get('description', 'Unknown error')}")
                                return jsonify({"status": "error", "message": f"Telegram API: {result.get('description', 'Unknown error')}"}), 500
                        elif file_ext in ['.jpg', '.jpeg', '.png', '.webp']:
                            files['photo'] = f
                            response = requests.post(
                                f'https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto',
                                files=files,
                                data=data_form
                            )
                            response.raise_for_status()
                            result = response.json()
                            if result.get('ok'):
                                # Photo returns array, take last (highest quality)
                                telegram_file_id = result['result'].get('photo', [{}])[-1].get('file_id')
                                logger.info(f"Photo uploaded successfully, file_id: {telegram_file_id}")
                            else:
                                logger.error(f"Telegram API error: {result.get('description', 'Unknown error')}")
                                return jsonify({"status": "error", "message": f"Telegram API: {result.get('description', 'Unknown error')}"}), 500
                        else:
                            files['document'] = f
                            response = requests.post(
                                f'https://api.telegram.org/bot{BOT_TOKEN}/sendDocument',
                                files=files,
                                data=data_form
                            )
                            response.raise_for_status()
                            result = response.json()
                            if result.get('ok'):
                                telegram_file_id = result['result'].get('document', {}).get('file_id')
                                logger.info(f"Document uploaded successfully, file_id: {telegram_file_id}")
                            else:
                                logger.error(f"Telegram API error: {result.get('description', 'Unknown error')}")
                                return jsonify({"status": "error", "message": f"Telegram API: {result.get('description', 'Unknown error')}"}), 500
                    
                    if not telegram_file_id:
                        return jsonify({"status": "error", "message": "Не удалось получить file_id из Telegram"}), 500
                    
                    # Determine media type
                    if file_ext in ['.mp4', '.mov', '.avi', '.webm']:
                        media_type = 'video'
                    elif file_ext in ['.mp3', '.wav', '.ogg', '.m4a', '.aac']:
                        media_type = 'audio'
                    elif file_ext in ['.jpg', '.jpeg', '.png', '.webp']:
                        media_type = 'photo'
                    else:
                        media_type = 'document'
                    
                    # Save to database with file_id
                    dummy_url = f"api_file_{file_id}"
                    cache_id = db.save_file_to_cache(dummy_url, [telegram_file_id], media_type, 0)
                    
                    if cache_id:
                        # Update history with telegram_file_id
                        if session_id in sessions_data:
                            for item in sessions_data[session_id]['history']:
                                if item['id'] == file_id:
                                    item['telegram_file_id'] = telegram_file_id
                                    break
                        
                        # Return bot link with cache_id
                        bot_link = f"https://t.me/{bot_username}?start=file_{cache_id}"
                        
                        return jsonify({
                            "status": "success",
                            "bot_link": bot_link,
                            "cache_id": cache_id,
                            "telegram_file_id": telegram_file_id,
                            "message": "Используйте эту ссылку для открытия файла в боте"
                        })
                    else:
                        return jsonify({"status": "error", "message": "Не удалось сохранить файл в базу данных"}), 500
                        
                except Exception as upload_error:
                    logger.error(f"File upload to Telegram failed: {upload_error}")
                    return jsonify({"status": "error", "message": f"Ошибка загрузки: {str(upload_error)}"}), 500
            else:
                return jsonify({"status": "error", "message": "Не удалось получить информацию о боте"}), 500
        else:
            return jsonify({"status": "error", "message": f"HTTP {response.status_code}"}), 500
            
    except Exception as e:
        logger.error(f"Telegram link generation failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/process', methods=['POST'])
def process_full_cycle():
    """
    Full cycle: Download -> Transcribe -> Summary
    Request body: { "url": "https://..." }
    """
    from flask import request, jsonify
    data = request.json
    if not data or 'url' not in data:
        return jsonify({"error": "URL is required"}), 400
    
    url = data['url']
    
    try:
        # Step 1: Download
        logger.info(f"Step 1: Downloading {url}")
        files, task_dir = get_downloader().download(url)
        
        if not files:
            return jsonify({"status": "error", "message": "Не удалось скачать файл"}), 500
        
        # Pick first file (usually video/audio)
        target_file = files[0]
        file_ext = os.path.splitext(target_file)[1].lower()
        
        # Check if it's audio/video
        is_media = file_ext in ['.mp4', '.mov', '.avi', '.webm', '.mp3', '.wav', '.ogg', '.m4a', '.aac']
        
        if not is_media:
            return jsonify({
                "status": "error", 
                "message": "Файл не является аудио или видео"
            }), 400
        
        # Step 2: Transcribe
        logger.info(f"Step 2: Transcribing {target_file}")
        transcribed_text = transcribe_audio_segments(target_file)
        
        if not transcribed_text or not transcribed_text.strip():
            return jsonify({
                "status": "error",
                "message": "Не удалось расшифровать аудио"
            }), 500
        
        # Step 3: Generate Summary
        logger.info(f"Step 3: Generating summary")
        summary = generate_summary_sync(transcribed_text)
        
        if summary.startswith("❌"):
            return jsonify({
                "status": "error",
                "message": summary
            }), 500
        
        # Prepare file info for response
        base_url = request.host_url.rstrip('/')
        rel_path = os.path.relpath(target_file, os.getcwd())
        file_url = f"{base_url}/files/{rel_path.replace(os.sep, '/')}"
        
        file_info = {
            "path": target_file,
            "filename": os.path.basename(target_file),
            "url": file_url,
            "size": os.path.getsize(target_file)
        }
        
        # Add to history
        file_entry = add_to_history(file_info)
        file_info['id'] = file_entry['id']
        
        return jsonify({
            "status": "success",
            "file": file_info,
            "transcription": transcribed_text,
            "summary": summary
        })
        
    except Exception as e:
        logger.error(f"Full cycle processing failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    # Run on port 5030 - только если API включен
    if ENABLE_API:
        app = get_app()
        app.run(host='0.0.0.0', port=5030)
    else:
        print("API is disabled (ENABLE_API=False)")

