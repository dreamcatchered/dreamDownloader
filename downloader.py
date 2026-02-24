import os
import shutil
import uuid
import logging
import time
import subprocess
import sys
import json
import gc
import threading
from config import PROXY_URL, USE_PROXY

# Глобальный мьютекс для yt-dlp чтобы избежать _ProgressState ошибок
_ytdlp_lock = threading.Lock()

# Ленивая загрузка тяжёлых модулей - не импортируем на уровне модуля:
# - yt_dlp (тяжёлый, только при скачивании)
# - pytubefix (тяжёлый, только при скачивании YouTube)

def unload_heavy_modules():
    """Выгружает тяжёлые модули из памяти после использования.
    БЕЗОПАСНАЯ версия: только удаляет из sys.modules, без очистки __dict__,
    чтобы не ломать параллельные потоки, которые уже держат ссылки на модуль."""
    modules_to_unload = ['yt_dlp', 'pytubefix', 'yt_dlp.extractor', 'yt_dlp.downloader']
    for module_name in modules_to_unload:
        keys_to_remove = [key for key in list(sys.modules.keys()) if key == module_name or key.startswith(module_name + '.')]
        for key in keys_to_remove:
            try:
                del sys.modules[key]
            except KeyError:
                pass
    gc.collect()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Downloader:
    def __init__(self):
        self.base_dir = "downloads"
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)

    def _progress_hook(self, d):
        """Hook to log download progress"""
        if d['status'] == 'downloading':
            try:
                percent = d.get('_percent_str', 'N/A')
                speed = d.get('_speed_str', 'N/A')
                eta = d.get('_eta_str', 'N/A')
                # Логируем каждые 10% или если прошло много времени
                # Но чтобы не спамить, будем просто выводить в stdout с flush
                print(f"[download] {percent} of {d.get('_total_bytes_str', 'N/A')} at {speed} ETA {eta}")
                sys.stdout.flush()
            except Exception:
                pass
        elif d['status'] == 'finished':
            print(f"[download] Download completed: {d.get('filename', 'unknown')}")
            sys.stdout.flush()

    def detect_content_type(self, url):
        """Определяет тип контента (photo/video/audio) по структуре URL"""
        
        # SoundCloud
        if 'soundcloud.com' in url:
            return 'audio'

        # YouTube
        if 'youtube.com' in url or 'youtu.be' in url:
            return 'video'

        # Instagram patterns
        if 'instagram.com' in url:
            if '/reel/' in url:
                return 'video'  # Reels - всегда видео
            elif '/tv/' in url:
                return 'video'  # IGTV - всегда видео
            elif '/p/' in url:
                # Посты - обычно фото-карусели, используем gallery-dl сразу
                return 'photo'
        
        # TikTok patterns
        if 'tiktok.com' in url or 'vt.tiktok.com' in url:
            if '/photo/' in url:
                return 'photo'  # TikTok фото-слайдшоу
            elif '/video/' in url or '/@' in url:
                return 'video'  # TikTok видео
        
        # Default: считаем видео (yt-dlp лучше работает с видео)
        return 'video'

    def download(self, url):
        task_id = str(uuid.uuid4())
        task_dir = os.path.join(self.base_dir, task_id)
        os.makedirs(task_dir)

        # Инициализируем переменную для частично скачанных файлов
        partial_files = None

        # Определяем тип контента по URL
        content_type = self.detect_content_type(url)
        logger.info(f"Detected content type: {content_type} for {url}")
        
        # Стратегия на основе типа контента:
        # - photo (TikTok слайдшоу) -> gallery-dl
        # - video (Reels, IGTV, TikTok видео) -> yt-dlp
        # - unknown_post (Instagram /p/) -> сначала yt-dlp, потом gallery-dl если не получится
        
        if content_type == 'photo':
            # Для фото используем gallery-dl
            try:
                self._download_gallery_dl(url, task_dir)
            except Exception as e:
                logger.warning(f"gallery-dl failed for photo: {e}, trying yt-dlp fallback...")
                try:
                    self._download_ytdlp(url, task_dir)
                except Exception as e2:
                    logger.error(f"yt-dlp fallback also failed: {e2}")
                    shutil.rmtree(task_dir, ignore_errors=True)
                    raise e2
        elif content_type == 'video' and ('youtube.com' in url or 'youtu.be' in url):
            # Для YouTube пробуем pytubefix, при ошибке BotDetection пробуем другие методы
            try:
                self._download_youtube_pytubefix(url, task_dir)
            except Exception as e:
                error_str = str(e).lower()
                # Если это BotDetection или похожая ошибка, пробуем использовать cookies с yt-dlp
                if 'bot' in error_str or 'detect' in error_str:
                    logger.warning(f"pytubefix detected as bot: {e}, trying yt-dlp with cookies...")
                    try:
                        self._download_youtube_with_cookies(url, task_dir)
                    except Exception as e2:
                        logger.error(f"yt-dlp with cookies also failed: {e2}")
                        shutil.rmtree(task_dir, ignore_errors=True)
                        raise e2
                else:
                    # Другие ошибки - пробуем обычный yt-dlp
                    logger.warning(f"pytubefix failed: {e}, trying yt-dlp fallback...")
                    try:
                        self._download_ytdlp(url, task_dir)
                    except Exception as e2:
                        logger.error(f"yt-dlp fallback also failed: {e2}")
                        shutil.rmtree(task_dir, ignore_errors=True)
                        raise e2
        elif content_type == 'video' and '/reel/' in url:
            # Для Instagram рилсов сначала пробуем без куки, потом с куки
            try:
                logger.info(f"Trying Instagram reel without cookies first: {url}")
                self._download_instagram_reel_no_cookies(url, task_dir)
            except Exception as e:
                error_str = str(e).lower()
                logger.warning(f"Instagram reel download without cookies failed: {e}, trying with cookies...")
                # Пробуем с куки только если это ошибка связанная с авторизацией/доступом
                should_try_with_cookies = any(keyword in error_str for keyword in [
                    'login', 'private', 'unavailable', 'access denied', 
                    'authentication', 'cookie', 'session', '403', '401'
                ])
                if should_try_with_cookies:
                    try:
                        self._download_instagram_reel_with_cookies(url, task_dir)
                    except Exception as e2:
                        logger.error(f"Instagram reel download with cookies also failed: {e2}")
                        shutil.rmtree(task_dir, ignore_errors=True)
                        raise e2
                else:
                    # Если это не ошибка авторизации, пробуем обычный yt-dlp
                    try:
                        self._download_ytdlp(url, task_dir)
                    except Exception as e2:
                        logger.error(f"yt-dlp fallback also failed: {e2}")
                        shutil.rmtree(task_dir, ignore_errors=True)
                        raise e2
        else:
            # Для видео (video) используем yt-dlp (быстрее)
            try:
                self._download_ytdlp(url, task_dir)
            except Exception as e:
                error_str = str(e).lower()
                error_full = str(e)
                
                # Проверяем наличие частично скачанных файлов при таймауте
                is_timeout = 'timeout' in error_str or 'timed out' in error_str or 'read operation timed out' in error_str
                if is_timeout:
                    logger.warning(f"Download timeout detected: {e}, checking for partially downloaded files...")
                    partial_files = []
                    for root, dirs, filenames in os.walk(task_dir):
                        for f in filenames:
                            # Пропускаем временные файлы .part и .ytdl
                            if f.endswith('.part') or f.endswith('.ytdl'):
                                continue
                            file_path = os.path.join(root, f)
                            try:
                                file_size = os.path.getsize(file_path)
                                # Если файл больше 100KB, считаем его валидным
                                if file_size > 100 * 1024:
                                    partial_files.append(file_path)
                                    logger.info(f"Found partially downloaded file: {f} ({file_size / 1024 / 1024:.2f} MB)")
                            except OSError:
                                continue
                    
                    if partial_files:
                        logger.info(f"Using {len(partial_files)} partially downloaded file(s) despite timeout error")
                        # Продолжаем обработку с найденными файлами
                    else:
                        logger.warning("No valid partially downloaded files found, will try fallback or raise error")
                
                # Проверяем, является ли это фото (по редиректу в ошибке)
                is_photo_redirect = '/photo/' in error_full
                
                # Если yt-dlp не справился, пробуем gallery-dl для Instagram/TikTok
                should_try_gallery_dl = any(keyword in error_str for keyword in [
                    'no video formats', 
                    'no formats', 
                    'unable to download', 
                    'unavailable',
                    'unsupported url'
                ])
                
                # Для TikTok особенно важно: короткие ссылки могут редиректить на фото
                if 'tiktok.com' in url or 'vt.tiktok.com' in url:
                    should_try_gallery_dl = True
                
                # Если в ошибке есть /photo/ - это точно фото
                if is_photo_redirect:
                    should_try_gallery_dl = True
                    logger.info("Detected photo redirect in error, will use gallery-dl")
                
                # Если есть частично скачанные файлы при таймауте, не пробуем fallback
                if is_timeout and partial_files:
                    logger.info("Skipping fallback, using partially downloaded files")
                elif should_try_gallery_dl and ('instagram.com' in url or 'tiktok.com' in url or 'vt.tiktok.com' in url):
                    logger.warning(f"yt-dlp failed: {e}, trying gallery-dl fallback...")
                    try:
                        self._download_gallery_dl(url, task_dir)
                    except Exception as e2:
                        logger.error(f"gallery-dl fallback also failed: {e2}")
                        # Проверяем еще раз на частично скачанные файлы
                        if not partial_files:
                            partial_files = []
                            for root, dirs, filenames in os.walk(task_dir):
                                for f in filenames:
                                    if f.endswith('.part') or f.endswith('.ytdl'):
                                        continue
                                    file_path = os.path.join(root, f)
                                    try:
                                        file_size = os.path.getsize(file_path)
                                        if file_size > 100 * 1024:
                                            partial_files.append(file_path)
                                    except OSError:
                                        continue
                        
                        if not partial_files:
                            shutil.rmtree(task_dir, ignore_errors=True)
                            raise e2
                        else:
                            logger.info(f"Using {len(partial_files)} partially downloaded file(s) despite all errors")
                elif not (is_timeout and partial_files):
                    # Если это не таймаут с файлами, выбрасываем ошибку
                    raise e

        # Используем частично скачанные файлы, если они были найдены при таймауте
        if partial_files is not None:
            files = partial_files
        else:
            files = []
            for root, dirs, filenames in os.walk(task_dir):
                for f in filenames:
                    # Пропускаем временные файлы yt-dlp (.part, .ytdl)
                    if not f.endswith('.part') and not f.endswith('.ytdl'):
                        file_path = os.path.join(root, f)
                        try:
                            file_size = os.path.getsize(file_path)
                            # Минимальный размер файла - 10KB (чтобы отфильтровать пустые/битые файлы)
                            if file_size > 10 * 1024:
                                files.append(file_path)
                        except OSError:
                            continue
        
        if not files:
            shutil.rmtree(task_dir, ignore_errors=True)
            raise Exception("No files downloaded.")
        
        logger.info(f"Downloaded files in {task_dir}: {[os.path.basename(f) for f in files]}")
        
        # Для SoundCloud получаем метаданные и переименовываем файл
        # Фильтруем файлы: оставляем только аудио, обложки отдельно
        audio_files = []
        thumbnail_files = []
        
        if 'soundcloud.com' in url:
            for f in files:
                ext = os.path.splitext(f)[1].lower()
                if ext in ['.mp3', '.m4a', '.aac', '.ogg', '.wav', '.opus', '.flac']:
                    audio_files.append(f)
                elif ext in ['.jpg', '.jpeg', '.png', '.webp']:
                    thumbnail_files.append(f)
            
            # Если есть аудио файл, переименовываем его
            if audio_files:
                try:
                    metadata = self._get_soundcloud_metadata(url)
                    if metadata:
                        old_path = audio_files[0]
                        ext = os.path.splitext(old_path)[1]
                        # Создаем имя файла: автор - название.mp3
                        artist = metadata.get('uploader', 'Unknown')
                        title = metadata.get('title', 'Track')
                        # Очищаем имя от недопустимых символов для файловой системы
                        safe_artist = "".join(c for c in artist if c.isalnum() or c in (' ', '-', '_')).strip()
                        safe_title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).strip()
                        new_filename = f"{safe_artist} - {safe_title}{ext}"
                        new_path = os.path.join(task_dir, new_filename)
                        os.rename(old_path, new_path)
                        audio_files = [new_path]
                        logger.info(f"Renamed SoundCloud file to: {new_filename}")
                        
                        # Сохраняем метаданные в файл для использования при отправке
                        metadata_file = os.path.join(task_dir, 'metadata.json')
                        try:
                            import json
                            with open(metadata_file, 'w', encoding='utf-8') as f:
                                json.dump(metadata, f, ensure_ascii=False)
                        except Exception as e:
                            logger.warning(f"Failed to save metadata: {e}")
                except Exception as e:
                    logger.warning(f"Failed to rename SoundCloud file: {e}")
            
            # Возвращаем только аудио файлы (обложки будут использоваться как thumbnail)
            files = audio_files if audio_files else files
        
        # Выгружаем тяжёлые модули из памяти после скачивания
        unload_heavy_modules()
        
        return files, task_dir

    def _get_cookies_file(self, url):
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

    def _download_gallery_dl(self, url, task_dir):
        """Method using gallery-dl for photos/carousels"""
        logger.info(f"Using gallery-dl for: {url}")
        
        cookies_file = self._get_cookies_file(url)
        
        # Configure gallery-dl to output files directly to task_dir
        cmd = [
            'gallery-dl',
            '--dest', task_dir,
            '--directory', '.',
            '--filename', '{category}_{id}_{num}.{extension}',
            url
        ]
        
        if USE_PROXY and PROXY_URL:
            cmd.extend(['--proxy', PROXY_URL])
            
        if cookies_file:
            cmd.extend(['--cookies', cookies_file])
            logger.info(f"Using cookies file: {cookies_file} (hot-reloadable, no restart needed)")
            
        process = subprocess.run(cmd, capture_output=True, text=True, check=True)
        logger.info(f"gallery-dl output: {process.stdout}")

    def _get_soundcloud_metadata(self, url):
        """Получает метаданные SoundCloud (название, автор, обложка)"""
        # Ленивая загрузка yt_dlp только когда нужно
        import yt_dlp
        
        try:
            ydl_opts = {
                'quiet': True,
                'no_warnings': True,
                'skip_download': True,
                'extract_flat': False,
            }
            
            if USE_PROXY and PROXY_URL:
                ydl_opts['proxy'] = PROXY_URL
            
            # Добавляем cookies согласно документации yt-dlp
            # В Python API используется 'cookiefile' (аналог --cookies в CLI)
            cookies_file = self._get_cookies_file(url)
            if cookies_file:
                if os.path.exists(cookies_file) and os.path.getsize(cookies_file) > 0:
                    ydl_opts['cookiefile'] = cookies_file
                else:
                    logger.warning(f"Cookies file {cookies_file} is empty or doesn't exist")
            
            with _ytdlp_lock:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=False)
                    if info:
                        metadata = {
                            'title': info.get('title', 'Unknown'),
                            'uploader': info.get('uploader', info.get('artist', 'Unknown')),
                            'thumbnail': info.get('thumbnail') or info.get('artwork_url'),
                            'description': info.get('description', '')
                        }
                        return metadata
        except Exception as e:
            logger.warning(f"Failed to get SoundCloud metadata: {e}")
        return None

    def _download_instagram_reel_no_cookies(self, url, task_dir):
        """Download Instagram reel without cookies - оптимизировано для скорости"""
        # Ленивая загрузка yt_dlp только когда нужно
        import yt_dlp
        
        logger.info(f"Using yt-dlp for Instagram reel WITHOUT cookies: {url}")
        
        # Оптимизированные настройки для быстрой загрузки Instagram рилсов
        ydl_opts = {
            'outtmpl': os.path.join(task_dir, '%(id)s.%(ext)s'),
            'quiet': True,
            'no_warnings': True,
            'restrictfilenames': True,
            'progress_hooks': [self._progress_hook],
            # Используем best вместо bestvideo+bestaudio - быстрее (не нужно объединять)
            'format': 'best',
            # Мобильный user-agent для обхода ограничений
            'user_agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
            # Настройки экстрактора для Instagram без куки
            'extractor_args': {
                'instagram': {
                    'include_carousel': False,  # Для рилсов не нужна карусель
                }
            },
            # Дополнительные заголовки для обхода ограничений
            'http_headers': {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Referer': 'https://www.instagram.com/',
            },
            # Минимальные настройки для скорости
            'writethumbnail': False,
            'writeinfojson': False,
        }
        
        # Add proxy if enabled
        if USE_PROXY and PROXY_URL:
            ydl_opts['proxy'] = PROXY_URL
        
        # НЕ добавляем куки - это метод БЕЗ куки
        logger.info("Downloading Instagram reel WITHOUT cookies")
        
        with _ytdlp_lock:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])
    
    def _download_instagram_reel_with_cookies(self, url, task_dir):
        """Download Instagram reel with cookies (fallback если без куки не получилось) - оптимизировано для скорости"""
        # Ленивая загрузка yt_dlp только когда нужно
        import yt_dlp
        
        logger.info(f"Using yt-dlp for Instagram reel WITH cookies: {url}")
        
        ydl_opts = {
            'outtmpl': os.path.join(task_dir, '%(id)s.%(ext)s'),
            'quiet': True,
            'no_warnings': True,
            'restrictfilenames': True,
            'progress_hooks': [self._progress_hook],
            # Используем best вместо bestvideo+bestaudio - быстрее (не нужно объединять)
            'format': 'best',
            'user_agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
            'extractor_args': {
                'instagram': {
                    'include_carousel': False,
                }
            },
            # Дополнительные заголовки
            'http_headers': {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Referer': 'https://www.instagram.com/',
            },
            # Минимальные настройки для скорости
            'writethumbnail': False,
            'writeinfojson': False,
        }
        
        # Add proxy if enabled
        if USE_PROXY and PROXY_URL:
            ydl_opts['proxy'] = PROXY_URL
        
        # Добавляем куки согласно документации yt-dlp:
        # В Python API используется параметр 'cookiefile' (аналог --cookies в CLI)
        # Файл должен быть в формате Netscape HTTP Cookie File
        # См. https://github.com/yt-dlp/yt-dlp/wiki/FAQ#how-do-i-pass-cookies-to-yt-dlp
        cookies_file = self._get_cookies_file(url)
        if cookies_file:
            # Проверяем, что файл существует и не пустой
            if os.path.exists(cookies_file) and os.path.getsize(cookies_file) > 0:
                ydl_opts['cookiefile'] = cookies_file
                logger.info(f"Using cookies file: {cookies_file} for Instagram reel (Netscape format)")
            else:
                logger.warning(f"Cookies file {cookies_file} is empty or doesn't exist")
        else:
            logger.warning("No cookies file found, but trying with cookies method anyway")
        
        with _ytdlp_lock:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])

    def _download_ytdlp(self, url, task_dir):
        """Primary download method - fastest, uses yt-dlp"""
        # Ленивая загрузка yt_dlp только когда нужно
        import yt_dlp
        
        logger.info(f"Using yt-dlp (fast) for: {url}")
        
        # Basic options
        ydl_opts = {
            'outtmpl': os.path.join(task_dir, '%(id)s.%(ext)s'),
            'quiet': True,
            'no_warnings': True,
            'restrictfilenames': True,
            'progress_hooks': [self._progress_hook],
            'continue_dl': True,  # Продолжать загрузку частично скачанных файлов
            'nopart': False,  # Не удалять частично скачанные файлы (.part)
            # Network timeouts to prevent hanging
            'socket_timeout': 120,  # 120 seconds socket timeout
            'retries': 10,  # Increase retry count
            'fragment_retries': 10,  # Retry fragments
            'file_access_retries': 5,  # Retry file access
            'extractor_retries': 5,  # Retry extractor
        }
        
        # Add proxy if enabled
        if USE_PROXY and PROXY_URL:
            ydl_opts['proxy'] = PROXY_URL
        
        # Platform-specific format selection
        if 'soundcloud.com' in url:
            # SoundCloud: download best audio and convert to mp3
            ydl_opts['format'] = 'bestaudio/best'
            ydl_opts['postprocessors'] = [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }]
            # Сохраняем метаданные
            ydl_opts['writethumbnail'] = True
            ydl_opts['embedthumbnail'] = False  # Не встраиваем, используем отдельно
        elif 'youtube.com' in url or 'youtu.be' in url:
            # YouTube: video with audio, max 1080p to avoid huge files for TG
            # Более гибкий формат для работы с Shorts и обычными видео
            # Пробуем сначала лучший формат с ограничением по высоте, потом без ограничений
            ydl_opts['format'] = 'bestvideo[height<=1080]+bestaudio/best[height<=1080]/bestvideo+bestaudio/best'
            # Добавляем настройки для обхода детекции бота
            ydl_opts['extractor_args'] = {
                'youtube': {
                    'player_client': ['android', 'ios', 'web'],
                    'player_skip': ['webpage', 'configs'],
                }
            }
            # Используем мобильный user-agent для обхода детекции
            ydl_opts['user_agent'] = 'Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36'
        elif 'instagram.com' in url:
            # Instagram: best format (works for both video and images) - оптимизировано для скорости
            ydl_opts['format'] = 'best'
            ydl_opts['extractor_args'] = {
                'instagram': {
                    'include_carousel': True,
                }
            }
            # Используем мобильный user-agent для обхода ограничений
            ydl_opts['user_agent'] = 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1'
            # Дополнительные заголовки для Instagram
            ydl_opts['http_headers'] = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Referer': 'https://www.instagram.com/',
            }
        elif 'tiktok.com' in url or 'vt.tiktok.com' in url:
            # TikTok: prefer video
            ydl_opts['format'] = 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best'
        else:
            # General: best available
            ydl_opts['format'] = 'best'

        # Add cookies if available (читается каждый раз заново - можно обновлять без перезапуска)
        # Согласно документации yt-dlp: https://github.com/yt-dlp/yt-dlp/wiki/FAQ#how-do-i-pass-cookies-to-yt-dlp
        # В Python API используется 'cookiefile' (аналог --cookies в CLI)
        # Файл должен быть в формате Netscape HTTP Cookie File
        cookies_file = self._get_cookies_file(url)
        if cookies_file:
            if os.path.exists(cookies_file) and os.path.getsize(cookies_file) > 0:
                ydl_opts['cookiefile'] = cookies_file
                logger.info(f"Using cookies file: {cookies_file} (hot-reloadable, no restart needed, Netscape format)")
            else:
                logger.warning(f"Cookies file {cookies_file} is empty or doesn't exist")
            
        with _ytdlp_lock:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])

    def _download_youtube_with_cookies(self, url, task_dir):
        """Download YouTube video using yt-dlp with enhanced bot detection bypass"""
        # Ленивая загрузка yt_dlp только когда нужно
        import yt_dlp
        
        logger.info(f"Using yt-dlp with enhanced settings for YouTube: {url}")
        
        ydl_opts = {
            'outtmpl': os.path.join(task_dir, '%(id)s.%(ext)s'),
            'quiet': True,
            'no_warnings': True,
            'restrictfilenames': True,
            'progress_hooks': [self._progress_hook],
            'format': 'bestvideo[height<=1080]+bestaudio/best[height<=1080]/bestvideo+bestaudio/best',
            'extractor_args': {
                'youtube': {
                    'player_client': ['android', 'ios', 'web'],
                    'player_skip': ['webpage', 'configs'],
                    'skip': ['dash', 'hls'],
                }
            },
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }
        
        # Add proxy if enabled
        if USE_PROXY and PROXY_URL:
            ydl_opts['proxy'] = PROXY_URL
        
        # Добавляем cookies для YouTube
        # Согласно документации yt-dlp: https://github.com/yt-dlp/yt-dlp/wiki/FAQ#how-do-i-pass-cookies-to-yt-dlp
        # В Python API используется 'cookiefile' (аналог --cookies в CLI)
        cookies_file = self._get_cookies_file(url)
        if cookies_file:
            if os.path.exists(cookies_file) and os.path.getsize(cookies_file) > 0:
                ydl_opts['cookiefile'] = cookies_file
                logger.info(f"Using cookies file: {cookies_file} for YouTube (Netscape format)")
            else:
                logger.warning(f"Cookies file {cookies_file} is empty or doesn't exist")
        
        with _ytdlp_lock:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])

    def _download_youtube_pytubefix(self, url, task_dir):
        """Download YouTube video using pytubefix (more reliable in 2025)"""
        # Ленивая загрузка pytubefix только когда нужно
        try:
            from pytubefix import YouTube
            from pytubefix.exceptions import VideoUnavailable, RegexMatchError, BotDetection
        except ImportError:
            raise Exception("pytubefix is not installed")
        
        logger.info(f"Using pytubefix for YouTube: {url}")
        
        try:
            # Configure proxy if available
            proxies = None
            if USE_PROXY and PROXY_URL:
                logger.info(f"Using proxy for pytubefix: {PROXY_URL}")
                # pytubefix поддерживает прокси через параметр proxies
                proxies = {
                    'http': PROXY_URL,
                    'https': PROXY_URL
                }
            
            # Create YouTube object with proxy if available
            # Для обхода детекции бота пробуем разные варианты
            yt = None
            try:
                # Сначала пробуем с прокси без OAuth
                if proxies:
                    yt = YouTube(url, proxies=proxies)
                else:
                    yt = YouTube(url)
            except (BotDetection, Exception) as init_error:
                # Если детектируется как бот, пробуем с use_oauth
                logger.warning(f"First attempt failed: {init_error}, trying with use_oauth...")
                try:
                    if proxies:
                        yt = YouTube(url, proxies=proxies, use_oauth=True, allow_oauth_cache=True)
                    else:
                        yt = YouTube(url, use_oauth=True, allow_oauth_cache=True)
                except Exception as oauth_error:
                    logger.error(f"OAuth attempt also failed: {oauth_error}")
                    raise
            
            # Get the best quality video stream (max 1080p for Telegram)
            stream = None
            
            # Try progressive streams first (video + audio together) - max 1080p
            progressive_streams = []
            try:
                progressive_streams = list(yt.streams.filter(progressive=True, file_extension='mp4'))
            except BotDetection as bot_error:
                logger.warning(f"BotDetection when getting streams: {bot_error}, trying with use_oauth...")
                # Пробуем пересоздать объект с use_oauth для обхода детекции
                try:
                    if proxies:
                        yt = YouTube(url, proxies=proxies, use_oauth=True, allow_oauth_cache=True)
                    else:
                        yt = YouTube(url, use_oauth=True, allow_oauth_cache=True)
                    progressive_streams = list(yt.streams.filter(progressive=True, file_extension='mp4'))
                except Exception as oauth_error:
                    logger.error(f"OAuth retry also failed: {oauth_error}")
                    raise Exception(f"Bot detected: {str(bot_error)}")
            if progressive_streams:
                # Filter streams with resolution <= 1080p
                filtered = [s for s in progressive_streams if s.resolution and int(s.resolution.replace('p', '')) <= 1080]
                if filtered:
                    stream = max(filtered, key=lambda s: int(s.resolution.replace('p', '')))
                else:
                    # If no 1080p, get highest available progressive
                    stream = max(progressive_streams, key=lambda s: int(s.resolution.replace('p', '')) if s.resolution else 0)
            
            if stream:
                # Progressive stream found, download it
                output_path = stream.download(output_path=task_dir, filename=f"{yt.video_id}")
                logger.info(f"YouTube video downloaded: {output_path}")
                return
            
            # If no progressive, get best video and audio separately
            video_streams = []
            audio_streams = []
            try:
                video_streams = list(yt.streams.filter(adaptive=True, only_video=True, file_extension='mp4'))
                audio_streams = list(yt.streams.filter(adaptive=True, only_audio=True))
            except BotDetection as bot_error:
                logger.warning(f"BotDetection when getting adaptive streams: {bot_error}")
                raise Exception(f"Bot detected: {str(bot_error)}")
            
            if video_streams and audio_streams:
                # Filter video streams to max 1080p
                video_filtered = [s for s in video_streams if s.resolution and int(s.resolution.replace('p', '')) <= 1080]
                if video_filtered:
                    video_stream = max(video_filtered, key=lambda s: int(s.resolution.replace('p', '')))
                else:
                    video_stream = max(video_streams, key=lambda s: int(s.resolution.replace('p', '')) if s.resolution else 0)
                
                audio_stream = max(audio_streams, key=lambda s: s.abr if s.abr else 0)
                
                if video_stream and audio_stream:
                    # Download both
                    video_path = video_stream.download(output_path=task_dir, filename='video')
                    audio_path = audio_stream.download(output_path=task_dir, filename='audio')
                    
                    # Merge using ffmpeg
                    output_path = os.path.join(task_dir, f"{yt.video_id}.mp4")
                    merge_cmd = [
                        'ffmpeg', '-i', video_path, '-i', audio_path,
                        '-c:v', 'copy', '-c:a', 'aac', '-strict', 'experimental',
                        '-y', output_path
                    ]
                    result = subprocess.run(merge_cmd, check=True, capture_output=True, text=True)
                    
                    # Clean up temp files
                    if os.path.exists(video_path):
                        os.remove(video_path)
                    if os.path.exists(audio_path):
                        os.remove(audio_path)
                    
                    logger.info(f"YouTube video downloaded and merged: {output_path}")
                    return
            
            # Fallback: get any available stream
            all_streams = []
            try:
                all_streams = list(yt.streams.filter(file_extension='mp4'))
            except BotDetection as bot_error:
                logger.warning(f"BotDetection when getting all streams: {bot_error}")
                raise Exception(f"Bot detected: {str(bot_error)}")
            if all_streams:
                # Try to get highest resolution
                stream = max(all_streams, key=lambda s: int(s.resolution.replace('p', '')) if s.resolution and 'p' in s.resolution else 0)
                if stream:
                    output_path = stream.download(output_path=task_dir, filename=f"{yt.video_id}")
                    logger.info(f"YouTube video downloaded (fallback): {output_path}")
                    return
            
            raise Exception("No suitable streams found")
                    
        except BotDetection as e:
            logger.warning(f"BotDetection in pytubefix (video is available, but detected as bot): {e}")
            # Видео доступно, но детектируется как бот - это значит pytubefix не может обойти защиту
            # Пробрасываем исключение, чтобы fallback на yt-dlp сработал
            raise Exception(f"Bot detected by pytubefix (video exists): {str(e)}")
        except VideoUnavailable:
            raise Exception("Video is unavailable")
        except RegexMatchError:
            raise Exception("Could not extract video information")
        except Exception as e:
            logger.error(f"pytubefix download error: {e}")
            raise

    def cleanup(self, task_dir):
        """Удаляет папку task_dir и все её содержимое. Гарантирует удаление файлов."""
        if not task_dir:
            return
            
        if os.path.exists(task_dir):
            try:
                # Удаляем всю папку рекурсивно
                shutil.rmtree(task_dir, ignore_errors=False)
                logger.info(f"Successfully cleaned up directory: {task_dir}")
            except Exception as e:
                logger.error(f"Error cleaning up {task_dir}: {e}")
                # Пробуем еще раз с ignore_errors=True для надежности
                try:
                    shutil.rmtree(task_dir, ignore_errors=True)
                    logger.warning(f"Force cleaned up directory: {task_dir}")
                except Exception as e2:
                    logger.error(f"Failed to force cleanup {task_dir}: {e2}")
        else:
            logger.debug(f"Directory does not exist (already cleaned?): {task_dir}")

    def _run_ffmpeg_with_nice(self, cmd):
        """Запускает ffmpeg с пониженным приоритетом CPU (nice) на Linux для защиты VPS"""
        if os.name != 'nt':  # Не Windows
            cmd = ['nice', '-n', '10'] + cmd
        return subprocess.run(cmd, check=True, capture_output=True)
    
    def convert_to_mp3(self, input_path, output_dir):
        """Convert video to MP3 using ffmpeg with CPU limits to prevent VPS overload"""
        output_path = os.path.join(output_dir, "audio.mp3")
        cmd = [
            'ffmpeg', '-y',
            '-i', input_path,
            '-q:a', '0',
            '-map', 'a',
            output_path
        ]
        try:
            self._run_ffmpeg_with_nice(cmd)
            return output_path
        except subprocess.CalledProcessError as e:
            logger.error(f"FFmpeg mp3 conversion failed: {e.stderr.decode()}")
            raise

    def convert_to_voice(self, input_path, output_dir):
        """Convert video to OGG Opus voice message with CPU limits"""
        output_path = os.path.join(output_dir, "voice.ogg")
        cmd = [
            'ffmpeg', '-y',
            '-i', input_path,
            '-vn', # Disable video
            '-c:a', 'libopus',
            '-b:a', '32k',
            '-vbr', 'on',
            '-application', 'voip',
            output_path
        ]
        try:
            self._run_ffmpeg_with_nice(cmd)
            return output_path
        except subprocess.CalledProcessError as e:
            logger.error(f"FFmpeg voice conversion failed: {e.stderr.decode()}")
            raise

    def needs_telegram_optimization(self, file_path):
        """
        Проверяет, нужно ли оптимизировать видео для Telegram.
        Возвращает True если:
        - Размер файла > 48 MB (нужно сжатие)
        - Или нужно проверить/исправить формат (H.264 + AAC)
        - Или вертикальное видео без правильных метаданных
        """
        try:
            import subprocess
            import json
            
            file_size = os.path.getsize(file_path)
            size_mb = file_size / (1024 * 1024)
            
            # Если файл больше 48 MB - нужна оптимизация (сжатие)
            if size_mb > 48:
                return True, f"File size {size_mb:.2f} MB > 48 MB"
            
            # Проверяем кодек видео - ВСЕГДА оптимизируем для гарантии правильного формата
            try:
                cmd = [
                    'ffprobe', '-v', 'error', '-select_streams', 'v:0',
                    '-show_entries', 'stream=codec_name,width,height,display_aspect_ratio,sample_aspect_ratio',
                    '-of', 'json',
                    file_path
                ]
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=5, check=True)
                data = json.loads(result.stdout)
                
                if 'streams' in data and len(data['streams']) > 0:
                    stream = data['streams'][0]
                    codec_name = stream.get('codec_name', '').lower()
                    width = stream.get('width', 0)
                    height = stream.get('height', 0)
                    display_aspect_ratio = stream.get('display_aspect_ratio', '')
                    
                    # Если кодек не H.264 - обязательно оптимизируем
                    if codec_name not in ['h264', 'libx264']:
                        return True, f"Video codec is {codec_name}, needs H.264 conversion"
                    
                    # Вертикальное видео без правильных метаданных
                    if height > width and (not display_aspect_ratio or display_aspect_ratio == 'N/A'):
                        return True, f"Vertical video {width}x{height} without proper aspect ratio metadata"
                    
            except Exception as e:
                logger.debug(f"Could not check video codec: {e}")
                # Если не можем проверить - лучше оптимизировать для гарантии
                return True, f"Could not verify codec, optimizing for safety: {e}"
            
            # Для маленьких видео с правильным кодеком - не оптимизируем (быстрее)
            return False, None
        except Exception as e:
            logger.warning(f"Error checking if video needs optimization: {e}")
            # В случае ошибки - лучше перестраховаться и оптимизировать
            return True, f"Error checking: {e}"
    
    def get_video_info(self, file_path):
        """Получает ширину, высоту и длительность видео"""
        try:
            cmd = [
                'ffprobe', '-v', 'error', '-select_streams', 'v:0',
                '-show_entries', 'stream=width,height,duration',
                '-of', 'json',
                file_path
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            info = json.loads(result.stdout)
            stream = info['streams'][0]
            return {
                'width': int(stream.get('width', 0)),
                'height': int(stream.get('height', 0)),
                'duration': int(float(stream.get('duration', 0)))
            }
        except Exception as e:
            logger.error(f"Error getting video info: {e}")
            return None

    def optimize_for_telegram(self, input_path, output_dir, fast_mode=True):
        """
        Оптимизирует видео для Telegram: H.264, AAC, YUV420P, четные размеры, SAR 1:1.
        """
        try:
            # Имя выходного файла
            base_name = os.path.splitext(os.path.basename(input_path))[0]
            output_path = os.path.join(output_dir, f"{base_name}_optimized.mp4")
            
            # ВАЖНЫЙ ФИЛЬТР:
            # 1. scale=ceil(iw/2)*2:ceil(ih/2)*2 -> округляет размеры до четных (требование многих плееров)
            # 2. setsar=1 -> делает пиксели квадратными (исправляет сплющивание/растягивание)
            vf_filter = "scale=ceil(iw/2)*2:ceil(ih/2)*2,setsar=1"

            cmd = [
                'ffmpeg', '-y', 
                '-i', input_path,
                '-c:v', 'libx264',
                '-preset', 'superfast', # Чуть медленнее ultrafast, но лучше совместимость
                '-crf', '26',           # Оптимальное качество для мессенджера
                '-profile:v', 'main',   # High профиль иногда глючит на старых андроидах
                '-pix_fmt', 'yuv420p',  # Обязательно для Telegram
                '-vf', vf_filter,       # Исправляет геометрию
                '-c:a', 'aac',
                '-b:a', '128k',
                '-ac', '2',             # Стерео звук
                '-movflags', '+faststart', # Позволяет воспроизводить видео до полной загрузки
                '-metadata:s:v:0', 'rotate=0', # Сбрасываем флаг поворота, т.к. мы уже применили фильтры
                output_path
            ]
            
            logger.info(f"[OPTIMIZE] Command: {' '.join(cmd)}")
            
            if os.name != 'nt':
                cmd = ['nice', '-n', '15'] + cmd

            process = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=600,
                check=True
            )
            
            if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                return output_path
            else:
                logger.error(f"[OPTIMIZE] ❌ Output file not created")
                return None
                
        except Exception as e:
            logger.error(f"[OPTIMIZE] ❌ Error optimizing video: {e}", exc_info=True)
            return None

    def convert_to_video_note(self, input_path, output_dir):
        """Convert video or audio to square MP4 (Video Note) < 1 min with CPU limits"""
        output_path = os.path.join(output_dir, "videonote.mp4")
        
        # Check if input is audio file (MP3, etc)
        input_ext = os.path.splitext(input_path)[1].lower()
        is_audio = input_ext in ['.mp3', '.wav', '.ogg', '.m4a', '.aac', '.flac', '.opus']
        
        if is_audio:
            # For audio files, create a video with static image (640x640 black or colored background)
            # Generate a simple colored background image on the fly
            bg_image = os.path.join(output_dir, "bg.png")
            
            # Create a 640x640 image with ffmpeg (с ограничением CPU)
            self._run_ffmpeg_with_nice([
                'ffmpeg', '-y',
                '-f', 'lavfi',
                '-i', 'color=c=0x1a1a1a:s=640x640:d=1',
                '-frames:v', '1',
                bg_image
            ])
            
            # Get audio duration
            probe_cmd = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', input_path]
            probe_result = subprocess.run(probe_cmd, capture_output=True, text=True, check=True)
            duration = float(probe_result.stdout.strip())
            
            # Limit to 60 seconds for video notes
            duration = min(duration, 60.0)
            
            # Create video from audio and image
            cmd = [
                'ffmpeg', '-y',
                '-loop', '1',
                '-i', bg_image,
                '-i', input_path,
                '-vf', 'scale=640:640:force_original_aspect_ratio=decrease,pad=640:640:(ow-iw)/2:(oh-ih)/2',
                '-c:v', 'libx264',
                '-tune', 'stillimage',
                '-c:a', 'aac',
                '-b:a', '64k',
                '-pix_fmt', 'yuv420p',
                '-shortest',
                '-t', str(duration),
                output_path
            ]
        else:
            # For video files, crop to square and scale
            cmd = [
                'ffmpeg', '-y',
                '-i', input_path,
                '-vf', 'crop=min(iw\\,ih):min(iw\\,ih),scale=640:640',
                '-c:v', 'libx264',
                '-crf', '26',
                '-c:a', 'aac',
                '-b:a', '64k',
                '-t', '60',
                output_path
            ]
        
        try:
            self._run_ffmpeg_with_nice(cmd)
            return output_path
        except subprocess.CalledProcessError as e:
            error_msg = e.stderr.decode() if e.stderr else str(e)
            logger.error(f"FFmpeg video note conversion failed: {error_msg}")
            raise

    def compress_video(self, input_path, output_dir, target_size_mb=49):
        """
        Compress video to target size (MB) using 1-pass variable bitrate.
        Target size defaults to 49MB to be safe for Telegram (50MB limit).
        """
        try:
            import subprocess
            import math
            
            # 1. Get duration
            cmd_probe = [
                'ffprobe', '-v', 'error', 
                '-show_entries', 'format=duration', 
                '-of', 'default=noprint_wrappers=1:nokey=1', 
                input_path
            ]
            try:
                result = subprocess.run(cmd_probe, capture_output=True, text=True, check=True)
                duration = float(result.stdout.strip())
            except Exception as e:
                logger.error(f"[COMPRESS] Failed to get duration: {e}")
                return None
                
            if duration <= 0:
                logger.error("[COMPRESS] Invalid duration")
                return None
                
            # 2. Calculate bitrate
            # Target size in bits
            target_bits = target_size_mb * 8 * 1024 * 1024
            # Audio bitrate (128k default)
            audio_bitrate_kbps = 128
            audio_bits = audio_bitrate_kbps * 1024 * duration
            
            # Video bits available
            video_bits = target_bits - audio_bits
            
            if video_bits <= 0:
                # Video too short or target too small, just try with minimum bitrate
                video_bitrate_kbps = 100
            else:
                video_bitrate_bps = video_bits / duration
                video_bitrate_kbps = video_bitrate_bps / 1024
            
            # Safety margin (90%)
            video_bitrate_kbps = video_bitrate_kbps * 0.9
            
            # Minimum bitrate 50k
            if video_bitrate_kbps < 50:
                video_bitrate_kbps = 50
                
            logger.info(f"[COMPRESS] Target: {target_size_mb}MB, Duration: {duration}s, Bitrate: {int(video_bitrate_kbps)}k")
            
            base_name = os.path.splitext(os.path.basename(input_path))[0]
            output_path = os.path.join(output_dir, f"{base_name}_compressed.mp4")
            
            # 3. Compress - ВСЕГДА создаем правильный H.264 + AAC для Telegram
            cmd = [
                'ffmpeg', '-y', '-i', input_path,
                '-c:v', 'libx264',  # H.264 для совместимости
                '-b:v', f'{int(video_bitrate_kbps)}k',
                '-maxrate', f'{int(video_bitrate_kbps * 1.5)}k',
                '-bufsize', f'{int(video_bitrate_kbps * 2)}k',
                '-preset', 'medium', # Better compression per bit than ultrafast
                '-pix_fmt', 'yuv420p',  # Обязательно для совместимости с Telegram
                '-c:a', 'aac',  # AAC для совместимости
                '-b:a', '128k',
                '-movflags', '+faststart',
                output_path
            ]
            
            if os.name != 'nt':
                cmd = ['nice', '-n', '15'] + cmd
                
            logger.info(f"[COMPRESS] Running: {' '.join(cmd)}")
            subprocess.run(cmd, check=True, timeout=900) # 15 min limit
            
            if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                 return output_path
            return None
            
        except Exception as e:
            logger.error(f"[COMPRESS] Error: {e}", exc_info=True)
            return None

    def fix_video_for_telegram(self, input_path, output_dir):
        """
        Re-encode video with high quality settings ("Ideal MP4").
        Used for the 'Fix' button.
        """
        try:
            base_name = os.path.splitext(os.path.basename(input_path))[0]
            output_path = os.path.join(output_dir, f"{base_name}_fixed.mp4")
            
            cmd = [
                'ffmpeg', '-y', '-i', input_path,
                '-c:v', 'libx264',
                '-preset', 'medium',
                '-crf', '23', 
                '-c:a', 'aac',
                '-b:a', '128k',
                '-movflags', '+faststart',
                '-pix_fmt', 'yuv420p',
                output_path
            ]
            
            if os.name != 'nt':
                cmd = ['nice', '-n', '15'] + cmd
                
            logger.info(f"[FIX] Running: {' '.join(cmd)}")
            subprocess.run(cmd, check=True, timeout=600)
            
            if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                 return output_path
            return None
        except Exception as e:
            logger.error(f"[FIX] Error: {e}", exc_info=True)
            return None

    def generate_thumbnail(self, video_path, output_dir, time_offset=1.0):
        """
        Генерирует JPEG миниатюру из видео (для предпросмотра в Telegram).
        
        Args:
            video_path: Путь к видео файлу
            output_dir: Директория для сохранения thumbnail
            time_offset: Время в секундах от начала видео для кадра (по умолчанию 1 сек)
        
        Returns:
            Путь к JPEG файлу или None при ошибке
        """
        try:
            base_name = os.path.splitext(os.path.basename(video_path))[0]
            thumbnail_path = os.path.join(output_dir, f"{base_name}_thumb.jpg")
            
            # Генерируем thumbnail: берем кадр на time_offset секунде, масштабируем до 320x320 (макс для Telegram)
            cmd = [
                'ffmpeg', '-y',
                '-ss', str(time_offset),  # Переходим к нужному моменту
                '-i', video_path,
                '-vf', 'scale=320:320:force_original_aspect_ratio=decrease',  # Масштабируем, сохраняя пропорции
                '-frames:v', '1',  # Только один кадр
                '-q:v', '2',  # Высокое качество JPEG (2 = лучшее, 31 = худшее)
                thumbnail_path
            ]
            
            if os.name != 'nt':
                cmd = ['nice', '-n', '15'] + cmd
            
            logger.info(f"[THUMB] Generating thumbnail: {os.path.basename(video_path)}")
            subprocess.run(cmd, check=True, capture_output=True, timeout=10)
            
            if os.path.exists(thumbnail_path):
                file_size = os.path.getsize(thumbnail_path)
                # Проверяем размер (должен быть <200KB для Telegram)
                if file_size > 200 * 1024:
                    # Если слишком большой, пересжимаем
                    logger.warning(f"[THUMB] Thumbnail too large ({file_size/1024:.1f}KB), recompressing...")
                    temp_path = os.path.join(output_dir, f"{base_name}_thumb_temp.jpg")
                    cmd_compress = [
                        'ffmpeg', '-y',
                        '-i', thumbnail_path,
                        '-vf', 'scale=320:320:force_original_aspect_ratio=decrease',
                        '-q:v', '5',  # Немного хуже качество для меньшего размера
                        temp_path
                    ]
                    if os.name != 'nt':
                        cmd_compress = ['nice', '-n', '15'] + cmd_compress
                    subprocess.run(cmd_compress, check=True, capture_output=True, timeout=10)
                    if os.path.exists(temp_path) and os.path.getsize(temp_path) < 200 * 1024:
                        os.replace(temp_path, thumbnail_path)
                    elif os.path.exists(temp_path):
                        os.remove(temp_path)
                
                final_size = os.path.getsize(thumbnail_path)
                logger.info(f"[THUMB] ✅ Generated: {final_size/1024:.1f}KB")
                return thumbnail_path
            else:
                logger.error(f"[THUMB] ❌ Thumbnail file not created")
                return None
                
        except subprocess.TimeoutExpired:
            logger.error(f"[THUMB] ❌ Timeout generating thumbnail")
            return None
        except subprocess.CalledProcessError as e:
            logger.error(f"[THUMB] ❌ FFmpeg error: {e.stderr.decode() if e.stderr else str(e)}")
            return None
        except Exception as e:
            logger.error(f"[THUMB] ❌ Error generating thumbnail: {e}", exc_info=True)
            return None


