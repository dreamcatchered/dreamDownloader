import sqlite3
import logging
import json
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_file="bot_database.db"):
        self.connection = sqlite3.connect(db_file, check_same_thread=False)
        self.cursor = self.connection.cursor()
        self.create_tables()

    def create_tables(self):
        try:
            # Таблица пользователей
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER UNIQUE,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    language_code TEXT,
                    registration_date DATETIME
                )
            """)
            
            # Проверяем, существует ли таблица file_cache
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='file_cache'")
            table_exists = self.cursor.fetchone()
            
            if table_exists:
                # Таблица существует - проверяем, есть ли колонка id
                self.cursor.execute("PRAGMA table_info(file_cache)")
                columns = [col[1] for col in self.cursor.fetchall()]
                
                if 'id' not in columns:
                    # Миграция: создаем новую таблицу с id
                    logger.info("Migrating file_cache table to add id column...")
                    self.cursor.execute("""
                        CREATE TABLE file_cache_new (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            url TEXT UNIQUE,
                            file_id TEXT,
                            media_type TEXT,
                            uploader_id INTEGER,
                            created_at DATETIME
                        )
                    """)
                    # Копируем данные
                    self.cursor.execute("INSERT INTO file_cache_new (url, file_id, media_type, uploader_id, created_at) SELECT url, file_id, media_type, uploader_id, created_at FROM file_cache")
                    # Удаляем старую таблицу
                    self.cursor.execute("DROP TABLE file_cache")
                    # Переименовываем новую
                    self.cursor.execute("ALTER TABLE file_cache_new RENAME TO file_cache")
                    logger.info("Migration completed successfully.")
                else:
                    logger.debug("Column id already exists in file_cache.")
            else:
                # Создаем новую таблицу с правильной структурой
                self.cursor.execute("""
                    CREATE TABLE file_cache (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        url TEXT UNIQUE,
                        file_id TEXT, -- JSON массив для каруселей: ["file_id1", "file_id2"] или просто "file_id"
                        media_type TEXT, -- 'photo' or 'video' or 'audio' or 'carousel'
                        uploader_id INTEGER,
                        created_at DATETIME
                    )
                """)
            
            # Создаем индекс на url для быстрого поиска
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_url ON file_cache(url)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_id ON file_cache(id)")
            
            # Таблица для хранения расшифровок голосовых сообщений
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS transcriptions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_unique_id TEXT UNIQUE,
                    user_id INTEGER,
                    transcription_text TEXT,
                    created_at DATETIME
                )
            """)
            
            # Создаем индексы для быстрого поиска
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_file_unique_id ON transcriptions(file_unique_id)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_id ON transcriptions(user_id)")
            
            # Таблица для хранения информации о скачанных файлах
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS downloaded_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE,
                    file_path TEXT,
                    file_size INTEGER,
                    file_type TEXT,
                    media_type TEXT,
                    task_dir TEXT,
                    downloaded_at DATETIME,
                    expires_at DATETIME,
                    cache_id INTEGER,
                    FOREIGN KEY (cache_id) REFERENCES file_cache(id)
                )
            """)
            
            # Создаем индексы для быстрого поиска
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_downloaded_url ON downloaded_files(url)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_downloaded_cache_id ON downloaded_files(cache_id)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_downloaded_expires ON downloaded_files(expires_at)")
            
            self.connection.commit()
            logger.info("Database tables checked/created.")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")

    def add_user(self, user):
        try:
            self.cursor.execute("""
                INSERT OR IGNORE INTO users (telegram_id, username, first_name, last_name, language_code, registration_date)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (user.id, user.username, user.first_name, user.last_name, user.language_code, datetime.now()))
            self.connection.commit()
        except Exception as e:
            logger.error(f"Error adding user: {e}")

    def get_cached_file(self, url):
        try:
            self.cursor.execute("SELECT file_id, media_type FROM file_cache WHERE url = ?", (url,))
            result = self.cursor.fetchone()
            if result:
                file_id_str, media_type = result
                # Если это JSON (карусель), парсим
                try:
                    file_ids = json.loads(file_id_str)
                    return file_ids if isinstance(file_ids, list) else [file_ids], media_type
                except:
                    # Один файл
                    return [file_id_str], media_type
            return None
        except Exception as e:
            logger.error(f"Error getting cache: {e}")
            return None

    def save_file_to_cache(self, url, file_ids, media_type, user_id):
        """Сохраняет один file_id или список file_ids в кэш. Возвращает id записи."""
        try:
            # Преобразуем в JSON строку
            if isinstance(file_ids, list):
                file_id_str = json.dumps(file_ids)
                if len(file_ids) > 1:
                    media_type = 'carousel'  # Множество файлов
            else:
                file_id_str = str(file_ids)
            
            # Проверяем, существует ли запись с таким url
            self.cursor.execute("SELECT id FROM file_cache WHERE url = ?", (url,))
            existing = self.cursor.fetchone()
            
            if existing:
                # Обновляем существующую запись
                cache_id = existing[0]
                self.cursor.execute("""
                    UPDATE file_cache 
                    SET file_id = ?, media_type = ?, uploader_id = ?, created_at = ?
                    WHERE id = ?
                """, (file_id_str, media_type, user_id, datetime.now(), cache_id))
            else:
                # Вставляем новую запись
                self.cursor.execute("""
                    INSERT INTO file_cache (url, file_id, media_type, uploader_id, created_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (url, file_id_str, media_type, user_id, datetime.now()))
                cache_id = self.cursor.lastrowid
            
            self.connection.commit()
            return cache_id
        except Exception as e:
            logger.error(f"Error saving to cache: {e}")
            return None
    
    def get_file_by_id(self, cache_id):
        """Получает file_id и media_type по id из кэша"""
        try:
            self.cursor.execute("SELECT file_id, media_type FROM file_cache WHERE id = ?", (cache_id,))
            result = self.cursor.fetchone()
            if result:
                file_id_str, media_type = result
                # Если это JSON (карусель), парсим
                try:
                    file_ids = json.loads(file_id_str)
                    return file_ids if isinstance(file_ids, list) else [file_ids], media_type
                except:
                    # Один файл
                    return [file_id_str], media_type
            return None
        except Exception as e:
            logger.error(f"Error getting file by id: {e}")
            return None
    
    def get_cache_id_by_url(self, url):
        """Получает cache_id по URL"""
        try:
            self.cursor.execute("SELECT id FROM file_cache WHERE url = ?", (url,))
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Error getting cache_id by url: {e}")
            return None

    def save_transcription(self, file_unique_id, user_id, transcription_text):
        """Сохраняет расшифровку в базу данных"""
        try:
            self.cursor.execute("""
                INSERT OR REPLACE INTO transcriptions (file_unique_id, user_id, transcription_text, created_at)
                VALUES (?, ?, ?, ?)
            """, (file_unique_id, user_id, transcription_text, datetime.now()))
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error saving transcription: {e}")
            return False
    
    def get_transcription(self, file_unique_id, user_id=None):
        """Получает расшифровку по file_unique_id"""
        try:
            if user_id:
                self.cursor.execute("""
                    SELECT transcription_text FROM transcriptions 
                    WHERE file_unique_id = ? AND user_id = ?
                """, (file_unique_id, user_id))
            else:
                self.cursor.execute("""
                    SELECT transcription_text FROM transcriptions 
                    WHERE file_unique_id = ?
                """, (file_unique_id,))
            
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Error getting transcription: {e}")
            return None
    
    def get_user_transcriptions(self, user_id):
        """Получает все расшифровки пользователя"""
        try:
            self.cursor.execute("""
                SELECT file_unique_id, transcription_text FROM transcriptions 
                WHERE user_id = ?
                ORDER BY created_at DESC
            """, (user_id,))
            return dict(self.cursor.fetchall())
        except Exception as e:
            logger.error(f"Error getting user transcriptions: {e}")
            return {}
    
    def delete_transcription(self, file_unique_id, user_id=None):
        """Удаляет расшифровку"""
        try:
            if user_id:
                self.cursor.execute("""
                    DELETE FROM transcriptions 
                    WHERE file_unique_id = ? AND user_id = ?
                """, (file_unique_id, user_id))
            else:
                self.cursor.execute("""
                    DELETE FROM transcriptions 
                    WHERE file_unique_id = ?
                """, (file_unique_id,))
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error deleting transcription: {e}")
            return False
    
    def save_downloaded_file(self, url, file_path, file_size, file_type, media_type, task_dir, cache_id=None, expires_hours=24):
        """Сохраняет информацию о скачанном файле"""
        try:
            import os
            from datetime import datetime, timedelta
            
            # Проверяем, существует ли файл
            if not os.path.exists(file_path):
                logger.warning(f"File does not exist: {file_path}")
                return None
            
            expires_at = datetime.now() + timedelta(hours=expires_hours)
            
            # Проверяем, существует ли запись с таким url
            self.cursor.execute("SELECT id FROM downloaded_files WHERE url = ?", (url,))
            existing = self.cursor.fetchone()
            
            if existing:
                # Обновляем существующую запись
                file_id = existing[0]
                self.cursor.execute("""
                    UPDATE downloaded_files 
                    SET file_path = ?, file_size = ?, file_type = ?, media_type = ?, 
                        task_dir = ?, downloaded_at = ?, expires_at = ?, cache_id = ?
                    WHERE id = ?
                """, (file_path, file_size, file_type, media_type, task_dir, 
                      datetime.now(), expires_at, cache_id, file_id))
            else:
                # Вставляем новую запись
                self.cursor.execute("""
                    INSERT INTO downloaded_files (url, file_path, file_size, file_type, media_type, task_dir, downloaded_at, expires_at, cache_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (url, file_path, file_size, file_type, media_type, task_dir, 
                      datetime.now(), expires_at, cache_id))
                file_id = self.cursor.lastrowid
            
            self.connection.commit()
            return file_id
        except Exception as e:
            logger.error(f"Error saving downloaded file: {e}")
            return None
    
    def get_downloaded_file(self, url):
        """Получает информацию о скачанном файле, если он существует и не истек срок"""
        try:
            import os
            from datetime import datetime
            
            self.cursor.execute("""
                SELECT file_path, file_size, file_type, media_type, task_dir, cache_id, expires_at
                FROM downloaded_files 
                WHERE url = ? AND expires_at > ?
            """, (url, datetime.now()))
            
            result = self.cursor.fetchone()
            if result:
                file_path, file_size, file_type, media_type, task_dir, cache_id, expires_at = result
                
                # Проверяем, существует ли файл на диске
                if os.path.exists(file_path):
                    return {
                        'file_path': file_path,
                        'file_size': file_size,
                        'file_type': file_type,
                        'media_type': media_type,
                        'task_dir': task_dir,
                        'cache_id': cache_id
                    }
                else:
                    # Файл удален, удаляем запись из БД
                    logger.info(f"File no longer exists on disk: {file_path}, removing from DB")
                    self.cursor.execute("DELETE FROM downloaded_files WHERE url = ?", (url,))
                    self.connection.commit()
            
            return None
        except Exception as e:
            logger.error(f"Error getting downloaded file: {e}")
            return None
    
    def delete_downloaded_file(self, url):
        """Удаляет информацию о скачанном файле"""
        try:
            self.cursor.execute("DELETE FROM downloaded_files WHERE url = ?", (url,))
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error deleting downloaded file: {e}")
            return False
    
    def cleanup_expired_files(self):
        """Удаляет записи об истекших файлах"""
        try:
            from datetime import datetime
            import os
            
            self.cursor.execute("""
                SELECT id, file_path, task_dir FROM downloaded_files 
                WHERE expires_at < ?
            """, (datetime.now(),))
            
            expired = self.cursor.fetchall()
            deleted_count = 0
            
            for file_id, file_path, task_dir in expired:
                # Удаляем файл и папку если они существуют
                try:
                    if file_path and os.path.exists(file_path):
                        os.remove(file_path)
                    if task_dir and os.path.exists(task_dir):
                        import shutil
                        shutil.rmtree(task_dir, ignore_errors=True)
                except Exception as e:
                    logger.warning(f"Error cleaning up file {file_path}: {e}")
                
                # Удаляем запись из БД
                self.cursor.execute("DELETE FROM downloaded_files WHERE id = ?", (file_id,))
                deleted_count += 1
            
            self.connection.commit()
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} expired file records")
            return deleted_count
        except Exception as e:
            logger.error(f"Error cleaning up expired files: {e}")
            return 0
    
    def close(self):
        self.connection.close()

