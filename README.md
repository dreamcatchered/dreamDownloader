# dreamDownloader

Telegram bot for downloading media from YouTube, Instagram, TikTok and other platforms.

Send a link — get the file. Supports video, audio, reels and playlists. Includes a Flask REST API for external integrations.

## Features

- YouTube — video and audio (MP3) download
- Instagram — posts, reels, stories
- TikTok and other platforms via yt-dlp
- AI-powered video summarization
- Flask REST API
- Proxy support
- Download history

## Stack

![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-000000?style=flat&logo=flask&logoColor=white)
![yt-dlp](https://img.shields.io/badge/yt--dlp-FF0000?style=flat&logo=youtube&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-003B57?style=flat&logo=sqlite&logoColor=white)

## Setup

```bash
pip install -r requirements.txt
```

Set environment variables:
```env
BOT_TOKEN=your_bot_token
API_TOKEN=your_ai_api_token
PROXY_URL=http://user:pass@host:port
```

```bash
python bot.py
```

For age-restricted content, add your own `ig_cookies.txt` and `yt_cookies.txt`.

## Contact

Telegram: [@dreamcatch_r](https://t.me/dreamcatch_r)
