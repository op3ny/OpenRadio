#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
OpenRadio Radio Player (v2.0.0)
------------------------------------------------------------
- Fixed undefined 'limitself' error in track filters generation
- Removed search limit for SoundCloud queries
- Enforced 20-second delay between SoundCloud requests
- Ensured continuous streaming with aggressive queue refilling
- Fixed stalling by improving buffer and stream loop logic
- Added detailed logging for queue and streaming issues
- Maintained original optimizations with enhanced reliability
"""

import os
import sys
import json
import time
import random
import logging
import argparse
import subprocess
import requests
import signal
import threading
import queue
from datetime import datetime, timedelta
from collections import defaultdict, deque
from enum import Enum, auto
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Deque, Any, Set
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor
import re

# Constants
VERSION = "8.4.9 Ultra-Optimized"
DEFAULT_USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
MAX_RETRIES = 3
RETRY_DELAY = 2
BUFFER_CHECK_INTERVAL = 0.5
TRANSITION_DURATION = 3
MIN_BUFFER_SIZE = 1
MAX_QUEUE_SIZE = 5 
STREAM_TIMEOUT = 30
PRELOAD_WORKERS = 2
FFMPEG_MONITOR_INTERVAL = 0.5
FFMPEG_RESTART_DELAY = 0
PLAYBACK_END_MARGIN = 5
DISCLAIMER_DURATION = 5
CONTROL_PIPE = "/tmp/openradio_control_pipe"
CONTROL_COMMANDS = {"pause", "resume", "skip", "shutdown", "volume_up", "volume_down"}

# Setup logging
def setup_logging(base_dir: str) -> logging.Logger:
    log_dir = os.path.join(base_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger("OPENRadio")
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    main_log = os.path.join(log_dir, "open_radio.log")
    file_handler = logging.FileHandler(main_log)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

class JsonLogger:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.log_dir = os.path.join(base_dir, "logs", "json")
        os.makedirs(self.log_dir, exist_ok=True)

    def _write_json_log(self, filename: str, data: Dict) -> None:
        try:
            filepath = os.path.join(self.log_dir, filename)
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2, default=str)
        except Exception as e:
            logging.getLogger("OPENRadio").error(f"Error writing JSON log: {e}")

    def log_current_track(self, track: Dict) -> None:
        self._write_json_log("current_track.json", track)

    def log_next_track(self, track: Dict) -> None:
        self._write_json_log("next_track.json", track if track else {})

    def log_previous_track(self, track: Dict) -> None:
        self._write_json_log("previous_track.json", track if track else {})

    def log_queue(self, queue: List[Dict]) -> None:
        self._write_json_log("upcoming_tracks.json", {"tracks": queue})

    def log_history(self, history: List[Dict]) -> None:
        self._write_json_log("played_tracks.json", {"tracks": history})

    def log_artists(self, artists: Dict[str, int]) -> None:
        self._write_json_log("artist_stats.json", artists)

    def log_status(self, status: Dict) -> None:
        self._write_json_log("player_status.json", status)

@dataclass
class Track:
    id: str
    title: str
    artist: str
    duration: float
    url: str
    source: str
    audio_path: str
    ready: bool = False
    last_played: Optional[datetime] = None
    play_count: int = 0
    normalized_title: str = ""

    def to_dict(self) -> Dict:
        data = asdict(self)
        if self.last_played:
            data['last_played'] = self.last_played.isoformat()
        return data

class PlayerState(Enum):
    IDLE = auto()
    BUFFERING = auto()
    PLAYING = auto()
    ERROR = auto()
    SHUTDOWN = auto()
    PAUSED = auto()

DEFAULT_CONFIG = {
    "system": {
        "played_file": "data/played.json",
        "stats_file": "data/stats.json",
        "temp_dir": "temp",
        "cache_dir": "cache",
        "max_history": 50,
        "buffer_size": MIN_BUFFER_SIZE,
        "max_cache_size": 5,  # Increased
        "track_buffer_size": MAX_QUEUE_SIZE,
        "preload_ahead": 3,  # Increased for more slack
        "control_pipe": CONTROL_PIPE,
    },
    "stream": {
        "width": 1280,
        "height": 720,
        "fps": 30,
        "bitrate": "1000k",
        "audio_bitrate": "128k",
        "crf": 28,
        "preset": "veryfast",
        "threads": 2,
        "keyframe_interval": 60,
        "rtmp_url": "rtmp://SUAURLRTMP",
        "stream_key": "SUA-STREAM-KEY",
    },
    "player": {
        "min_duration": 120,
        "max_duration": 3600,
        "max_consecutive_artists": 2,
        "artist_cooldown": 5,
        "track_cooldown": 30,
        "retry_delay": 5,  # Reduced for faster retries
        "min_buffer_size": MIN_BUFFER_SIZE,
        "max_empty_retries": 20,  # Increased for resilience
        "buffer_refill_threshold": 3,  # Refill if below 3
        "ffmpeg_restart_delay": FFMPEG_RESTART_DELAY,
        "playback_end_margin": PLAYBACK_END_MARGIN,
        "disclaimer_duration": DISCLAIMER_DURATION,
        "volume": 80,
    },
    "visuals": {
        "transition_duration": TRANSITION_DURATION,
        "font_size_title": 48,
        "font_size_artist": 36,
        "font_size_info": 24,
        "margin": 30,
        "text_color": "white",
        "highlight_color": "#00FFCC",
        "background_colors": [
            "#1E90FF", "#32CD32", "#FF4500", "#9932CC",
            "#FF1493", "#00CED1", "#FFD700"
        ],
        "font_file": "DejaVuSans-Bold.ttf",
        "logo_text": "OPENRADIO",
        "credits_text": "Criado por op3n",
        "live_indicator": "• AO VIVO •",
        "box_opacity": 0.7,
        "box_border": 5,
        "disclaimer_text": "AVISO LEGAL\nEste conteúdo é protegido por direitos autorais.\nA transmissão é realizada apenas para fins educativos e de entretenimento.\nTodo o conteúdo audiovisual é de fontes públicas\nOPEN Rádio não armazena ou reproduz conteúdo de forma ilegal.",
    },
    "search": {
        "max_results": 50,  # Increased to handle more results
        "backoff_factor": 2,
        "max_retries": 3,
        "user_agent": DEFAULT_USER_AGENT,
        "search_url": "https://soundcloud.com/search/sounds?q=",
        "timeout": 30,
        "download_timeout": 200,
        "request_delay": 20.0,  # Fixed 20s delay
        "max_consecutive_failures": 5,  # Increased
    },
    "queries": [
        "Chico Buarque",
        "Os Paralamas do Sucesso",
        "Mato Seco"
    ]
}

class TrackManager:
    def __init__(self, config: Dict, logger: logging.Logger, json_logger: JsonLogger):
        self.config = config
        self.logger = logger
        self.json_logger = json_logger
        self.played_tracks: Deque[Track] = deque(maxlen=self.config["system"]["max_history"])
        self.track_stats = defaultdict(int)
        self.artist_stats = defaultdict(int)
        self.title_stats = defaultdict(int)
        self.last_artists = deque(maxlen=self.config["player"]["max_consecutive_artists"])
        self.lock = threading.Lock()
        self._load_data()

    def _normalize_title(self, title: str) -> str:
        normalized = re.sub(r'\([^)]*\)|\[[^\]]*\]|\W+', '', title.lower())
        return normalized.strip()

    def _load_data(self) -> None:
        try:
            played_file = self.config["system"]["played_file"]
            if os.path.isdir(played_file):
                self.logger.warning(f"'{played_file}' is a directory, removing it")
                import shutil
                shutil.rmtree(played_file)
            if not os.path.exists(played_file):
                os.makedirs(os.path.dirname(played_file), exist_ok=True)
                with open(played_file, 'w') as f:
                    json.dump([], f)

            with open(played_file, 'r') as f:
                history = json.load(f)
                with self.lock:
                    self.played_tracks = deque(
                        [self._dict_to_track(track) for track in history if isinstance(track, dict)],
                        maxlen=self.config["system"]["max_history"])

            stats_file = self.config["system"]["stats_file"]
            if os.path.isdir(stats_file):
                self.logger.warning(f"'{stats_file}' is a directory, removing it")
                import shutil
                shutil.rmtree(stats_file)
            if not os.path.exists(stats_file):
                os.makedirs(os.path.dirname(stats_file), exist_ok=True)
                with open(stats_file, 'w') as f:
                    json.dump({"tracks": {}, "artists": {}, "titles": {}}, f)

            with open(stats_file, 'r') as f:
                stats = json.load(f)
                with self.lock:
                    self.track_stats = defaultdict(int, stats.get("tracks", {}))
                    self.artist_stats = defaultdict(int, stats.get("artists", {}))
                    self.title_stats = defaultdict(int, stats.get("titles", {}))
                    self.json_logger.log_artists(self.artist_stats)
        except Exception as e:
            self.logger.error(f"Error loading data: {e}")

    def _dict_to_track(self, data: Dict) -> Track:
        track = Track(
            id=data.get("id", ""),
            title=data.get("title", "Unknown Title"),
            artist=data.get("artist", "Unknown Artist"),
            duration=data.get("duration", 0),
            url=data.get("url", ""),
            source=data.get("source", ""),
            audio_path=data.get("audio_path", ""),
            ready=data.get("ready", False),
            last_played=datetime.fromisoformat(data["last_played"]) if data.get("last_played") else None,
            play_count=data.get("play_count", 0)
        )
        track.normalized_title = self._normalize_title(track.title)
        return track

    def save_data(self) -> bool:
        try:
            with self.lock:
                played_copy = [track.to_dict() for track in self.played_tracks]
                track_stats_copy = dict(self.track_stats)
                artist_stats_copy = dict(self.artist_stats)
                title_stats_copy = dict(self.title_stats)

            played_file = self.config["system"]["played_file"]
            os.makedirs(os.path.dirname(played_file), exist_ok=True)
            with open(played_file, 'w') as f:
                json.dump(played_copy, f, indent=2)

            stats_file = self.config["system"]["stats_file"]
            os.makedirs(os.path.dirname(stats_file), exist_ok=True)
            with open(stats_file, 'w') as f:
                json.dump({
                    "tracks": track_stats_copy,
                    "artists": artist_stats_copy,
                    "titles": title_stats_copy,
                    "last_update": datetime.now().isoformat(),
                    "total_plays": sum(track_stats_copy.values())
                }, f, indent=2)

            self.json_logger.log_history(played_copy)
            self.json_logger.log_artists(artist_stats_copy)
            return True
        except Exception as e:
            self.logger.error(f"Error saving data: {e}")
            return False

    def add_played_track(self, track: Track) -> None:
        if not track or not track.id or not track.artist:
            return

        need_save = False
        with self.lock:
            track.last_played = datetime.now()
            track.play_count = self.track_stats.get(track.id, 0) + 1
            self.played_tracks.append(track)
            self.track_stats[track.id] += 1
            self.artist_stats[track.artist] += 1
            self.title_stats[track.normalized_title] += 1
            self.last_artists.append(track.artist)
            need_save = (len(self.played_tracks) % 5 == 0)

        self.json_logger.log_current_track(track.to_dict())
        if need_save:
            self.save_data()

    def is_recent_artist(self, artist: str) -> bool:
        with self.lock:
            return artist in self.last_artists

    def is_recent_track(self, track_id: str) -> bool:
        with self.lock:
            for track in self.played_tracks:
                if track.id == track_id:
                    cooldown = self.config["player"]["track_cooldown"] * 60
                    return (datetime.now() - track.last_played).total_seconds() < cooldown
            return False

    def is_duplicate_title(self, title: str) -> bool:
        normalized = self._normalize_title(title)
        with self.lock:
            return self.title_stats.get(normalized, 0) > 0

    def cleanup_cache(self) -> None:
        cache_dir = self.config["system"]["cache_dir"]
        os.makedirs(cache_dir, exist_ok=True)

        try:
            with self.lock:
                valid_files = {track.id for track in self.played_tracks if track.id}

            for file in os.listdir(cache_dir):
                if file.endswith(".m4a") and file[:-4] not in valid_files:
                    try:
                        os.remove(os.path.join(cache_dir, file))
                        self.logger.info(f"Removed orphaned cache file: {file}")
                    except Exception as e:
                        self.logger.error(f"Error removing orphaned file {file}: {e}")
        except Exception as e:
            self.logger.error(f"Error during cache cleanup: {e}")

class SoundCloudAPI:
    def __init__(self, config: Dict, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self.config["search"]["user_agent"],
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Referer": "https://soundcloud.com/",
        })
        self.current_query = ""
        self.last_request_time = 0
        self.consecutive_failures = 0
        self.request_history = deque(maxlen=50)
        self.user_agents = self._generate_user_agents()
        self.current_agent_index = 0

    def _generate_user_agents(self) -> List[str]:
        return [
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
        ]

    def _normalize_title(self, title: str) -> str:
        normalized = re.sub(r'\([^)]*\)|\[[^\]]*\]|\W+', '', title.lower())
        return normalized.strip()

    def _rotate_user_agent(self) -> None:
        self.current_agent_index = (self.current_agent_index + 1) % len(self.user_agents)
        self.session.headers.update({
            "User-Agent": self.user_agents[self.current_agent_index]
        })

    def _record_request(self, url: str, success: bool) -> None:
        timestamp = time.time()
        self.request_history.append({
            "timestamp": timestamp,
            "url": url,
            "success": success,
            "delay": self.config["search"]["request_delay"]
        })

    def _enforce_rate_limit(self) -> None:
        time_since_last = time.time() - self.last_request_time
        required_delay = self.config["search"]["request_delay"]
        if time_since_last < required_delay:
            wait_time = required_delay - time_since_last
            self.logger.debug(f"Rate limiting: waiting {wait_time:.1f}s")
            time.sleep(wait_time)
        self.last_request_time = time.time()

    def search_tracks(self, query: str) -> List[str]:
        self.logger.debug(f"Searching for: {query} (no limit)")
        self.current_query = query

        self._enforce_rate_limit()
        self._rotate_user_agent()

        try:
            url = self.config["search"]["search_url"] + query.replace(' ', '+')
            response = self.session.get(
                url,
                timeout=self.config["search"]["timeout"]
            )

            if response.status_code == 429:
                self.consecutive_failures += 1
                self._record_request(url, False)
                self.logger.warning("Rate limit exceeded (429)")
                return []

            response.raise_for_status()

            self.consecutive_failures = 0
            self._record_request(url, True)

            soup = BeautifulSoup(response.text, 'html.parser')
            links = []

            for a in soup.find_all('a', href=True):
                href = a['href']
                if href.count('/') == 2 and not href.startswith(('/you/', '/search', '/discover')):
                    full_url = f"https://soundcloud.com{href}"
                    if full_url not in links:
                        links.append(full_url)

            self.logger.debug(f"Found {len(links)} tracks for query: {query}")
            return links

        except Exception as e:
            self.consecutive_failures += 1
            self._record_request(url, False)
            self.logger.error(f"Search error: {e}")
            return []

    def get_track_info(self, track_url: str) -> Optional[Track]:
        for attempt in range(self.config["search"]["max_retries"]):
            try:
                self._enforce_rate_limit()
                self._rotate_user_agent()

                cmd = [
                    'yt-dlp',
                    '--dump-json',
                    '--no-playlist',
                    '--no-warnings',
                    '--referer', 'https://soundcloud.com/',
                    '--user-agent', self.user_agents[self.current_agent_index],
                    track_url
                ]

                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=self.config["search"]["timeout"],
                    check=True,
                    preexec_fn=lambda: os.nice(19)
                )

                info = json.loads(result.stdout)
                track = Track(
                    id=str(info.get('id', '')),
                    title=info.get('title', 'Unknown Title'),
                    artist=info.get('uploader', 'Unknown Artist'),
                    duration=info.get('duration', 0),
                    url=track_url,
                    source=self.current_query,
                    audio_path="",
                    ready=False
                )
                track.normalized_title = self._normalize_title(track.title)

                self.consecutive_failures = 0
                self._record_request(track_url, True)
                return track

            except subprocess.TimeoutExpired as e:
                self.consecutive_failures += 1
                self._record_request(track_url, False)
                self.logger.warning(f"Info timeout attempt {attempt + 1}: {e}")
                time.sleep(RETRY_DELAY * (attempt + 1))
            except subprocess.CalledProcessError as e:
                self.consecutive_failures += 1
                self._record_request(track_url, False)
                self.logger.warning(f"Info error attempt {attempt + 1}: {e.stderr}")
                time.sleep(RETRY_DELAY * (attempt + 1))
            except Exception as e:
                self.consecutive_failures += 1
                self._record_request(track_url, False)
                self.logger.warning(f"Info error attempt {attempt + 1}: {e}")
                time.sleep(RETRY_DELAY * (attempt + 1))

        return None

    def download_track(self, track: Track) -> Optional[Track]:
        if not track or not track.url:
            return None

        cache_dir = self.config["system"]["cache_dir"]
        audio_path = os.path.join(cache_dir, f"{track.id}.m4a")

        if os.path.exists(audio_path):
            track.audio_path = audio_path
            track.ready = True
            return track

        audio_downloaded = False
        for attempt in range(self.config["search"]["max_retries"]):
            try:
                self._enforce_rate_limit()
                self._rotate_user_agent()

                cmd = [
                    'yt-dlp',
                    '-x', '--audio-format', 'm4a',
                    '--audio-quality', self.config["stream"]["audio_bitrate"],
                    '--no-playlist',
                    '--no-warnings',
                    '--referer', 'https://soundcloud.com/',
                    '--user-agent', self.user_agents[self.current_agent_index],
                    '-o', audio_path,
                    track.url
                ]

                result = subprocess.run(
                    cmd,
                    check=True,
                    timeout=self.config["search"]["download_timeout"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    preexec_fn=lambda: os.nice(19)
                )

                if os.path.exists(audio_path):
                    audio_downloaded = True
                    self.consecutive_failures = 0
                    self._record_request(track.url, True)
                    break

            except subprocess.TimeoutExpired as e:
                self.consecutive_failures += 1
                self._record_request(track.url, False)
                self.logger.warning(f"Download timeout attempt {attempt + 1}: {e}")
                time.sleep(RETRY_DELAY * (attempt + 1))
            except subprocess.CalledProcessError as e:
                self.consecutive_failures += 1
                self._record_request(track.url, False)
                self.logger.warning(f"Download error attempt {attempt + 1}: {e.stderr}")
                time.sleep(RETRY_DELAY * (attempt + 1))
            except Exception as e:
                self.consecutive_failures += 1
                self._record_request(track.url, False)
                self.logger.warning(f"Download error attempt {attempt + 1}: {e}")
                time.sleep(RETRY_DELAY * (attempt + 1))

        if not audio_downloaded:
            return None

        track.audio_path = audio_path
        track.ready = True
        return track

class AudioStreamer:
    def __init__(self, config: Dict, track_manager: TrackManager, soundcloud: SoundCloudAPI, logger: logging.Logger, json_logger: JsonLogger):
        self.config = config
        self.track_manager = track_manager
        self.soundcloud = soundcloud
        self.logger = logger
        self.json_logger = json_logger
        self.stream_process = None
        self.running = False
        self.current_track = None
        self.next_track = None
        self.previous_track = None
        self.track_queue = queue.Queue(maxsize=self.config["system"]["track_buffer_size"])
        self.queued_ids: Set[str] = set()
        self.state = PlayerState.IDLE
        self.state_lock = threading.Lock()
        self.empty_buffer_retries = 0
        self.stream_thread = None
        self.buffer_thread = None
        self.control_thread = None
        self.volume = self.config["player"]["volume"]
        self.skip_requested = threading.Event()
        self.pause_requested = threading.Event()
        self.resume_requested = threading.Event()
        self.shutdown_requested = threading.Event()
        self.volume_change = threading.Event()
        self.new_volume = 80
        self.executor = ThreadPoolExecutor(max_workers=PRELOAD_WORKERS)

    def set_state(self, state: PlayerState) -> None:
        with self.state_lock:
            self.state = state
            self._update_status_log()

    def get_state(self) -> PlayerState:
        with self.state_lock:
            return self.state

    def _update_status_log(self) -> None:
        status = {
            "state": self.state.name,
            "current_track": self.current_track.to_dict() if self.current_track else None,
            "next_track": self.next_track.to_dict() if self.next_track else None,
            "previous_track": self.previous_track.to_dict() if self.previous_track else None,
            "queue_size": self.track_queue.qsize(),
            "volume": self.volume,
            "timestamp": datetime.now().isoformat()
        }
        self.json_logger.log_status(status)

    def start(self) -> None:
        if self.running:
            return

        self.running = True
        self.stream_thread = threading.Thread(
            target=self._stream_loop,
            daemon=True
        )
        self.stream_thread.start()

        self.buffer_thread = threading.Thread(
            target=self._buffer_loop,
            daemon=True
        )
        self.buffer_thread.start()

        self.control_thread = threading.Thread(
            target=self._watch_control_pipe,
            daemon=True
        )
        self.control_thread.start()

    def stop(self) -> None:
        self.running = False
        self.executor.shutdown(wait=False)

        if self.stream_thread:
            self.stream_thread.join(timeout=2)
        if self.buffer_thread:
            self.buffer_thread.join(timeout=2)
        if self.control_thread:
            self.control_thread.join(timeout=2)

        self._stop_stream()

    def _setup_control_pipe(self) -> None:
        control_pipe = self.config["system"]["control_pipe"]
        try:
            if os.path.exists(control_pipe):
                os.remove(control_pipe)
        except:
            pass

        try:
            os.mkfifo(control_pipe)
            self.logger.info(f"Created control pipe at {control_pipe}")
        except Exception as e:
            self.logger.error(f"Error creating control pipe: {e}")

    def _watch_control_pipe(self) -> None:
        self._setup_control_pipe()
        control_pipe = self.config["system"]["control_pipe"]

        while self.running:
            try:
                with open(control_pipe, 'r') as pipe:
                    while self.running:
                        line = pipe.readline().strip()
                        if not line:
                            time.sleep(0.1)
                            continue
                        self._process_command(line.lower())
            except Exception as e:
                self.logger.error(f"Control pipe error: {e}")
                time.sleep(1)

    def _process_command(self, command: str) -> None:
        if command not in CONTROL_COMMANDS:
            return

        if command == "pause":
            self.pause_requested.set()
            self.resume_requested.clear()
        elif command == "resume":
            self.resume_requested.set()
            self.pause_requested.clear()
        elif command == "skip":
            self.skip_requested.set()
        elif command == "volume_up":
            self.new_volume = min(100, self.volume + 5)
            self.volume_change.set()
        elif command == "volume_down":
            self.new_volume = max(0, self.volume - 5)
            self.volume_change.set()
        elif command == "shutdown":
            self.shutdown_requested.set()

    def enqueue_track(self, track: Track) -> bool:
        if not track or not track.ready or not os.path.exists(track.audio_path):
            self.logger.warning(f"Cannot enqueue invalid track: {track}")
            return False

        if track.id in self.queued_ids:
            self.logger.debug(f"Skipping duplicate track in queue: {track.id}")
            return False

        try:
            self.track_queue.put(track, timeout=1)
            self.queued_ids.add(track.id)
            self.logger.debug(f"Enqueued track: {track.title} (ID: {track.id})")
            self._update_queue_log()
            self._update_status_log()
            return True
        except queue.Full:
            self.logger.warning("Queue full, cannot enqueue track")
            return False
        except Exception as e:
            self.logger.error(f"Error enqueueing: {e}")
            return False

    def _update_queue_log(self) -> None:
        queue_list = []
        temp_queue = queue.Queue()

        while not self.track_queue.empty():
            track = self.track_queue.get_nowait()
            queue_list.append(track.to_dict())
            temp_queue.put(track)

        while not temp_queue.empty():
            self.track_queue.put(temp_queue.get_nowait())

        self.json_logger.log_queue(queue_list)

    def _get_rtmp_url(self) -> str:
        rtmp_url = self.config["stream"]["rtmp_url"]
        stream_key = self.config["stream"]["stream_key"]
        return f"{rtmp_url}/{stream_key}"

    def _start_stream(self, audio_path: str, filters: str, volume: int, duration: float) -> bool:
        try:
            volume_filter = f"volume={volume/100.0},atempo=1.0"

            cmd = [
                'ffmpeg',
                '-re',
                '-i', audio_path,
                '-f', 'lavfi',
                '-i', f'color=c={random.choice(self.config["visuals"]["background_colors"])}:s={self.config["stream"]["width"]}x{self.config["stream"]["height"]}:r={self.config["stream"]["fps"]}',
                '-filter_complex', filters,
                '-filter:a', volume_filter,
                '-map', '[v]',
                '-map', '0:a',
                '-c:v', 'libx264',
                '-preset', self.config["stream"]["preset"],
                '-crf', str(self.config["stream"]["crf"]),
                '-tune', 'zerolatency',
                '-b:v', self.config["stream"]["bitrate"],
                '-g', str(self.config["stream"]["keyframe_interval"]),
                '-threads', str(self.config["stream"]["threads"]),
                '-c:a', 'aac',
                '-b:a', self.config["stream"]["audio_bitrate"],
                '-ar', '44100',
                '-vsync', '1',
                '-t', str(duration),
                '-f', 'flv',
                '-loglevel', 'verbose',
                self._get_rtmp_url()
            ]

            self.logger.debug(f"FFmpeg command: {' '.join(cmd)}")

            self.stream_process = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                preexec_fn=lambda: os.nice(-10) if os.getuid() == 0 else os.nice(0)
            )

            def log_ffmpeg_output():
                while self.stream_process and self.stream_process.poll() is None:
                    try:
                        err_line = self.stream_process.stderr.readline()
                        if err_line:
                            self.logger.debug(f"FFmpeg stderr: {err_line.strip()}")
                    except:
                        break
                if self.stream_process and self.stream_process.poll() is not None:
                    rc = self.stream_process.returncode
                    if rc == 0:
                        self.logger.info(f"FFmpeg completed successfully")
                    else:
                        self.logger.error(f"FFmpeg exited with code {rc}")

            threading.Thread(target=log_ffmpeg_output, daemon=True).start()

            time.sleep(0.5)
            if self.stream_process.poll() is None:
                return True
            else:
                self.logger.error("FFmpeg failed to start")
                return False
        except Exception as e:
            self.logger.error(f"Start stream error: {e}")
            return False

    def _stop_stream(self) -> None:
        if self.stream_process:
            try:
                self.stream_process.terminate()
                self.stream_process.wait(timeout=3)
            except:
                self.stream_process.kill()
                self.stream_process.wait()
            finally:
                self.stream_process = None
                self.logger.debug("Stream process stopped")

    def _delete_previous_track(self) -> None:
        if self.previous_track and self.previous_track.audio_path and os.path.exists(self.previous_track.audio_path):
            try:
                os.remove(self.previous_track.audio_path)
                self.logger.info(f"Deleted previous track audio file: {self.previous_track.audio_path}")
            except Exception as e:
                self.logger.error(f"Error deleting previous track audio file {self.previous_track.audio_path}: {e}")

    def _handle_immediate_commands(self) -> None:
        if self.shutdown_requested.is_set():
            self.running = False
            self.logger.info("Shutdown requested")
            return

        if self.volume_change.is_set():
            self.volume = self.new_volume
            self.volume_change.clear()
            self._update_status_log()
            if self.get_state() == PlayerState.PLAYING and self.current_track:
                self._stop_stream()
                filters = self._generate_track_filters(self.current_track)
                self._start_stream(self.current_track.audio_path, filters, self.volume, self.current_track.duration)

        if self.skip_requested.is_set():
            self.skip_requested.clear()
            self._stop_stream()
            self.logger.info("Skip requested")

        if self.pause_requested.is_set():
            self.pause_requested.clear()
            if self.get_state() == PlayerState.PLAYING:
                self.set_state(PlayerState.PAUSED)
                self._stop_stream()
                self.logger.info("Paused")

        if self.resume_requested.is_set():
            self.resume_requested.clear()
            if self.get_state() == PlayerState.PAUSED and self.current_track:
                self.set_state(PlayerState.PLAYING)
                filters = self._generate_track_filters(self.current_track)
                self._start_stream(self.current_track.audio_path, filters, self.volume, self.current_track.duration)
                self.logger.info("Resumed")

    def _stream_track(self, track: Track) -> bool:
        if not track or not track.audio_path or not os.path.exists(track.audio_path):
            self.logger.error(f"Invalid track or audio file missing: {track}")
            return False

        self.set_state(PlayerState.PLAYING)
        self.stream_start_time = time.time()
        filters = self._generate_track_filters(track)

        if not self._start_stream(track.audio_path, filters, self.volume, track.duration):
            self.set_state(PlayerState.ERROR)
            return False

        expected_end_time = self.stream_start_time + track.duration + self.config["player"]["playback_end_margin"]

        while self.running:
            self._handle_immediate_commands()
            if not self.running or self.skip_requested.is_set():
                break

            current_time = time.time()
            if current_time > expected_end_time:
                break

            if self.stream_process and self.stream_process.poll() is not None:
                returncode = self.stream_process.returncode
                if returncode != 0:
                    self.set_state(PlayerState.ERROR)
                    self.logger.error(f"Stream process terminated unexpectedly with code {returncode}")
                break

            time.sleep(0.1)

        self._stop_stream()
        return True

    def _get_font_path(self) -> str:
        font_file = self.config["visuals"]["font_file"]
        system_fonts = [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
            "Arial"
        ]
        for font in system_fonts:
            if os.path.exists(font) or font == "Arial":
                return font
        return "Arial"

    def _generate_track_filters(self, track: Track) -> str:
        width = self.config["stream"]["width"]
        height = self.config["stream"]["height"]
        margin = self.config["visuals"]["margin"]
        font_file = self._get_font_path()
        transition_duration = self.config["visuals"]["transition_duration"]
        box_opacity = self.config["visuals"]["box_opacity"]
        box_border = self.config["visuals"]["box_border"]
        disclaimer_duration = self.config["player"]["disclaimer_duration"]

        def escape_text(text: str) -> str:
            return text.replace(":", "\\:").replace("'", "\\'").replace("[", "\\[").replace("]", "\\]")

        title = escape_text(track.title)
        artist = escape_text(track.artist)
        disclaimer = escape_text(self.config["visuals"]["disclaimer_text"])

        disclaimer_fade_in = f"if(lt(t,{transition_duration}),t/{transition_duration},1)"
        disclaimer_fade_out = f"if(gt(t,{disclaimer_duration}-{transition_duration}),({disclaimer_duration}-t)/{transition_duration},1)"

        main_fade_in = f"if(lt(t,{disclaimer_duration}+{transition_duration}),(t-{disclaimer_duration})/{transition_duration},1)"
        main_fade_out = f"if(gt(t,{track.duration}-{transition_duration}),({track.duration}-t)/{transition_duration},1)"

        filters = [
            f"[1:v]format=yuv420p[bg];"
            f"[bg]drawtext=text='{disclaimer}':"
            f"fontfile='{font_file}':"
            f"fontcolor=white:"
            f"fontsize={self.config['visuals']['font_size_info']}:"
            f"x=(w-text_w)/2:y=(h-text_h)/2:"
            f"box=1:boxcolor=black@0.8:boxborderw={box_border}:"
            f"alpha='{disclaimer_fade_in}*{disclaimer_fade_out}':"
            f"enable='between(t,0,{disclaimer_duration})'[dis];"

            f"[dis]drawtext=text='{title}':"
            f"fontfile='{font_file}':"
            f"fontcolor={self.config['visuals']['text_color']}:"
            f"fontsize={self.config['visuals']['font_size_title']}:"
            f"x={margin}:y=h-text_h-{margin}:"
            f"box=1:boxcolor=black@{box_opacity}:boxborderw={box_border}:"
            f"alpha='{main_fade_in}*{main_fade_out}':"
            f"enable='gt(t,{disclaimer_duration})'[title];"

            f"[title]drawtext=text='{artist}':"
            f"fontfile='{font_file}':"
            f"fontcolor={self.config['visuals']['highlight_color']}:"
            f"fontsize={self.config['visuals']['font_size_artist']}:"
            f"x={margin}:y=h-text_h-{margin}-{self.config['visuals']['font_size_title']}-30:"
            f"box=1:boxcolor=black@{box_opacity}:boxborderw={box_border}:"
            f"alpha='{main_fade_in}*{main_fade_out}':"
            f"enable='gt(t,{disclaimer_duration})'[artist];"

            f"[artist]drawtext=text='{self.config['visuals']['logo_text']}':"
            f"fontfile='{font_file}':"
            f"fontcolor={self.config['visuals']['highlight_color']}:"
            f"fontsize={self.config['visuals']['font_size_info']}:"
            f"x=w-text_w-{margin}:y=h-text_h-{margin}:"
            f"box=1:boxcolor=black@{box_opacity}:boxborderw={box_border}:"
            f"alpha='{main_fade_in}*{main_fade_out}':"
            f"enable='gt(t,{disclaimer_duration})'[logo];"

            f"[logo]drawtext=text='{self.config['visuals']['credits_text']}':"
            f"fontfile='{font_file}':"
            f"fontcolor=white:"
            f"fontsize={self.config['visuals']['font_size_info']}:"
            f"x={margin}:y={margin}:"
            f"box=1:boxcolor=black@{box_opacity}:boxborderw={box_border}:"
            f"alpha='{main_fade_in}*{main_fade_out}':"
            f"enable='gt(t,{disclaimer_duration})'[credits];"

            f"[credits]drawtext=text='{self.config['visuals']['live_indicator']}':"
            f"fontfile='{font_file}':"
            f"fontcolor=red:fontsize=36:"
            f"x=w-text_w-{margin}:y={margin}:"
            f"box=1:boxcolor=black@{box_opacity}:boxborderw={box_border}:"
            f"alpha='{main_fade_in}*{main_fade_out}':"
            f"enable='gt(t,{disclaimer_duration})'[v]"
        ]

        return ''.join(filters)

    def _buffer_loop(self) -> None:
        min_buffer = self.config["player"]["min_buffer_size"]
        refill_threshold = self.config["player"]["buffer_refill_threshold"]

        while self.running:
            current_size = self.track_queue.qsize()
            self.logger.debug(f"Buffer loop: queue size = {current_size}")

            if current_size < refill_threshold:
                for _ in range(refill_threshold - current_size):
                    track = self._find_next_track()
                    if track:
                        if self.enqueue_track(track):
                            self.logger.info(f"Buffered track: {track.title}")
                        else:
                            self.logger.warning(f"Failed to enqueue track: {track.title}")
                    else:
                        self.logger.warning("No valid track found for buffering")
                        time.sleep(1)

            time.sleep(BUFFER_CHECK_INTERVAL)

    def _find_next_track(self) -> Optional[Track]:
        max_attempts = 5  # Increased for more attempts

        for attempt in range(max_attempts):
            query = random.choice(self.config["queries"])
            track_urls = self.soundcloud.search_tracks(query)
            self.logger.debug(f"Attempt {attempt + 1}: Found {len(track_urls)} URLs for query '{query}'")

            if not track_urls:
                time.sleep(1)
                continue

            random.shuffle(track_urls)  # Shuffle to avoid repeating same tracks
            for url in track_urls:
                track_info = self.soundcloud.get_track_info(url)
                if not track_info:
                    continue

                if track_info.duration < self.config["player"]["min_duration"] or track_info.duration > self.config["player"]["max_duration"]:
                    self.logger.debug(f"Skipping track {track_info.title}: duration {track_info.duration}s out of bounds")
                    continue

                if self.track_manager.is_recent_track(track_info.id) or self.track_manager.is_recent_artist(track_info.artist) or self.track_manager.is_duplicate_title(track_info.title):
                    self.logger.debug(f"Skipping track {track_info.title}: recent track/artist or duplicate")
                    continue

                if track_info.id in self.queued_ids:
                    self.logger.debug(f"Skipping track {track_info.title}: already in queue")
                    continue

                downloaded_track = self.soundcloud.download_track(track_info)
                if downloaded_track and downloaded_track.ready:
                    self.logger.info(f"Successfully prepared track: {downloaded_track.title}")
                    return downloaded_track

            time.sleep(1)

        self.logger.warning("No valid track found after max attempts")
        return None

    def _stream_loop(self) -> None:
        while self.running:
            self.set_state(PlayerState.BUFFERING)
            self.logger.debug("Stream loop: Buffering state")

            try:
                track = self.track_queue.get(timeout=5)  # Increased timeout
                self.queued_ids.discard(track.id)
                self.logger.info(f"Dequeued track: {track.title} (ID: {track.id})")
            except queue.Empty:
                self.logger.warning("Queue empty, attempting to find new track")
                track = self._find_next_track()
                if not track:
                    self.empty_buffer_retries += 1
                    self.logger.error(f"Failed to find a valid track (retry {self.empty_buffer_retries}/{self.config['player']['max_empty_retries']})")
                    if self.empty_buffer_retries >= self.config["player"]["max_empty_retries"]:
                        self.logger.error("Buffer empty after max retries")
                        self.set_state(PlayerState.ERROR)
                        break
                    time.sleep(self.config["player"]["retry_delay"])
                    continue

                if not self.enqueue_track(track):
                    self.logger.error("Failed to enqueue track")
                    self.set_state(PlayerState.ERROR)
                    time.sleep(self.config["player"]["retry_delay"])
                    continue
                try:
                    track = self.track_queue.get(timeout=5)
                    self.queued_ids.discard(track.id)
                    self.logger.info(f"Dequeued newly enqueued track: {track.title}")
                except queue.Empty:
                    self.logger.error("Failed to retrieve enqueued track")
                    self.set_state(PlayerState.ERROR)
                    time.sleep(self.config["player"]["retry_delay"])
                    continue

            try:
                self._delete_previous_track()
                self.previous_track = self.current_track
                self.current_track = track
                self.track_manager.add_played_track(self.current_track)
                self.logger.info(f"Playing: {self.current_track.title} by {self.current_track.artist}")

                self.json_logger.log_current_track(self.current_track.to_dict())
                if self.previous_track:
                    self.json_logger.log_previous_track(self.previous_track.to_dict())
                self._update_status_log()

                success = self._stream_track(self.current_track)

                if not success:
                    self.set_state(PlayerState.ERROR)
                    self.logger.error(f"Failed to stream track: {self.current_track.title}")
                    time.sleep(self.config["player"]["retry_delay"])
                    continue

                self.empty_buffer_retries = 0
                self.previous_track = self.current_track
                self.current_track = None

                if self.previous_track:
                    self.json_logger.log_previous_track(self.previous_track.to_dict())
                self.json_logger.log_current_track({})
                self._update_status_log()

            except Exception as e:
                self.logger.error(f"Stream loop error: {e}")
                self.set_state(PlayerState.ERROR)
                time.sleep(self.config["player"]["retry_delay"])
                continue

class HsystRadioPlayer:
    def __init__(self):
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.logger = setup_logging(self.base_dir)
        self.json_logger = JsonLogger(self.base_dir)
        self.config = DEFAULT_CONFIG

        for section in self.config:
            if isinstance(self.config[section], dict):
                for key in self.config[section]:
                    if isinstance(self.config[section][key], str) and any(x in self.config[section][key] for x in ["data/", "cache/", "temp/"]):
                        self.config[section][key] = os.path.join(self.base_dir, self.config[section][key])
                        os.makedirs(self.config[section][key], exist_ok=True)

        self.track_manager = TrackManager(self.config, self.logger, self.json_logger)
        self.soundcloud = SoundCloudAPI(self.config, self.logger)
        self.streamer = AudioStreamer(self.config, self.track_manager, self.soundcloud, self.logger, self.json_logger)
        self.running = False

        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum, frame) -> None:
        self.logger.info(f"Signal {signum}, shutting down...")
        self.stop()
        sys.exit(0)

    def start(self) -> None:
        if self.running:
            return

        self.logger.info(f"Starting OPEN Radio v{VERSION}")
        self.running = True

        self.track_manager.cleanup_cache()
        self.streamer.start()

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def stop(self) -> None:
        if not self.running:
            return

        self.logger.info("Stopping...")
        self.running = False

        self.streamer.stop()
        self.track_manager.save_data()
        self.logger.info("Stopped")

def main():
    parser = argparse.ArgumentParser(description=f"OPEN RADIO v{VERSION}")
    parser.add_argument('--verbose', action='store_true', help="Verbose logging")

    args = parser.parse_args()

    if args.verbose:
        for handler in logging.getLogger("OPENRadio").handlers:
            if isinstance(handler, logging.StreamHandler):
                handler.setLevel(logging.DEBUG)

    player = HsystRadioPlayer()
    player.start()

if __name__ == "__main__":
    main()
