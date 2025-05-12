#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Open Radio Player
-------------------------
Version 1.0.0

A customizable automated radio streaming solution using SoundCloud content.
"""

import os
import json
import random
import logging
import argparse
import subprocess
import requests
import time
from bs4 import BeautifulSoup
from collections import deque

# =============================================
# CONFIGURATION SECTION - EDIT THESE SETTINGS
# =============================================

class Config:
    # Stream Settings
    STREAM = {
        "width": 1280,           # Stream resolution width
        "height": 720,           # Stream resolution height
        "fps": 30,               # Frames per second
        "warning_duration": 5,   # Legal warning duration (seconds)
    }
    
    # FFmpeg Settings
    FFMPEG = {
        "preset": "ultrafast",   # Encoding preset (ultrafast, superfast, veryfast, faster, fast, medium, slow, slower, veryslow)
        "threads": 1,            # Number of encoding threads
        "crf": 28,               # Constant Rate Factor (0-51, lower is better quality)
        "audio_bitrate": "128k", # Audio bitrate
    }
    
    # Directory Settings
    PATHS = {
        "temp_dir": "temp",      # Temporary files directory
        "cache_dir": "cache",    # Cached tracks directory
        "log_file": "radio.log", # Log file path
        "history_file": "played.json", # Play history file
    }
    
    # Content Settings
    CONTENT = {
        "max_history": 20,       # How many tracks to remember before repeating
        "min_duration": 120,     # Minimum track duration (seconds)
        "max_duration": 600,     # Maximum track duration (seconds)
        "margin": 20,            # Screen margin for text (pixels)
    }
    
    # SoundCloud Settings
    SOUNDCLOUD = {
        "search_url": "https://soundcloud.com/search/sounds?q=",
        "initial_results": 5,    # Initial search results to fetch
        "max_results": 50,       # Maximum results when expanding search
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    }
    
    # Text & Display Settings
    DISPLAY = {
        "commercial_texts": [    # Random texts to display during playback
            "Made by op3n/op3ny",
            "OpenRadio - Viva ao OpenSource!",
            "Acesse o meu GitHub! op3ny",
            "OpenRadio // #Radio"
        ],
        "background_colors": [   # Random background colors
            "0x1E90FF", "0x32CD32", "0xFF4500", "0x9932CC",
            "0xFF1493", "0x00CED1", "0xFFD700"
        ],
        "legal_warning": """      # Legal warning text (supports \n for new lines)
AVISO IMPORTANTE\n\n
Todo conteúdo é protegido por direitos autorais.\n
Esta transmissão tem fins educacionais e não comerciais.\n
Os créditos são exibidos para reconhecimento dos autores.\n\n
OPEN RADIO respeita os direitos dos artistas.
"""
    }

# =============================================
# END OF CONFIGURATION - CODE BELOW
# =============================================

# Setup directories
os.makedirs(Config.PATHS["temp_dir"], exist_ok=True)
os.makedirs(Config.PATHS["cache_dir"], exist_ok=True)

# Configure logging
logging.basicConfig(
    filename=Config.PATHS["log_file"],
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("OpenRadio")

class SoundCloudScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": Config.SOUNDCLOUD["user_agent"]})
    
    def search_tracks(self, query, limit=Config.SOUNDCLOUD["initial_results"]):
        """Search for tracks on SoundCloud"""
        logger.info(f"Searching SoundCloud for: {query} (limit: {limit})")
        
        try:
            url = Config.SOUNDCLOUD["search_url"] + query.replace(' ', '+')
            res = self.session.get(url)
            res.raise_for_status()
            
            soup = BeautifulSoup(res.text, 'html.parser')
            links = set()

            for a in soup.find_all('a', href=True):
                href = a['href']
                if href.count('/') == 2 and not href.startswith(('/you/', '/search', '/discover', '/stream', '/library')):
                    full_url = f"https://soundcloud.com{href}"
                    links.add(full_url)
                    if len(links) >= limit:
                        break

            return list(links)[:limit]
        
        except Exception as e:
            logger.error(f"SoundCloud search error: {str(e)}")
            return []

class OpenRadio:
    def __init__(self, rtmp_url):
        self.rtmp_url = rtmp_url
        self.scraper = SoundCloudScraper()
        self.played_history = deque(maxlen=Config.CONTENT["max_history"])
        self.load_history()
        self.current_bg_color = ""
        self.current_commercial_text = ""
        self.current_search_limit = Config.SOUNDCLOUD["initial_results"]
    
    def load_history(self):
        """Load playback history from file"""
        try:
            with open(Config.PATHS["history_file"], "r") as f:
                history = json.load(f)
                self.played_history = deque(history, maxlen=Config.CONTENT["max_history"])
        except (FileNotFoundError, json.JSONDecodeError):
            self.played_history = deque(maxlen=Config.CONTENT["max_history"])
    
    def save_history(self):
        """Save playback history to file"""
        with open(Config.PATHS["history_file"], "w") as f:
            json.dump(list(self.played_history), f, indent=2)
    
    def track_in_history(self, track_info):
        """Check if track is in playback history"""
        for entry in self.played_history:
            if isinstance(entry, dict):
                if entry.get('artist') == track_info['artist'] and entry.get('title') == track_info['title']:
                    return True
        return False
    
    def show_legal_warning(self):
        """Display legal warning screen"""
        cmd = [
            'ffmpeg', '-y',
            '-f', 'lavfi', '-i', f'color=c=black:s={Config.STREAM["width"]}x{Config.STREAM["height"]}:d={Config.STREAM["warning_duration"]}',
            '-f', 'lavfi', '-i', 'anullsrc=channel_layout=stereo:sample_rate=44100',
            '-vf', f"drawtext=text='{Config.DISPLAY['legal_warning']}':fontsize=24:fontcolor=white:x=(w-text_w)/2:y=(h-text_h)/2:box=1:boxcolor=black@0.7:boxborderw=10",
            '-c:v', 'libx264', '-preset', Config.FFMPEG["preset"],
            '-t', str(Config.STREAM["warning_duration"]),
            '-c:a', 'aac', '-b:a', Config.FFMPEG["audio_bitrate"],
            '-f', 'flv',
            self.rtmp_url
        ]
        
        process = subprocess.Popen(cmd)
        time.sleep(Config.STREAM["warning_duration"])
        process.terminate()
        process.wait()
    
    def get_track_info(self, track_url):
        """Get track metadata using yt-dlp"""
        try:
            cmd = [
                'yt-dlp',
                '--dump-json',
                '--no-playlist',
                track_url
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            info = json.loads(result.stdout)
            
            return {
                'id': info['id'],
                'title': info['title'],
                'artist': info['uploader'],
                'duration': info['duration'],
                'url': track_url
            }
        except Exception as e:
            logger.error(f"Error getting track info from {track_url}: {str(e)}")
            return None
    
    def download_track(self, track_info):
        """Download track using yt-dlp"""
        track_path = os.path.join(Config.PATHS["cache_dir"], f"{track_info['id']}.mp3")
        
        if os.path.exists(track_path):
            return track_path
        
        try:
            cmd = [
                'yt-dlp',
                '-x',  # Extract audio only
                '--audio-format', 'mp3',
                '--audio-quality', Config.FFMPEG["audio_bitrate"],
                '--no-playlist',
                '-o', track_path,
                track_info['url']
            ]
            subprocess.run(cmd, check=True)
            return track_path
        except Exception as e:
            logger.error(f"Download error: {str(e)}")
            return None
    
    def create_overlay_filters(self, track_info):
        """Create FFmpeg overlay filters for the stream"""
        m = Config.CONTENT["margin"]
        self.current_bg_color = random.choice(Config.DISPLAY["background_colors"])
        self.current_commercial_text = random.choice(Config.DISPLAY["commercial_texts"])
        
        main_filter = (
            f"color=c={self.current_bg_color}:s={Config.STREAM['width']}x{Config.STREAM['height']}:r={Config.STREAM['fps']},"
            f"format=yuv420p[bg];"
        )
        
        overlays = (
            f"[bg]drawtext=text='{track_info['artist']} - {track_info['title']}':"
            f"fontcolor=white:fontsize=28:x={m}:y=h-text_h-{m}:"
            f"box=1:boxcolor=black@0.5:boxborderw=5[track];"
            
            f"[track]drawtext=text='{self.current_commercial_text}':"
            f"fontcolor=white:fontsize=48:x=(w-text_w)/2:y=(h-text_h)/2:"
            f"box=1:boxcolor=black@0.5:boxborderw=10[commercial];"
            
            f"[commercial]drawtext=text='OPEN RADIO':"
            f"fontcolor=0x0099FF:fontsize=24:x=w-text_w-{m}:y=h-text_h-{m}:"
            f"box=1:boxcolor=black@0.5:boxborderw=5[hsyst];"
            
            f"[hsyst]drawtext=text='#Radio -- Made by op3n':"
            f"fontcolor=white:fontsize=24:x={m}:y={m}:"
            f"box=1:boxcolor=black@0.5:boxborderw=5[madeby];"
            
            f"[madeby]drawtext=text='• AO VIVO •':"
            f"fontcolor=red:fontsize=20:x=w-text_w-{m}:y={m}:"
            f"box=1:boxcolor=black@0.5:boxborderw=5"
        )
        
        return main_filter + overlays
    
    def stream_track(self, track_info):
        """Stream a single track with visual overlay"""
        self.show_legal_warning()
        
        track_path = self.download_track(track_info)
        if not track_path:
            return False
        
        overlay_filters = self.create_overlay_filters(track_info)
        
        cmd = [
            'ffmpeg', '-y',
            '-re',
            '-f', 'lavfi', '-i', f'color=c=black:s={Config.STREAM["width"]}x{Config.STREAM["height"]}:r={Config.STREAM["fps"]}',
            '-re',
            '-i', track_path,
            '-filter_complex', overlay_filters,
            '-c:v', 'libx264', '-preset', Config.FFMPEG["preset"],
            '-crf', str(Config.FFMPEG["crf"]),
            '-threads', str(Config.FFMPEG["threads"]),
            '-c:a', 'aac', '-b:a', Config.FFMPEG["audio_bitrate"],
            '-f', 'flv',
            self.rtmp_url
        ]
        
        process = subprocess.Popen(cmd)
        
        try:
            time.sleep(track_info['duration'])
            process.terminate()
            process.wait()
        except:
            process.terminate()
            process.wait()
            raise
        
        try:
            os.remove(track_path)
        except:
            pass
        
        return True
    
    def run(self, query):
        """Main streaming loop"""
        while True:
            track_urls = self.scraper.search_tracks(query, self.current_search_limit)
            
            if not track_urls:
                # Expand search if no results found
                self.current_search_limit = min(
                    self.current_search_limit * 2,
                    Config.SOUNDCLOUD["max_results"]
                )
                
                if self.current_search_limit >= Config.SOUNDCLOUD["max_results"]:
                    logger.info("Max search results reached. Resetting history.")
                    self.played_history.clear()
                    self.current_search_limit = Config.SOUNDCLOUD["initial_results"]
                    self.save_history()
                
                logger.error(f"No tracks found. Increasing search limit to {self.current_search_limit}")
                time.sleep(10)
                continue
            
            # Reset search limit after successful search
            self.current_search_limit = Config.SOUNDCLOUD["initial_results"]
            
            for track_url in track_urls:
                track_info = self.get_track_info(track_url)
                if not track_info:
                    continue
                
                # Check duration limits
                if not (Config.CONTENT["min_duration"] <= track_info['duration'] <= Config.CONTENT["max_duration"]):
                    continue
                
                # Check if track was recently played
                if self.track_in_history(track_info):
                    continue
                
                logger.info(f"Playing: {track_info['artist']} - {track_info['title']}")
                
                try:
                    if self.stream_track(track_info):
                        # Add to playback history
                        self.played_history.append({
                            'url': track_url,
                            'artist': track_info['artist'],
                            'title': track_info['title'],
                            'time': time.time()
                        })
                        self.save_history()
                except KeyboardInterrupt:
                    logger.info("Stream stopped by user")
                    return
                except Exception as e:
                    logger.error(f"Stream error: {str(e)}")
                    continue

def main():
    parser = argparse.ArgumentParser(description="Open Radio - Automated Streaming Solution")
    parser.add_argument('--rtmp', required=True, help="RTMP server URL (rtmp://server/app/stream_key)")
    parser.add_argument('--query', required=True, help="Search query for SoundCloud tracks")
    
    args = parser.parse_args()
    
    print("\n" + "="*50)
    print(" "*15 + "OPEN RADIO PLAYER")
    print("="*50 + "\n")
    
    radio = OpenRadio(args.rtmp)
    
    try:
        radio.run(args.query)
    except KeyboardInterrupt:
        print("\nStopping stream...")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
