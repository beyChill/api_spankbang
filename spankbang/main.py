import ast
import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Final, Optional, Sequence

import aiofiles
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)
from wreq import Client, Emulation, Platform, Profile

from url_tracker import filter_and_lock_urls

# --- Configuration ---
MIN_RESOLUTION: Final[int] = 720
BASE_URL: Final[str] = "https://spankbang.party/"

# Regex Patterns
STREAM_DATA_PATTERN: Final[re.Pattern] = re.compile(r"var\s+stream_data\s*=\s*(\{.*?\});")
VIDEO_DATE_PATTERN: Final[re.Pattern] = re.compile(r'<time[^>]*datetime="([^"]+)"')
RESOLUTION_PATTERN: Final[re.Pattern] = re.compile(r"(\d+)p\.mp4")

# Headers
SCRAPE_HEADERS: Final[dict] = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": BASE_URL,
    "Range": "bytes=0-",
    "Sec-Fetch-Storage-Access": "none",
    "Sec-GPC": "1",
    "Connection": "keep-alive",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigation",
    "Sec-Fetch-Site": "same-origin",
    "Priority": "u=0,i",
}

DL_HEADERS: Final[dict] = {
    "Accept": "video/webm,video/ogg,video/*;q=0.9,application/ogg;q=0.7,audio/*;q=0.6,*/*;q=0.5",
    "Accept-Language": "en-US,en;q=0.9",
    "Range": "bytes=0-",
    "Referer": BASE_URL,
    "Sec-Fetch-Dest": "video",
}


@dataclass(frozen=True)
class VideoMetadata:
    """Immutable record of video information."""

    source_url: str
    video_id: str
    slug: str
    display_name: str
    stream_url: Optional[str] = None
    date: str = "unknown_date"
    resolution: int = 0

    @property
    def filename(self) -> str:
        timestamp = datetime.now().strftime("%H%M%S")
        return f"{self.display_name} [spankbang] ({self.date}) {timestamp}.mp4"


class SpankBangApp:
    """The master orchestrator for the SpankBang scraping and downloading process."""

    def __init__(self, concurrency: int = 3):
        self.console = Console()
        self._setup_logging()
        self.log = logging.getLogger("spankbang")
        self.concurrency = concurrency
        self.video_dir = Path("./videos")

    def _setup_logging(self) -> None:
        logging.basicConfig(
            level="INFO",
            format="%(message)s",
            datefmt="[%X]",
            handlers=[RichHandler(rich_tracebacks=True, console=self.console)],
        )

    def sanitize_filename(self, text: str) -> str:
        """Removes filesystem-unsafe characters, collapses multiple separators, and strips edges."""
        sanitized = re.sub(r"[^a-zA-Z0-9\-_]+", "_", text)
        return sanitized.strip("_")

    async def _fetch_metadata(self, client: Client, url: str) -> Optional[VideoMetadata]:
        """Fetches and parses metadata for a single video URL."""
        if not url:
            return None

        # Basic ID/Slug extraction
        parts = url.rstrip("/").split("/")
        if len(parts) < 3:
            return None

        video_id, slug = parts[-3], parts[-1]
        name = self.sanitize_filename(slug)

        try:
            resp = await client.get(url, headers=SCRAPE_HEADERS)
            if not resp.status.is_success():
                self.log.error(f"Failed to fetch {url}: HTTP {resp.status}")
                return None

            html = await resp.text()

            # Extract stream dictionary
            stream_match = STREAM_DATA_PATTERN.search(html)
            stream_url = None
            resolution = 0
            if stream_match:
                # Use ast.literal_eval for JS object literal parsing as per constraints
                data = ast.literal_eval(stream_match.group(1))
                if data:
                    stream_url = next(reversed(data.values()))[0]
                    if stream_url:
                        res_match = RESOLUTION_PATTERN.search(stream_url)
                        resolution = int(res_match.group(1)) if res_match else 0

            # Filter resolution early
            if resolution < MIN_RESOLUTION:
                self.log.info(f"[yellow]Skipping {name}: {resolution}p < {MIN_RESOLUTION}p[/]")
                return None

            date_match = VIDEO_DATE_PATTERN.search(html)
            date = date_match.group(1)[:10] if date_match else "unknown"

            return VideoMetadata(
                source_url=url,
                video_id=video_id,
                slug=slug,
                display_name=name,
                stream_url=stream_url,
                date=date,
                resolution=resolution,
            )

        except Exception as e:
            self.log.error(f"Metadata error for {url}: {e}")
            return None

    async def _download_worker(self, client: Client, meta: VideoMetadata, progress: Progress, total_task_id: TaskID, semaphore: asyncio.Semaphore) -> None:
        """Handles the actual file transfer for one video."""
        if not meta.stream_url:
            return

        out_path = self.video_dir / meta.filename
        self.video_dir.mkdir(parents=True, exist_ok=True)

        async with semaphore:
            task_id = progress.add_task(f"[cyan]{meta.display_name[:25]}...", total=None)
            try:
                resp = await client.get(meta.stream_url, headers=DL_HEADERS)
                if not resp.status.is_success():
                    self.log.error(f"Download failed: {meta.display_name} (HTTP {resp.status})")
                    return

                content_length = resp.content_length
                total_size = int(content_length) if content_length is not None else None
                progress.update(task_id, total=total_size)

                async with aiofiles.open(out_path, "wb") as f:
                    async with resp.stream() as streamer:
                        async for chunk in streamer:
                            if chunk:
                                await f.write(chunk)
                                progress.update(task_id, advance=len(chunk))

                self.log.info(f"[green]✓ Completed:[/] {meta.display_name}")

            except Exception as e:
                self.log.error(f"Transfer error for {meta.display_name}: {e}")
            finally:
                progress.update(total_task_id, advance=1)
                progress.remove_task(task_id)

    async def run(self, urls: Sequence[str]) -> None:
        """Main entry point for the application logic."""
        if not urls:
            self.log.info("No URLs provided.")
            return

        # Phase 1: Scrape
        self.log.info(f"Scraping metadata for {len(urls)} videos...")
        async with Client(emulation=Emulation(profile=Profile.Chrome140, platform=Platform.MacOS, headers=True)) as client:
            meta_tasks = [self._fetch_metadata(client, url) for url in urls]
            results = await asyncio.gather(*meta_tasks)

        valid_videos = [m for m in results if m and m.stream_url]

        if not valid_videos:
            self.log.warning("No downloadable videos.")
            return

        # Phase 2: Download
        self.log.info(f"Downloading {len(valid_videos)} videos (concurrency={self.concurrency})...")

        progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
            console=self.console,
            expand=True,
        )

        semaphore = asyncio.Semaphore(self.concurrency)

        with progress:
            total_task_id = progress.add_task("[yellow]Batch Progress", total=len(valid_videos))
            async with Client(emulation=Emulation.random()) as dl_client:
                dl_tasks = [self._download_worker(dl_client, meta, progress, total_task_id, semaphore) for meta in valid_videos]
                await asyncio.gather(*dl_tasks)


if __name__ == "__main__":
    # Input URLs
    raw_urls = [
        "https://spankbang.party/646iw/video/cherycheryl",
        "https://spankbang.party/74m8n/video/huge+natural+tits+ashlyn+peaks+masturbates+with+huge+dildo",
        "https://spankbang.party/7u9ea/video/close+up+masturbation+by+raven+with+huge+natural+tits",
    ]

    # Pre-filter using url_tracker (assuming this handles locking/cooldowns)
    ready_urls = filter_and_lock_urls(raw_urls, cooldown_hours=999999)

    app = SpankBangApp(concurrency=3)
    try:
        asyncio.run(app.run(ready_urls))
    except KeyboardInterrupt:
        app.log.info("[bold red]Process aborted by user.[/]")
