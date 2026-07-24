import ast
import asyncio
import re
from datetime import datetime
from pathlib import Path

from tqdm import tqdm
from wreq import Client, Emulation, Platform, Profile

from url_tracker import filter_and_lock_urls


class TooSmallError(Exception):
    """Raised when a value is smaller than the allowed minimum."""

    pass


urls = [
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
]

MIN_RESOLUTION: int = 720
BASE_URL = "https://spankbang.party/"
STREAM_DATA_PATTERN = re.compile(r"var\s+stream_data\s*=\s*(\{.*?\});")
VIDEO_DATE_PATTERN = re.compile(r'<time[^>]*datetime="([^"]+)"')
DL_HEADERS = {
    "Accept": "video/webm,video/ogg,video/*;q=0.9,application/ogg;q=0.7,audio/*;q=0.6,*/*;q=0.5",
    "Accept-Language": "en-US,en;q=0.9",
    "Range": "bytes=0-",
    "Sec-Fetch-Storage-Access": "none",
    "Sec-GPC": "1",
    "Connection": "keep-alive",
    "Referer": BASE_URL,
    "Sec-Fetch-Dest": "video",
    "Sec-Fetch-Mode": "no-cors",
    "Sec-Fetch-Site": "cross-site",
    "Priority": "u=4",
}


async def queue_manager(worker_id: int, queue: asyncio.Queue, client: Client, headers: dict, results: dict):

    while not queue.empty():
        url = await queue.get()
        raw_name = url.split("/")[-1]
        name_ = raw_name.replace("+", "_")
        print(f"[wkr {worker_id}] Fetching: {url}")

        try:
            resp = await client.get(url, headers=headers)

            if resp.status.is_success():
                html = await resp.text()

                stream_match = STREAM_DATA_PATTERN.search(html)
                result_dict = ast.literal_eval(stream_match.group(1)) if stream_match else None
                stream_url = next(reversed(result_dict.values()))[0] if result_dict else None
                if stream_url:
                    first_half = stream_url.split(".mp4?")[0]
                    end = first_half.rfind("-")
                    resolution = first_half[end + 1 :]
                    resolution = resolution[:-1]
                    if int(resolution) < MIN_RESOLUTION:
                        raise TooSmallError(f"{name_}:{resolution}p is too small; min is {MIN_RESOLUTION}p")

                date_match = VIDEO_DATE_PATTERN.search(html)
                video_date = date_match.group(1)[:10] if date_match else None

                results[name_] = {
                    "url": stream_url,
                    "date": video_date,
                }
            else:
                results[name_] = None
                print(f"[Warning: {worker_id}]  HTTP {resp.status} for {url}")
        except TooSmallError as e:
            results[name_] = None
            print(e)

        except Exception as e:
            results[name_] = None
            print(f"[Error {worker_id}]  processing {url}: {e}")

        finally:
            queue.task_done()

        await asyncio.sleep(2)


async def scrape_pipeline(urls_list: list) -> dict:

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Range": "bytes=0-",
        "Sec-Fetch-Storage-Access": "none",
        "Sec-GPC": "1",
        "Connection": "keep-alive",
        "Referer": BASE_URL,
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigation",
        "Sec-Fetch-Site": "same-origin",
        "Priority": "u=0,i",
    }

    client = Client(emulation=Emulation(profile=Profile.Chrome140, platform=Platform.MacOS, headers=True))

    queue = asyncio.Queue()
    for url in urls_list:
        if url:
            await queue.put(url)

    results = {}

    num_workers = min(3, len(urls_list))

    if num_workers == 0:
        return results

    print(f"Processing {len(urls_list)} URLs using {num_workers} parallel workers...")

    workers = [asyncio.create_task(queue_manager(i, queue, client, headers, results)) for i in range(num_workers)]

    await queue.join()

    for worker in workers:
        worker.cancel()

    return results


def generate_filename(name, date):
    ext = "mp4"
    timestamp = datetime.now().strftime("%H%M%S")
    return f"{name} [spankbang] ({date}) {timestamp}.{ext}"


async def download_many(video_data, concurrency=4):
    client = Client(emulation=Emulation.random())
    sem = asyncio.Semaphore(concurrency)

    async def queue_manager(name_, url, date):
        filename = generate_filename(name_, date)
        async with sem:
            resp = await client.get(url, headers=DL_HEADERS)

            total = None
            try:
                video_length = resp.content_length
                if video_length is not None:
                    total = int(video_length)
            except Exception:
                pass

            out_path = Path(f"./videos/{filename}")
            out_path.parent.mkdir(parents=True, exist_ok=True)

            with (
                open(out_path, "wb") as f,
                tqdm(
                    desc=out_path.name,
                    total=total,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    miniters=1,
                    leave=True,
                ) as bar,
            ):
                async with resp.stream() as streamer:
                    async for chunk in streamer:
                        if not chunk:
                            continue
                        f.write(chunk)
                        bar.update(len(chunk))

    await asyncio.gather(*(queue_manager(name, url, date) for name, url, date in video_data))


async def process_sb(urls):
    # Run the async pipeline loop
    scraped_data = await scrape_pipeline(urls)

    video_data = [(name, inner["url"], inner["date"]) for name, inner in scraped_data.items() if inner]

    await download_many(video_data)


if __name__ == "__main__":
    ready_urls = filter_and_lock_urls(urls, cooldown_hours=999999)

    if ready_urls:
        asyncio.run(process_sb(ready_urls))
    else:
        print("❌ Zero URLs to process.")
