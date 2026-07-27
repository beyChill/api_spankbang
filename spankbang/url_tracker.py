import sqlite3
from datetime import datetime, timedelta

DB_NAME = "processed_url.sqlite3"


def init_db():
    """Creates the tracking table if it doesn't exist."""
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_urls (
                url TEXT PRIMARY KEY,
                date_processed TIMESTAMP,
                resolution
            )
        """)


def is_url_on_cooldown(url: str, cooldown_hours: int = 24) -> bool:
    """Checks if a URL is still within its cooldown window."""
    init_db()
    if not url:
        return False

    cooldown_cutoff = datetime.now() - timedelta(hours=cooldown_hours)

    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT date_processed FROM processed_urls WHERE url = ?", (url,))
        row = cursor.fetchone()

        if row:
            last_run = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f")
            return last_run > cooldown_cutoff
    return False


def mark_url_processed(url: str):
    """Updates the timestamp for a URL to 'now'."""
    init_db()
    if not url:
        return

    now = datetime.now()
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute(
            """
            INSERT INTO processed_urls (url, date_processed) 
            VALUES (?, ?) 
            ON CONFLICT(url) DO UPDATE SET date_processed = excluded.date_processed
        """,
            (url, now),
        )
        conn.commit()


def filter_and_lock_urls(urls: list[str], cooldown_hours: int = 24) -> list[str]:
    init_db()

    allowed_urls = []
    skipped_count = 0

    for url in urls:
        if not url:
            continue
        if is_url_on_cooldown(url, cooldown_hours):
            skipped_count += 1
        else:
            allowed_urls.append(url)

    if skipped_count > 0:
        print(f"⏳ Skipped {skipped_count} URLs because they were processed within the last {cooldown_hours} hours.")

    return allowed_urls
