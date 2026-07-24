import sqlite3
from datetime import datetime, timedelta

DB_NAME = "scraper_history.db"

def init_db():
    """Creates the tracking table if it doesn't exist."""
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_urls (
                url TEXT PRIMARY KEY,
                last_processed TIMESTAMP
            )
        """)

def filter_and_lock_urls(urls: list[str], cooldown_hours: int = 24) -> list[str]:
    """
    Checks the database, filters out any URLs still in their cooldown window,
    and updates the timestamp for the allowed URLs so they are immediately locked.
    """
    init_db()  # Ensure DB exists
    
    allowed_urls = []
    skipped_count = 0
    now = datetime.now()
    cooldown_cutoff = now - timedelta(hours=cooldown_hours)

    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        
        for url in urls:
            # Check last processed time
            if not url:
                continue
            
            cursor.execute("SELECT last_processed FROM processed_urls WHERE url = ?", (url,))
            row = cursor.fetchone()
            
            if row:
                # Convert the stored string timestamp back to a datetime object
                last_run = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f")
                if last_run > cooldown_cutoff:
                    skipped_count += 1
                    continue  # Skip this URL, it's still locked
            
            # If allowed, add to execution list and update/insert timestamp
            allowed_urls.append(url)
            cursor.execute("""
                INSERT INTO processed_urls (url, last_processed) 
                VALUES (?, ?) 
                ON CONFLICT(url) DO UPDATE SET last_processed = excluded.last_processed
            """, (url, now))
            
        conn.commit()
        
    if skipped_count > 0:
        print(f"⏳ Skipped {skipped_count} URLs because they were processed within the last {cooldown_hours} hours.")
        
    return allowed_urls
