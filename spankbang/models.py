from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True)
class MediaTask:
    """A standardized instruction for the DownloadEngine to execute."""
    url: str
    filename: str
    file_size: Optional[int] = None  # In bytes, if known
