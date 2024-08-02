from dataclasses import dataclass
from typing import Callable, Optional


@dataclass
class CleverTapConfig:
    account_id: str
    passcode: str

    host_url: Optional[str] = None
    error_callback: Optional[Callable] = None
    start_consumer: bool = True
    enable_debug: bool = False
