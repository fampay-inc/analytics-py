from dataclasses import dataclass
from typing import Callable, Optional


@dataclass
class MixpanelConfig:
    project_id: str
    project_token: str
    service_account_username: str
    service_account_secret: str

    host_url: Optional[str] = None
    error_callback: Optional[Callable] = None
    start_consumer: bool = True
    enable_debug: bool = False
