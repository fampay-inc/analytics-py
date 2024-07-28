from dataclasses import dataclass

from fam_analytics_py.base import BaseConfig


@dataclass
class CleverTapConfig(BaseConfig):
    account_id: str
    passcode: str
