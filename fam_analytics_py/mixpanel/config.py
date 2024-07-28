from dataclasses import dataclass

from fam_analytics_py.base import BaseConfig


@dataclass
class MixpanelConfig(BaseConfig):
    project_id: str
    project_token: str
    service_account_username: str
    service_account_secret: str
