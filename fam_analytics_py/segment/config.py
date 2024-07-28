from dataclasses import dataclass

from fam_analytics_py.base import BaseConfig


@dataclass
class SegmentConfig(BaseConfig):
    write_key: str
