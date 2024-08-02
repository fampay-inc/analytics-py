from typing import Callable, Optional

from fam_analytics_py.clevertap import CleverTapClient, CleverTapConfig
from fam_analytics_py.mixpanel import MixpanelClient, MixpanelConfig
from fam_analytics_py.segment import SegmentClient, SegmentConfig


_clevertap_config: Optional[CleverTapConfig] = None
_mixpanel_config: Optional[MixpanelConfig] = None
_segment_config: Optional[SegmentConfig] = None

_clevertap_client = None
_mixpanel_client = None
_segment_client = None

is_clevertap_enabled: Callable[[], bool]
is_mixpanel_enabled: Callable[[], bool]
is_segment_enabled: Callable[[], bool]

is_initialized: bool = False


def _raise_if_config_not_set(
    config: "CleverTapConfig | MixpanelConfig | SegmentConfig | None",
):
    if not config:
        class_name = config.__class__.__name__
        raise Exception(f"Client requested before setting {class_name} config")


def raise_if_not_initialized():
    global is_initialized
    if not is_initialized:
        raise Exception("Module not initialized")


def set_clevertap_config(config: CleverTapConfig):
    global _clevertap_config
    _clevertap_config = config


def set_mixpanel_config(config: MixpanelConfig):
    global _mixpanel_config
    _mixpanel_config = config


def set_segment_config(config: SegmentConfig):
    global _segment_config
    _segment_config = config


def get_clevertap_client() -> CleverTapClient:
    global _clevertap_client, _clevertap_config
    _raise_if_config_not_set(config=_clevertap_config)

    if not _clevertap_client:
        _clevertap_client = CleverTapClient(
            credentials={
                "clevertap_account_id": _clevertap_config.account_id,
                "clevertap_passcode": _clevertap_config.passcode,
            },
            host=_clevertap_config.host_url,
            debug=_clevertap_config.enable_debug,
            on_error=_clevertap_config.error_callback,
            send=_clevertap_config.start_consumer,
        )

    return _clevertap_client


def get_mixpanel_client() -> MixpanelClient:
    global _mixpanel_client, _mixpanel_config
    _raise_if_config_not_set(config=_mixpanel_config)

    if not _mixpanel_client:
        _mixpanel_client = MixpanelClient(
            config=_mixpanel_config,
        )

    return _mixpanel_client


def get_segment_client() -> SegmentClient:
    global _segment_client, _segment_config
    _raise_if_config_not_set(config=_segment_config)

    if not _segment_client:
        _segment_client = SegmentClient(
            credentials=_segment_config.write_key,
            host=_segment_config.host_url,
            debug=_segment_config.enable_debug,
            on_error=_segment_config.error_callback,
            send=_segment_config.start_consumer,
        )

    return _segment_client
