from typing import Callable, Optional
from fam_analytics_py import globals
from fam_analytics_py.clevertap import CleverTapConfig
from fam_analytics_py.mixpanel import MixpanelConfig
from fam_analytics_py.segment import SegmentConfig


__all__ = (
    "alias",
    "flush",
    "group",
    "identify",
    "initialize",
    "join",
    "page",
    "screen",
    "track",
    "CleverTapConfig",
    "MixpanelConfig",
    "SegmentConfig",
)


def initialize(
    clevertap_config: Optional[CleverTapConfig] = None,
    mixpanel_config: Optional[MixpanelConfig] = None,
    segment_config: Optional[SegmentConfig] = None,
    is_clevertap_enabled: Optional[Callable[[], bool]] = None,
    is_mixpanel_enabled: Optional[Callable[[], bool]] = None,
    is_segment_enabled: Optional[Callable[[], bool]] = None,
):
    globals.set_clevertap_config(clevertap_config)
    globals.set_mixpanel_config(mixpanel_config)
    globals.set_segment_config(segment_config)

    globals.is_clevertap_enabled = is_clevertap_enabled or (
        lambda: clevertap_config is not None
    )
    globals.is_mixpanel_enabled = is_mixpanel_enabled or (
        lambda: mixpanel_config is not None
    )
    globals.is_segment_enabled = is_segment_enabled or (
        lambda: segment_config is not None
    )

    globals.is_initialized = True


def track(*args, **kwargs):
    """Send a track call."""
    _proxy("track", *args, **kwargs)


def identify(*args, **kwargs):
    """Send a identify call."""
    _proxy("identify", *args, **kwargs)


def group(*args, **kwargs):
    """Send a group call."""
    _proxy("group", *args, **kwargs)


def alias(*args, **kwargs):
    """Send a alias call."""
    _proxy("alias", *args, **kwargs)


def page(*args, **kwargs):
    """Send a page call."""
    _proxy("page", *args, **kwargs)


def screen(*args, **kwargs):
    """Send a screen call."""
    _proxy("screen", *args, **kwargs)


def flush():
    """Tell the client to flush."""
    _proxy("flush")


def join():
    """Block program until the client clears the queue"""
    _proxy("join")


def _proxy(method, *args, **kwargs):
    """Create an analytics client if one doesn't exist and send to it."""

    globals.raise_if_not_initialized()

    for is_client_enabled, get_client in [
        (globals.is_clevertap_enabled, globals.get_clevertap_client),
        (globals.is_mixpanel_enabled, globals.get_mixpanel_client),
        (globals.is_segment_enabled, globals.get_segment_client),
    ]:
        if is_client_enabled():
            client = get_client()
            fn = getattr(client, method)
            fn(*args, **kwargs)
