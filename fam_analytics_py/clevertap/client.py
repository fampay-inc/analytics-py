from datetime import datetime

from dateutil.tz import tzutc
from six import string_types

from fam_analytics_py.base import BaseClient
from fam_analytics_py.types import ID_TYPES
from fam_analytics_py.utils import (
    clean,
    guess_timezone,
    remove_trailing_slash,
    require,
    stringify_id,
)

from .consumer import CleverTapConsumer


class CleverTapClient(BaseClient):
    DEFAULT_HOST = "https://in1.api.clevertap.com"

    def __init__(
        self,
        write_key=None,
        credentials=None,
        host=None,
        debug=False,
        max_queue_size=10000,
        send=True,
        on_error=None,
    ):
        require("credentials", credentials, dict)

        super(CleverTapClient, self).__init__(
            write_key=write_key,
            credentials=credentials,
            host=host,
            debug=debug,
            max_queue_size=max_queue_size,
            send=send,
            on_error=on_error,
        )

    @property
    def upload_size(self):
        return 100

    def _get_consumer(self):
        return CleverTapConsumer(
            self.queue,
            write_key=self.write_key,
            upload_size=self.upload_size,
            url=self._get_url(),
            auth=self._get_auth(),
            headers=self._get_headers(),
            on_error=self.on_error,
        )

    def _get_url(self):
        return remove_trailing_slash(self.host or self.DEFAULT_HOST) + "/1/upload"

    def _get_auth(self):
        return None

    def _get_headers(self):
        return {
            "X-CleverTap-Account-Id": self.credentials["clevertap_account_id"],
            "X-CleverTap-Passcode": self.credentials["clevertap_passcode"],
        }

    def track(
        self,
        user_id=None,
        event=None,
        properties=None,
        context=None,
        timestamp=None,
        anonymous_id=None,
        integrations=None,
    ):
        properties = properties or {}
        require("user_id / anonymous_id", user_id or anonymous_id, ID_TYPES)
        require("properties", properties, dict)
        require("event", event, string_types)

        msg = {
            "type": "event",
            "evtName": event,
            "evtData": properties,
            "ts": timestamp,
            "identity": user_id,
        }

        if anonymous_id:
            msg.update({"objectId": anonymous_id})

        return self._enqueue(msg)

    def identify(
        self,
        user_id=None,
        traits=None,
        context=None,
        timestamp=None,
        anonymous_id=None,
        integrations=None,
    ):
        traits = traits or {}
        require("user_id / anonymous_id", user_id or anonymous_id, ID_TYPES)
        require("traits", traits, dict)

        msg = {
            "type": "profile",
            "profileData": traits,
            "ts": timestamp,
            "identity": user_id,
        }

        if anonymous_id:
            msg.update({"objectId": anonymous_id})

        return self._enqueue(msg)

    def alias(
        self,
        previous_id=None,
        user_id=None,
        context=None,
        timestamp=None,
        integrations=None,
    ):
        return None

    def group(
        self,
        user_id=None,
        group_id=None,
        traits=None,
        context=None,
        timestamp=None,
        anonymous_id=None,
        integrations=None,
    ):
        return None

    def screen(
        self,
        user_id=None,
        category=None,
        name=None,
        properties=None,
        context=None,
        timestamp=None,
        anonymous_id=None,
        integrations=None,
    ):
        return None

    def page(
        self,
        user_id=None,
        category=None,
        name=None,
        properties=None,
        context=None,
        timestamp=None,
        anonymous_id=None,
        integrations=None,
    ):
        return None

    def _prepare_msg(self, msg):
        timestamp = msg["ts"]
        if timestamp is None:
            timestamp = datetime.now(tzutc())

        require("type", msg["type"], string_types)
        require("ts", timestamp, datetime)

        # add the common keys and their values
        timestamp = guess_timezone(timestamp)
        msg["ts"] = str(int(timestamp.timestamp()))
        msg["identity"] = stringify_id(msg.get("identity", None))
        msg["objectId"] = stringify_id(msg.get("objectId", None))

        return clean(msg)
