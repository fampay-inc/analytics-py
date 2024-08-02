from datetime import datetime
from uuid import uuid4

from dateutil.tz import tzutc
from requests.auth import HTTPBasicAuth
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

from .consumer import SegmentConsumer


class SegmentClient(BaseClient):
    DEFAULT_HOST = "https://api.segment.io"

    def __init__(
        self,
        write_key=None,
        host=None,
        debug=False,
        max_queue_size=10000,
        send=True,
        on_error=None,
    ):
        require("write key", write_key, string_types)

        super(SegmentClient, self).__init__(
            write_key=write_key,
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
        return SegmentConsumer(
            self.queue,
            write_key=self.write_key,
            upload_size=self.upload_size,
            url=self._get_url(),
            auth=self._get_auth(),
            headers=self._get_headers(),
            on_error=self.on_error,
        )

    def _get_url(self):
        return remove_trailing_slash(self.host or self.DEFAULT_HOST) + "/v1/batch"

    def _get_auth(self):
        return HTTPBasicAuth(self.write_key, "")

    def _get_headers(self):
        return {}

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
        context = context or {}
        integrations = integrations or {}
        require("user_id or anonymous_id", user_id or anonymous_id, ID_TYPES)
        require("properties", properties, dict)
        require("event", event, string_types)

        msg = {
            "integrations": integrations,
            "anonymousId": anonymous_id,
            "properties": properties,
            "timestamp": timestamp,
            "context": context,
            "userId": user_id,
            "type": "track",
            "event": event,
        }
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
        context = context or {}
        integrations = integrations or {}
        require("user_id or anonymous_id", user_id or anonymous_id, ID_TYPES)
        require("traits", traits, dict)

        msg = {
            "integrations": integrations,
            "anonymousId": anonymous_id,
            "timestamp": timestamp,
            "context": context,
            "type": "identify",
            "userId": user_id,
            "traits": traits,
        }
        return self._enqueue(msg)

    def alias(
        self,
        previous_id=None,
        user_id=None,
        context=None,
        timestamp=None,
        integrations=None,
    ):
        context = context or {}
        integrations = integrations or {}
        require("previous_id", previous_id, ID_TYPES)
        require("user_id", user_id, ID_TYPES)

        msg = {
            "integrations": integrations,
            "previousId": previous_id,
            "timestamp": timestamp,
            "context": context,
            "userId": user_id,
            "type": "alias",
        }
        return self._enqueue(msg)

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
        traits = traits or {}
        context = context or {}
        integrations = integrations or {}
        require("user_id or anonymous_id", user_id or anonymous_id, ID_TYPES)
        require("group_id", group_id, ID_TYPES)
        require("traits", traits, dict)

        msg = {
            "integrations": integrations,
            "anonymousId": anonymous_id,
            "timestamp": timestamp,
            "groupId": group_id,
            "context": context,
            "userId": user_id,
            "traits": traits,
            "type": "group",
        }

        return self._enqueue(msg)

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
        properties = properties or {}
        context = context or {}
        integrations = integrations or {}
        require("user_id or anonymous_id", user_id or anonymous_id, ID_TYPES)
        require("properties", properties, dict)

        if name:
            require("name", name, string_types)
        if category:
            require("category", category, string_types)

        msg = {
            "integrations": integrations,
            "anonymousId": anonymous_id,
            "properties": properties,
            "timestamp": timestamp,
            "category": category,
            "context": context,
            "userId": user_id,
            "type": "screen",
            "name": name,
        }

        return self._enqueue(msg)

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
        properties = properties or {}
        context = context or {}
        integrations = integrations or {}
        require("user_id or anonymous_id", user_id or anonymous_id, ID_TYPES)
        require("properties", properties, dict)

        if name:
            require("name", name, string_types)
        if category:
            require("category", category, string_types)

        msg = {
            "integrations": integrations,
            "anonymousId": anonymous_id,
            "properties": properties,
            "timestamp": timestamp,
            "category": category,
            "context": context,
            "userId": user_id,
            "type": "page",
            "name": name,
        }

        return self._enqueue(msg)

    def _prepare_msg(self, msg):
        timestamp = msg["timestamp"]
        if timestamp is None:
            timestamp = datetime.utcnow().replace(tzinfo=tzutc())

        require("integrations", msg["integrations"], dict)
        require("type", msg["type"], string_types)
        require("timestamp", timestamp, datetime)
        require("context", msg["context"], dict)

        # add common
        timestamp = guess_timezone(timestamp)
        msg["timestamp"] = timestamp.isoformat()
        msg["messageId"] = str(uuid4())

        msg["userId"] = stringify_id(msg.get("userId", None))
        msg["anonymousId"] = stringify_id(msg.get("anonymousId", None))

        return clean(msg)
