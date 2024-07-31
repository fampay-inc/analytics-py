from datetime import datetime
import uuid

from dateutil.tz import tzutc
from requests.auth import HTTPBasicAuth

from fam_analytics_py.base import BaseClient
from fam_analytics_py.types import ID_TYPES
from fam_analytics_py.utils import (
    clean,
    remove_trailing_slash,
    require,
)

from .config import MixpanelConfig
from .constants import MessageType
from .consumer import MixpanelConsumer


class MixpanelClient(BaseClient):
    DEFAULT_HOST = "https://api.mixpanel.com"

    def __init__(
        self,
        config: MixpanelConfig,
        max_queue_size: int = 10000,
    ):
        self.config = config

        super(MixpanelClient, self).__init__(
            host=config.host_url,
            debug=config.enable_debug,
            max_queue_size=max_queue_size,
            on_error=config.error_callback,
            send=config.start_consumer,
        )

    @property
    def upload_size(self):
        return 100

    def _get_consumer(self):
        return MixpanelConsumer(
            config=self.config,
            queue=self.queue,
            url=self._get_url(),
            auth=self._get_auth(),
            headers=self._get_headers(),
            upload_size=self.upload_size,
            on_error=self.on_error,
        )

    def _get_url(self):
        return remove_trailing_slash(self.host or self.DEFAULT_HOST)

    def _get_auth(self):
        return HTTPBasicAuth(
            username=self.config.service_account_username,
            password=self.config.service_account_secret,
        )

    def _get_headers(self):
        return {
            "accept": "application/json",
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
        require("user_id / anonymous_id", user_id or anonymous_id, ID_TYPES)
        require("event", event, str)

        properties = properties or {}
        require("properties", properties, dict)

        insert_id = str(uuid.uuid4())
        distinct_id = str(user_id or anonymous_id)

        properties.update(
            {
                "time": timestamp,
                "$insert_id": insert_id,
                "distinct_id": distinct_id,
            }
        )
        self._update_timestamp_if_needed(properties=properties)

        msg = {
            "type": MessageType.event,
            "event": event,
            "properties": properties,
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
        require("user_id / anonymous_id", user_id or anonymous_id, ID_TYPES)
        traits = traits or {}
        require("traits", traits, dict)

        msg = {
            "type": MessageType.profile,
            "$token": self.config.project_token,
            "$distinct_id": str(user_id or anonymous_id),
            "$set": traits,
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

    def _update_timestamp_if_needed(self, properties: dict):
        datetime_obj = properties.pop("time")
        if datetime_obj:
            require("time", datetime_obj, datetime)
        else:
            datetime_obj = datetime.now(tzutc())
        properties["time"] = int(datetime_obj.timestamp())

    def _prepare_msg(self, msg):
        return clean(msg)
