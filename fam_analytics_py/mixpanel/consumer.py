import time
from dataclasses import dataclass, field
from fam_analytics_py.base import BaseConsumer
from fam_analytics_py.request import post

from .config import MixpanelConfig
from .constants import MessageType, PAYLOAD_PATH_MAP


class MixpanelConsumer(BaseConsumer):

    @dataclass
    class MessageBatches:
        events: "list[dict]" = field(default_factory=lambda: [])
        profiles: "list[dict]" = field(default_factory=lambda: [])

    def __init__(
        self,
        config: MixpanelConfig,
        queue,
        url,
        auth,
        headers,
        upload_size=100,
        on_error=None,
    ):
        self.config = config
        super().__init__(
            queue=queue,
            url=url,
            auth=auth,
            headers=headers,
            write_key=None,
            upload_size=upload_size,
            on_error=on_error,
        )

    def _segregate_batch(self, batch: "list[dict]") -> MessageBatches:
        batches = self.MessageBatches()
        for msg in batch:
            msg_type = msg.pop("type")
            if msg_type == MessageType.event:
                batches.events.append(msg)
            elif msg_type == MessageType.profile:
                batches.profiles.append(msg)

        return batches

    def request(self, batch):
        """Attempt to upload the batch and retry before raising an error"""

        batches = self._segregate_batch(batch=batch)

        path_payload_pair = [
            (
                PAYLOAD_PATH_MAP[MessageType.event].format(
                    base_url=self.url,
                    project_id=self.config.project_id,
                ),
                batches.events,
            ),
            (
                PAYLOAD_PATH_MAP[MessageType.profile].format(
                    base_url=self.url,
                ),
                batches.profiles,
            ),
        ]
        for path, payload in path_payload_pair:
            if not payload:
                continue

            attempt = 1
            while True:
                try:
                    post(
                        url=path, auth=self.auth, headers=self.headers, _payload=payload
                    )
                    break
                except Exception:
                    attempt += 1
                    if attempt > self.retries:
                        raise
                    time.sleep(0.1 * attempt)
