from datetime import datetime

from dateutil.tz import tzutc

from fam_analytics_py.base import BaseConsumer
from fam_analytics_py.request import post


class SegmentConsumer(BaseConsumer):
    def request(self, batch, attempt=0):
        """Attempt to upload the batch and retry before raising an error"""
        try:
            post(
                url=self.url,
                auth=self.auth,
                headers=self.headers,
                batch=batch,
                sentAt=datetime.utcnow().replace(tzinfo=tzutc()).isoformat(),
            )
        except Exception:
            if attempt > self.retries:
                raise
            self.request(batch, attempt + 1)
