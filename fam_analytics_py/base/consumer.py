import logging
from queue import Empty
from threading import Thread

LOGGER = logging.getLogger("fam-analytics-py")


class BaseConsumer(Thread):
    """Consumes the messages from the client's queue."""

    def __init__(
        self, queue, url, auth, headers, write_key=None, upload_size=100, on_error=None
    ):
        """Create a consumer thread."""
        Thread.__init__(self)
        # Make consumer a daemon thread so that it doesn't block program exit
        self.daemon = True
        self.upload_size = upload_size
        self.write_key = write_key
        self.url = url
        self.auth = auth
        self.headers = headers
        self.on_error = on_error
        self.queue = queue
        # It's important to set running in the constructor: if we are asked to
        # pause immediately after construction, we might set running to True in
        # run() *after* we set it to False in pause... and keep running forever.
        self.running = True
        self.retries = 3

    def run(self):
        """Runs the consumer."""
        while self.running:
            self.upload()

    def pause(self):
        """Pause the consumer."""
        self.running = False

    def upload(self):
        """Upload the next batch of items, return whether successful."""
        success = False
        batch = self.next()
        if len(batch) == 0:
            return False

        try:
            self.request(batch)
            success = True
        except Exception as e:
            LOGGER.error(f"error uploading: {e}")
            success = False
            if self.on_error:
                self.on_error(e, batch)
        finally:
            # mark items as acknowledged from queue
            for item in batch:
                self.queue.task_done()
            return success

    def next(self):
        """Return the next batch of items to upload."""
        queue = self.queue
        items = []
        while len(items) < self.upload_size:
            try:
                item = queue.get(block=True, timeout=0.5)
                items.append(item)
            except Empty:
                break

        return items

    def request(self, batch, attempt=0):
        raise NotImplementedError()
