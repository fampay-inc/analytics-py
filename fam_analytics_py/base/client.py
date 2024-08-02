import atexit
import logging
import queue

LOGGER = logging.getLogger("fam-analytics-py")


class BaseClient(object):
    """Base Client class. Inherit this to integrate a new client."""

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
        self.queue = queue.Queue(max_queue_size)
        self.host = host
        self.write_key = write_key
        self.credentials = credentials
        self.on_error = on_error
        self.debug = debug
        self.send = send

        self.consumer = self._get_consumer()

        if debug:
            LOGGER.setLevel(logging.DEBUG)

        # if we've disabled sending, just don't start the consumer
        if send:
            # On program exit, allow the consumer thread to exit cleanly.
            # This prevents exceptions and a messy shutdown when the
            # interpreter is destroyed before the daemon thread finishes
            # execution. However, it is *not* the same as flushing the queue!
            # To guarantee all messages have been delivered, you'll still
            # need to call flush().
            atexit.register(self.join)
            self.consumer.start()

    @property
    def upload_size(self):
        raise NotImplementedError

    def _get_consumer(self):
        raise NotImplementedError()

    def _get_url(self):
        raise NotImplementedError()

    def _get_auth(self):
        raise NotImplementedError()

    def _get_headers(self):
        raise NotImplementedError()

    def identify(
        self,
        user_id=None,
        traits=None,
        context=None,
        timestamp=None,
        anonymous_id=None,
        integrations=None,
    ):
        raise NotImplementedError()

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
        raise NotImplementedError()

    def alias(
        self,
        previous_id=None,
        user_id=None,
        context=None,
        timestamp=None,
        integrations=None,
    ):
        raise NotImplementedError()

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
        raise NotImplementedError()

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
        raise NotImplementedError()

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
        raise NotImplementedError()

    def _enqueue(self, msg):
        """Push a new `msg` onto the queue, return `(success, msg)`"""
        msg = self._prepare_msg(msg)
        LOGGER.debug("queueing: %s", msg)

        # if send is False, return msg as if it was successfully queued
        if not self.send:
            return True, msg

        try:
            self.queue.put(msg, block=False)
            LOGGER.debug("enqueued %s.", msg["type"])
            return True, msg
        except queue.Full:
            LOGGER.warn("analytics queue is full")
            return False, msg

    def _prepare_msg(self, msg):
        raise NotImplementedError()

    def flush(self):
        """Forces a flush from the internal queue to the server"""
        queue = self.queue
        size = queue.qsize()
        queue.join()
        # Note that this message may not be pcise, because of threading.
        LOGGER.debug("successfully flushed about %s items.", size)

    def join(self):
        """Ends the consumer thread once the queue is empty. Blocks execution until finished"""
        self.consumer.pause()
        try:
            self.consumer.join()
        except RuntimeError:
            # consumer thread has not started
            pass
