import json
import unittest
from datetime import datetime, date
from queue import Queue
from unittest.mock import patch

import pytz

from fam_analytics_py.clevertap import CleverTapClient, CleverTapConsumer

from . import MockResponse


class TestCleverTapClient(unittest.TestCase):
    def fail(self, e, batch):
        """Mark the failure handler"""
        self.failed = True

    def setUp(self):
        self.failed = False
        self.client = CleverTapClient(
            credentials={
                "clevertap_account_id": "",
                "clevertap_passcode": "",
            },
            on_error=self.fail,
        )

    def test_requires_credentials(self):
        self.assertRaises(AssertionError, CleverTapClient)

    def test_empty_flush(self):
        self.client.flush()

    @patch("requests.Session.post")
    def test_basic_track(self, mocked_function):

        mocked_function.return_value = MockResponse({}, status_code=200)

        client = self.client
        success, msg = client.track("userId", "python test event")
        client.flush()
        self.assertTrue(success)
        self.assertFalse(self.failed)

        self.assertEqual(msg["evtName"], "python test event")
        self.assertTrue(isinstance(msg["ts"], str))
        self.assertTrue(isinstance(msg["ts"], str))
        self.assertEqual(msg["identity"], "userId")
        self.assertEqual(msg["evtData"], {})
        self.assertEqual(msg["type"], "event")

    @patch("requests.Session.post")
    def test_stringifies_user_id(self, mocked_function):
        mocked_function.return_value = MockResponse({}, status_code=200)

        # A large number that loses precision in node:
        # node -e "console.log(157963456373623802 + 1)" > 157963456373623800
        client = self.client
        success, msg = client.track(
            user_id=157963456373623802, event="python test event"
        )
        client.flush()
        self.assertTrue(success)
        self.assertFalse(self.failed)

        self.assertEqual(msg["identity"], "157963456373623802")
        self.assertEqual(msg["objectId"], None)

    @patch("requests.Session.post")
    def test_stringifies_anonymous_id(self, mocked_function):
        mocked_function.return_value = MockResponse({}, status_code=200)

        # A large number that loses precision in node:
        # node -e "console.log(157963456373623803 + 1)" > 157963456373623800
        client = self.client
        success, msg = client.track(
            anonymous_id=157963456373623803, event="python test event"
        )
        client.flush()
        self.assertTrue(success)
        self.assertFalse(self.failed)

        self.assertEqual(msg["identity"], None)
        self.assertEqual(msg["objectId"], "157963456373623803")

    @patch("requests.Session.post")
    def test_advanced_track(self, mocked_function):
        mocked_function.return_value = MockResponse({}, status_code=200)

        client = self.client
        success, msg = client.track(
            "userId",
            "python test event",
            {"property": "value"},
            {"ip": "192.168.0.1"},
            datetime(2014, 9, 3, tzinfo=pytz.utc),
            "anonymousId",
            {"Amplitude": True},
        )

        self.assertTrue(success)

        self.assertEqual(
            msg["ts"], str(int(datetime(2014, 9, 3, tzinfo=pytz.utc).timestamp()))
        )
        self.assertEqual(msg["evtData"], {"property": "value"})
        self.assertEqual(msg["evtName"], "python test event")
        self.assertEqual(msg["objectId"], "anonymousId")

        self.assertEqual(msg["identity"], "userId")
        self.assertEqual(msg["type"], "event")

    @patch("requests.Session.post")
    def test_basic_identify(self, mocked_function):
        mocked_function.return_value = MockResponse({}, status_code=200)

        client = self.client
        success, msg = client.identify("userId", {"trait": "value"})
        client.flush()
        self.assertTrue(success)
        self.assertFalse(self.failed)

        self.assertEqual(msg["profileData"], {"trait": "value"})
        self.assertTrue(isinstance(msg["ts"], str))
        self.assertEqual(msg["identity"], "userId")
        self.assertEqual(msg["type"], "profile")

    @patch("requests.Session.post")
    def test_advanced_identify(self, mocked_function):
        mocked_function.return_value = MockResponse({}, status_code=200)

        client = self.client
        success, msg = client.identify(
            "userId",
            {"trait": "value"},
            {"ip": "192.168.0.1"},
            datetime(2014, 9, 3, tzinfo=pytz.utc),
            "anonymousId",
            {"Amplitude": True},
        )

        self.assertTrue(success)
        self.assertEqual(
            msg["ts"], str(int(datetime(2014, 9, 3, tzinfo=pytz.utc).timestamp()))
        )
        self.assertEqual(msg["profileData"], {"trait": "value"})
        self.assertEqual(msg["objectId"], "anonymousId")
        self.assertTrue(isinstance(msg["ts"], str))
        self.assertEqual(msg["identity"], "userId")
        self.assertEqual(msg["type"], "profile")

    @patch("requests.Session.post")
    def test_flush(self, mocked_function):
        mocked_function.return_value = MockResponse({}, status_code=200)

        client = self.client
        # set up the consumer with more requests than a single batch will allow
        for i in range(1000):
            success, msg = client.identify("userId", {"trait": "value"})
        # We can't reliably assert that the queue is non-empty here; that's
        # a race condition. We do our best to load it up though.
        client.flush()
        # Make sure that the client queue is empty after flushing
        self.assertTrue(client.queue.empty())

    @patch("requests.Session.post")
    def test_overflow(self, mocked_function):
        mocked_function.return_value = MockResponse({}, status_code=200)

        client = CleverTapClient(
            credentials={"clevertap_account_id": "", "clevertap_passcode": ""},
            max_queue_size=1,
        )
        # Ensure consumer thread is no longer uploading
        client.join()

        for i in range(10):
            client.identify("userId")

        success, msg = client.identify("userId")
        # Make sure we are informed that the queue is at capacity
        self.assertFalse(success)

    @patch("requests.Session.post")
    def test_numeric_user_id(self, mocked_function):
        mocked_function.return_value = MockResponse({}, status_code=200)

        self.client.track(1234, "python event")
        self.client.flush()
        self.assertFalse(self.failed)

    def test_debug(self):
        CleverTapClient(
            credentials={"clevertap_account_id": "", "clevertap_passcode": ""},
            debug=True,
        )

    @patch("requests.Session.post")
    def test_identify_with_date_object(self, mocked_function):
        mocked_function.return_value = MockResponse({}, status_code=200)

        client = self.client
        success, msg = client.identify(
            "userId",
            {
                "birthdate": date(1981, 2, 2),
            },
        )
        client.flush()
        self.assertTrue(success)
        self.assertFalse(self.failed)

        self.assertEqual(msg["profileData"], {"birthdate": date(1981, 2, 2)})

    @patch("requests.Session.post")
    def test_request_body(self, mocked_function):
        mocked_function.return_value = MockResponse({}, status_code=200)

        client = self.client
        client.track(
            "abcd",
            event="testing",
            properties={},
            timestamp=datetime(2014, 9, 3, tzinfo=pytz.utc),
        )
        client.flush()

        mocked_function.assert_called_with(
            "https://in1.api.clevertap.com/1/upload",
            data=json.dumps(
                {
                    "d": [
                        {
                            "type": "event",
                            "evtName": "testing",
                            "evtData": {},
                            "ts": str(
                                int(datetime(2014, 9, 3, tzinfo=pytz.utc).timestamp())
                            ),
                            "identity": "abcd",
                            "objectId": None,
                        }
                    ],
                }
            ),
            auth=None,
            headers={
                "X-CleverTap-Account-Id": "",
                "X-CleverTap-Passcode": "",
                "content-type": "application/json",
            },
            timeout=15,
        )


class TestCleverTapConsumer(unittest.TestCase):
    def test_next(self):
        q = Queue()
        consumer = CleverTapConsumer(
            q,
            write_key=None,
            url="https://in1.api.clevertap.com/1/upload",
            auth="",
            headers={},
        )
        q.put(1)
        next = consumer.next()
        self.assertEqual(next, [1])

    def test_next_limit(self):
        q = Queue()
        upload_size = 50
        consumer = CleverTapConsumer(
            q,
            write_key=None,
            url="https://in1.api.clevertap.com/1/upload",
            auth="",
            headers={},
            upload_size=upload_size,
        )
        for i in range(10000):
            q.put(i)
        next = consumer.next()
        self.assertEqual(next, list(range(upload_size)))

    @patch("requests.Session.post")
    def test_upload_success(self, mocked_function):
        mocked_function.return_value = MockResponse({}, 200)

        q = Queue()
        consumer = CleverTapConsumer(
            q,
            write_key=None,
            url="https://in1.api.clevertap.com/1/upload",
            auth="",
            headers={},
        )
        track = {"type": "track", "event": "python event", "userId": "userId"}
        q.put(track)
        success = consumer.upload()
        self.assertTrue(success)

    @patch("requests.Session.post")
    def test_upload_failure(self, mocked_function):
        mocked_function.return_value = MockResponse({}, status_code=400)

        self.on_err_called = False

        def custom_on_err(e, batch):
            self.on_err_called = True

        q = Queue()
        consumer = CleverTapConsumer(
            q,
            write_key=None,
            url="https://in1.api.clevertap.com/1/upload",
            auth="",
            headers={},
            on_error=custom_on_err,
        )

        track = {"type": "track", "event": "python event", "userId": "userId"}
        q.put(track)
        consumer.upload()

        self.assertTrue(self.on_err_called)

    @patch("requests.Session.post")
    def test_request(self, mocked_function):
        mocked_function.return_value = MockResponse({}, 200)

        consumer = CleverTapConsumer(
            None,
            write_key=None,
            url="https://in1.api.clevertap.com/1/upload",
            auth="",
            headers={},
        )
        track = {"type": "track", "event": "python event", "userId": "userId"}
        consumer.request([track])

    def test_pause(self):
        consumer = CleverTapConsumer(
            None,
            write_key=None,
            url="https://in1.api.clevertap.com/1/upload",
            auth="",
            headers={},
        )
        consumer.pause()
        self.assertFalse(consumer.running)

    @patch("requests.Session.post")
    def test_raise_retry_exception(self, mocked_function):
        consumer = CleverTapConsumer(
            None,
            write_key=None,
            url="https://in1.api.clevertap.com/1/upload",
            auth="",
            headers={},
        )

        with self.assertRaises(Exception):
            track = {"type": "track", "event": "python event", "userId": "userId"}
            consumer.request(track, attempt=4)
