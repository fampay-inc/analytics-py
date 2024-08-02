import unittest
from queue import Queue
from unittest.mock import Mock

from fam_analytics_py.base import BaseClient, BaseConsumer


class TestBaseClient(unittest.TestCase):
    def fail(self, e, batch):
        """Mark the failure handler"""
        self.failed = True

    def setUp(self) -> None:
        self.original_init = BaseClient.__init__
        BaseClient.__init__ = Mock(return_value=None)
        self.client = BaseClient("testsecret", on_error=self.fail)
        self.failed = False

    def tearDown(self) -> None:
        BaseClient.__init__ = self.original_init

    def test_unimplemented_upload_size(self):
        with self.assertRaises(NotImplementedError):
            self.client.upload_size

    def test_unimplemented_consumer_class(self):
        with self.assertRaises(NotImplementedError):
            self.client._get_consumer()

    def test_unimplemented_get_url(self):
        with self.assertRaises(NotImplementedError):
            self.client._get_url()

    def test_unimplemented_get_auth(self):
        with self.assertRaises(NotImplementedError):
            self.client._get_auth()

    def test_unimplemented_get_headers(self):
        with self.assertRaises(NotImplementedError):
            self.client._get_headers()

    def test_unimplemented_get_identity_msg(self):
        with self.assertRaises(NotImplementedError):
            self.client.identify()

    def test_unimplemented_get_track_msg(self):
        with self.assertRaises(NotImplementedError):
            self.client.track()

    def test_unimplemented_get_alias_msg(self):
        with self.assertRaises(NotImplementedError):
            self.client.alias()

    def test_unimplemented_get_group_msg(self):
        with self.assertRaises(NotImplementedError):
            self.client.group()

    def test_unimplemented_get_page_msg(self):
        with self.assertRaises(NotImplementedError):
            self.client.page()

    def test_unimplemented_get_screen_msg(self):
        with self.assertRaises(NotImplementedError):
            self.client.screen()

    def test_unimplemented_prepare_msg(self):
        with self.assertRaises(NotImplementedError):
            self.client._prepare_msg({})


class TestBaseConsumer(unittest.TestCase):
    def setUp(self) -> None:
        q = Queue()
        self.consumer = BaseConsumer(
            q, write_key="", url="https://api.segment.io/v1/batch", auth="", headers={}
        )

    def test_unimplemented_request(self):
        with self.assertRaises(NotImplementedError):
            self.consumer.request(batch=[])
