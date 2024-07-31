import unittest
from unittest.mock import patch

from fam_analytics_py.exceptions import APIError
from fam_analytics_py.request import post
from fam_analytics_py.segment import SegmentClient

from . import MockResponse


class TestSegmentRequests(unittest.TestCase):
    def test_valid_request(self):
        segment_client = SegmentClient(write_key="testsecret")
        res = post(
            url=segment_client._get_url(),
            headers=segment_client._get_headers(),
            auth=segment_client._get_auth(),
            batch=[{"userId": "userId", "event": "python event", "type": "track"}],
        )
        self.assertEqual(res.status_code, 200)

    def test_invalid_request_error(self):
        self.assertRaises(
            Exception, post, "testsecret", "https://api.segment.io", "[{]"
        )

    def test_invalid_host(self):
        self.assertRaises(Exception, post, "testsecret", "api.segment.io/", batch=[])

    @patch("requests.Session.post")
    def test_non_2XX_response(self, mocked_function):
        mocked_function.return_value = MockResponse(
            {"code": "test", "message": "test"}, status_code=400
        )

        segment_client = SegmentClient(write_key="testsecret")

        with self.assertRaises(APIError):
            post(
                url=segment_client._get_url(),
                headers=segment_client._get_headers(),
                auth=segment_client._get_auth(),
                batch=[{"userId": "userId", "event": "python event", "type": "track"}],
            )
