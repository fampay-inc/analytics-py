import unittest

from fam_analytics_py.request import APIError


class TestExceptions(unittest.TestCase):
    def test_api_exception_str_representation(self):
        url = "http://test.com"
        expected_status = 200
        code = "200"
        message = "OK"

        err = APIError(url=url, status=expected_status, code=code, message=message)

        received_str_rep = str(err)
        expected_str_rep = f"[Analytics: {url}] {code}: {message} ({expected_status})"

        self.assertEqual(received_str_rep, expected_str_rep)
