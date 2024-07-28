import json
import logging

from requests import sessions

from fam_analytics_py.exceptions import APIError
from fam_analytics_py.utils import DatetimeSerializer

LOGGER = logging.getLogger("fam-analytics-py")
_session = sessions.Session()


def post(url, headers, auth, _payload=None, **kwargs):
    """Post `_payload` or the `kwargs` to the API"""

    body = _payload
    if not body:
        body = kwargs

    data = json.dumps(body, cls=DatetimeSerializer)

    headers["content-type"] = "application/json"
    LOGGER.debug("making request: %s", data)
    res = _session.post(url, data=data, auth=auth, headers=headers, timeout=15)

    if res.status_code == 200:
        LOGGER.debug("data uploaded successfully")
        return res

    try:
        payload = res.json()
        LOGGER.debug("received response: %s", payload)
        if "message" in payload and "code" in payload:
            raise APIError(url, res.status_code, payload["code"], payload["message"])
        else:
            raise APIError(url, res.status_code, "unknown", res.text)
    except ValueError:
        raise APIError(url, res.status_code, "unknown", res.text)
