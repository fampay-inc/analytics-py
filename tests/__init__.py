class MockResponse:
    """
    MockResponse for Generic data
    """

    def __init__(
        self,
        json_data=None,
        status_code=200,
        content=None,
        text_data=None,
        headers=None,
    ):
        self.json_data = json_data
        self.text_data = text_data
        self.status_code = status_code
        self.content = content
        self.headers = headers

    @property
    def text(self):
        return self.text_data or ""

    def json(self):
        return self.json_data

    def raise_for_status(self):
        raise Exception()
