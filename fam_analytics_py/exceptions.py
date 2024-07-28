class APIError(Exception):
    def __init__(self, url, status, code, message):
        self.url = url
        self.status = status
        self.code = code
        self.message = message

    def __str__(self):
        msg = "[Analytics: {0}] {1}: {2} ({3})"
        return msg.format(self.url, self.code, self.message, self.status)
