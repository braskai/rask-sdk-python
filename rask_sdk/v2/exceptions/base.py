class RaskClientException(Exception):
    """Rask Client Exception."""

    def __init__(self, status_code: int, detail: str):
        """."""

        super().__init__(detail)
        self.status = status_code

    def __str__(self):
        return f"{self.status}: {super().__str__()}"
