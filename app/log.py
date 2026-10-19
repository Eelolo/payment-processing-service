import logging

LOG_FORMAT = "%(asctime)s %(levelname)-7s %(name)s: %(message)s"


def setup_logging(level: str) -> None:
    logging.basicConfig(level=level.upper(), format=LOG_FORMAT, force=True)
    # httpx logs every request at INFO, which duplicates our own webhook logs.
    logging.getLogger("httpx").setLevel(logging.WARNING)
