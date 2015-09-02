import logging

# TODO Figure out how logging works exactly...

def init_logging():
    formatter = logging.Formatter("%(asctime)s [%(name)s][%(levelname)s] %(message)s",
                                  "%Y-%m-%d %H:%M:%S")

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
