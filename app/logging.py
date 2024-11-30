import os
import logging
import sys

def _initialize_logger() -> logging.Logger:
    environment = os.getenv("ENV", "dev")
    if environment == "test":
        logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)
        logging.getLogger('sqlalchemy.engine.transaction').setLevel(logging.DEBUG)
        
        class TestLogger(logging.Logger):
            def debug(self, msg, *args, **kwargs):
                print(f"[debug] {msg}")

            def info(self, msg, *args, **kwargs):
                print(f"[info] {msg}")

            def warning(self, msg, *args, **kwargs):
                print(f"[warning] {msg}")

            def warn(self, msg, *args, **kwargs):
                print(f"[warn] {msg}")

            def error(self, msg, *args, **kwargs):
                print(f"[error] {msg}")

            def exception(self, msg, *args, exc_info=True, **kwargs):
                print(f"[exception] {msg}")

            def critical(self, msg, *args, **kwargs):
                print(f"[critical] {msg}")

            def fatal(self, msg, *args, **kwargs):
                print(f"[fatal] {msg}")

            def log(self, level, msg, *args, **kwargs):
                print(f"[log] {msg}")
        
        logger: logging.Logger = TestLogger(name=__name__)
    else:
        logger = logging.getLogger(__name__)
    
    if environment == "prod":
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.DEBUG)

    stream_handler = logging.StreamHandler(sys.stdout)
    log_formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    stream_handler.setFormatter(log_formatter)
    logger.addHandler(stream_handler)
    
    return logger


logger = _initialize_logger()
