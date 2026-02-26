import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """
    Returns a named logger that writes to both stdout and a shared log file.
    Safe to call multiple times — handlers are only attached once per logger.
    All modules should use this instead of calling logging.basicConfig().
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(name)s - %(message)s'
        )

        # Console handler with UTF-8 encoding for emoji support on all platforms
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        # Single shared file handler (UTF-8 for emoji support on Windows)
        fh = logging.FileHandler('sidbot.log', encoding='utf-8')
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        # Prevent log records from bubbling up to the root logger
        # to avoid duplicate output if basicConfig was called elsewhere
        logger.propagate = False

    return logger
