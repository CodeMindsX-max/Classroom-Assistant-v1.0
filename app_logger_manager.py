import json
import logging
import os
import threading
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler

APP_LOGGER_NAME = "classroom_assistant"
DEFAULT_LOG_DIRECTORY = "logs"
DEFAULT_LOG_FILENAME = "classroom_assistant.log"
DEFAULT_RETENTION_DAYS = 30
EVENT_LEVEL = 25
DEFAULT_LOG_LEVEL_NAME = os.getenv("CLASSROOM_ASSISTANT_LOG_LEVEL", "INFO").upper()
SENSITIVE_KEYWORDS = {
    "authorization",
    "classroom_url",
    "cookie",
    "password",
    "secret",
    "session",
    "token",
    "url",
}

_LOGGER_LOCK = threading.RLock()

logging.addLevelName(EVENT_LEVEL, "EVENT")

LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "EVENT": EVENT_LEVEL,
}


# This turns every log record into one JSON object
# so the file answers what happened, when, where, why, and how severe it was.
class JsonLogFormatter(logging.Formatter):
    def format(self, record):
        timestamp = datetime.fromtimestamp(record.created).astimezone().isoformat(timespec="seconds")
        payload = {
            "timestamp": timestamp,
            "when": timestamp,
            "severity": record.levelname,
            "level": record.levelname,
            "logger": record.name,
            "thread": record.threadName,
            "what": getattr(record, "what", record.getMessage()),
            "where": getattr(record, "where", f"{record.module}.{record.funcName}:{record.lineno}"),
            "why": getattr(record, "why", "No reason supplied."),
            "message": record.getMessage(),
        }

        context = sanitize_context(getattr(record, "context", {}))
        if context:
            payload["context"] = context

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False)


def _is_sensitive_key(key):
    normalized = str(key).strip().lower()
    return any(token in normalized for token in SENSITIVE_KEYWORDS)


def _sanitize_value(value, key=None, depth=0):
    if key is not None and _is_sensitive_key(key):
        return "[REDACTED]"

    if depth > 6:
        return "[TRUNCATED]"

    if isinstance(value, dict):
        return {
            str(nested_key): _sanitize_value(nested_value, key=nested_key, depth=depth + 1)
            for nested_key, nested_value in value.items()
        }

    if isinstance(value, (list, tuple, set)):
        return [_sanitize_value(item, depth=depth + 1) for item in value]

    if isinstance(value, str):
        return value if len(value) <= 500 else f"{value[:497]}..."

    if isinstance(value, (int, float, bool)) or value is None:
        return value

    return repr(value)


# This redacts sensitive keys before anything reaches the file
# so future UI and scheduler code can log context without leaking private values.
def sanitize_context(context):
    if context is None:
        return {}
    if isinstance(context, dict):
        return _sanitize_value(context)
    return {"value": _sanitize_value(context)}


def _event(self, message, *args, **kwargs):
    if self.isEnabledFor(EVENT_LEVEL):
        self._log(EVENT_LEVEL, message, args, **kwargs)


if not hasattr(logging.Logger, "event"):
    logging.Logger.event = _event


def _get_base_logger():
    return logging.getLogger(APP_LOGGER_NAME)


def _resolve_log_level(level):
    if level is None:
        level = DEFAULT_LOG_LEVEL_NAME
    if isinstance(level, str):
        return LOG_LEVELS.get(level.strip().upper(), logging.INFO)
    return int(level)


def get_log_file_path(log_dir=None):
    base_logger = _get_base_logger()
    configured_path = getattr(base_logger, "_log_path", None)
    if configured_path and log_dir is None:
        return configured_path
    directory = os.path.abspath(log_dir or DEFAULT_LOG_DIRECTORY)
    return os.path.join(directory, DEFAULT_LOG_FILENAME)


def _cleanup_old_logs(log_dir, retention_days):
    cutoff = datetime.now() - timedelta(days=retention_days)
    base_name = DEFAULT_LOG_FILENAME

    for file_name in os.listdir(log_dir):
        if not file_name.startswith(base_name):
            continue

        file_path = os.path.join(log_dir, file_name)
        if not os.path.isfile(file_path):
            continue

        try:
            modified_at = datetime.fromtimestamp(os.path.getmtime(file_path))
        except OSError:
            continue

        if modified_at < cutoff:
            try:
                os.remove(file_path)
            except OSError:
                continue


# This configures one shared rotating file logger
# so every module writes thread-safe JSON logs into the same daily timeline.
def configure_logging(log_dir=None, retention_days=DEFAULT_RETENTION_DAYS, level=None, force=False):
    with _LOGGER_LOCK:
        base_logger = _get_base_logger()
        desired_path = get_log_file_path(log_dir)
        resolved_level = _resolve_log_level(level)

        if (
            base_logger.handlers
            and not force
            and getattr(base_logger, "_log_path", None) == desired_path
            and getattr(base_logger, "_retention_days", None) == retention_days
            and getattr(base_logger, "_configured_level", None) == resolved_level
        ):
            return base_logger

        os.makedirs(os.path.dirname(desired_path), exist_ok=True)
        _cleanup_old_logs(os.path.dirname(desired_path), retention_days)

        for handler in list(base_logger.handlers):
            handler.close()
            base_logger.removeHandler(handler)

        handler = TimedRotatingFileHandler(
            desired_path,
            when="midnight",
            interval=1,
            backupCount=retention_days,
            encoding="utf-8",
            delay=True,
        )
        handler.setLevel(resolved_level)
        handler.setFormatter(JsonLogFormatter())

        base_logger.setLevel(resolved_level)
        base_logger.propagate = False
        base_logger.addHandler(handler)
        base_logger._log_path = desired_path
        base_logger._retention_days = retention_days
        base_logger._configured_level = resolved_level
        return base_logger


# This returns a child logger under the shared application logger
# so each file can report its own context while using one central configuration.
def get_app_logger(name=None):
    configure_logging()
    if not name:
        return _get_base_logger()

    logger_name = name if name.startswith(APP_LOGGER_NAME) else f"{APP_LOGGER_NAME}.{name}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.NOTSET)
    logger.propagate = True
    return logger


def flush_logging():
    with _LOGGER_LOCK:
        for handler in _get_base_logger().handlers:
            handler.flush()


def shutdown_logging():
    with _LOGGER_LOCK:
        base_logger = _get_base_logger()
        for handler in list(base_logger.handlers):
            handler.flush()
            handler.close()
            base_logger.removeHandler(handler)


def _log(level, logger, message, what=None, where=None, why=None, context=None, exc_info=False):
    target_logger = logger or get_app_logger()
    if not target_logger.isEnabledFor(level):
        return
    target_logger.log(
        level,
        message,
        extra={
            "what": what or message,
            "where": where or target_logger.name,
            "why": why or "No reason supplied.",
            "context": context,
        },
        exc_info=exc_info,
    )


def log_debug(logger, message, what=None, where=None, why=None, context=None):
    _log(logging.DEBUG, logger, message, what=what, where=where, why=why, context=context)


def log_info(logger, message, what=None, where=None, why=None, context=None):
    _log(logging.INFO, logger, message, what=what, where=where, why=why, context=context)


def log_warning(logger, message, what=None, where=None, why=None, context=None):
    _log(logging.WARNING, logger, message, what=what, where=where, why=why, context=context)


def log_error(logger, message, what=None, where=None, why=None, context=None, exc_info=False):
    _log(logging.ERROR, logger, message, what=what, where=where, why=why, context=context, exc_info=exc_info)


def log_event(logger, message, what=None, where=None, why=None, context=None):
    _log(EVENT_LEVEL, logger, message, what=what, where=where, why=why, context=context)
