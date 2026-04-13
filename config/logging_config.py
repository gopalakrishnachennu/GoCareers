"""
==========================================================
ADVANCED LOGGING CONFIGURATION
==========================================================
Self-diagnostic structured logging for the entire platform.

Logs to:
  logs/app.log          — All app-level logs (views, models, forms)
  logs/errors.log       — ERROR and CRITICAL only
  logs/requests.log     — Every HTTP request (middleware)
  logs/security.log     — Auth events, permission denials
  logs/debug.log        — DEBUG-level everything (dev only)

Console output in development shows colored, structured logs.
"""

import os
from pathlib import Path


def _resolve_log_dir() -> Path | None:
    """
    Pick a writable log directory.
    Prefer project ./logs, then /tmp/consulting-logs (for readonly platforms).
    """
    candidates = [
        Path(__file__).resolve().parent.parent / 'logs',
        Path(os.getenv("TMPDIR", "/tmp")) / "consulting-logs",
    ]
    for path in candidates:
        try:
            path.mkdir(parents=True, exist_ok=True)
            return path
        except OSError:
            continue
    return None


LOG_DIR = _resolve_log_dir()


def _file_handler(filename: str, level: str, formatter: str, max_bytes: int, backup_count: int, *, filters=None):
    """
    Build a rotating file handler when filesystem is writable.
    Fallback to NullHandler on read-only platforms.
    """
    if LOG_DIR is None:
        return {
            'level': level,
            'class': 'logging.NullHandler',
        }
    cfg = {
        'level': level,
        'class': 'logging.handlers.RotatingFileHandler',
        'filename': str(LOG_DIR / filename),
        'maxBytes': max_bytes,
        'backupCount': backup_count,
        'formatter': formatter,
        'encoding': 'utf-8',
    }
    if filters:
        cfg['filters'] = filters
    return cfg


def get_logging_config(debug=True):
    """Return the full LOGGING dict for Django settings."""
    return {
        'version': 1,
        'disable_existing_loggers': False,

        # --- Formatters ---
        'formatters': {
            'verbose': {
                'format': '{asctime} [{levelname}] {name} | {module}.{funcName}:{lineno} | {message}',
                'style': '{',
                'datefmt': '%Y-%m-%d %H:%M:%S',
            },
            'request': {
                'format': '{asctime} [{levelname}] {name} | {status_code} {message}',
                'style': '{',
                'datefmt': '%Y-%m-%d %H:%M:%S',
            },
            'simple': {
                'format': '{asctime} [{levelname}] {message}',
                'style': '{',
                'datefmt': '%H:%M:%S',
            },
        },

        # --- Filters ---
        'filters': {
            'require_debug_true': {
                '()': 'django.utils.log.RequireDebugTrue',
            },
            'require_debug_false': {
                '()': 'django.utils.log.RequireDebugFalse',
            },
        },

        # --- Handlers ---
        'handlers': {
            # Console (development)
            'console': {
                'level': 'DEBUG' if debug else 'INFO',
                'class': 'logging.StreamHandler',
                'formatter': 'simple',
            },

            # App log — all application activity
            'app_file': _file_handler(
                'app.log', 'INFO', 'verbose', 5 * 1024 * 1024, 5
            ),

            # Error log — ERROR + CRITICAL only
            'error_file': _file_handler(
                'errors.log', 'ERROR', 'verbose', 5 * 1024 * 1024, 10
            ),

            # Request log — HTTP traffic
            'request_file': _file_handler(
                'requests.log', 'INFO', 'verbose', 5 * 1024 * 1024, 3
            ),

            # Security log — auth events
            'security_file': _file_handler(
                'security.log', 'INFO', 'verbose', 5 * 1024 * 1024, 5
            ),

            # Debug log — everything (dev only)
            'debug_file': _file_handler(
                'debug.log', 'DEBUG', 'verbose', 10 * 1024 * 1024, 2,
                filters=['require_debug_true']
            ),
        },

        # --- Loggers ---
        'loggers': {
            # Django internals
            'django': {
                'handlers': ['console', 'app_file', 'error_file'],
                'level': 'INFO',
                'propagate': False,
            },
            'django.request': {
                'handlers': ['request_file', 'error_file', 'console'],
                'level': 'INFO',
                'propagate': False,
            },
            'django.security': {
                'handlers': ['security_file', 'error_file', 'console'],
                'level': 'INFO',
                'propagate': False,
            },
            'django.db.backends': {
                'handlers': ['debug_file'],
                'level': 'DEBUG' if debug else 'WARNING',
                'propagate': False,
            },

            # --- Application Loggers (per-app) ---
            'apps': {
                'handlers': ['console', 'app_file', 'error_file'],
                'level': 'DEBUG' if debug else 'INFO',
                'propagate': False,
            },
            'apps.users': {
                'handlers': ['console', 'app_file', 'security_file', 'error_file'],
                'level': 'DEBUG' if debug else 'INFO',
                'propagate': False,
            },
            'apps.jobs': {
                'handlers': ['console', 'app_file', 'error_file'],
                'level': 'DEBUG' if debug else 'INFO',
                'propagate': False,
            },
            'apps.submissions': {
                'handlers': ['console', 'app_file', 'error_file'],
                'level': 'DEBUG' if debug else 'INFO',
                'propagate': False,
            },
            'apps.resumes': {
                'handlers': ['console', 'app_file', 'error_file'],
                'level': 'DEBUG' if debug else 'INFO',
                'propagate': False,
            },
            'apps.messaging': {
                'handlers': ['console', 'app_file', 'error_file'],
                'level': 'DEBUG' if debug else 'INFO',
                'propagate': False,
            },
            'apps.analytics': {
                'handlers': ['console', 'app_file', 'error_file'],
                'level': 'DEBUG' if debug else 'INFO',
                'propagate': False,
            },

            # Middleware logger
            'middleware': {
                'handlers': ['console', 'request_file', 'error_file'],
                'level': 'DEBUG' if debug else 'INFO',
                'propagate': False,
            },

            # Health check / diagnostics
            'diagnostics': {
                'handlers': ['console', 'app_file'],
                'level': 'DEBUG',
                'propagate': False,
            },
        },
    }
