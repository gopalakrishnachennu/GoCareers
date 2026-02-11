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

# Base directory for logs
LOG_DIR = Path(__file__).resolve().parent.parent / 'logs'
LOG_DIR.mkdir(exist_ok=True)


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
            'app_file': {
                'level': 'INFO',
                'class': 'logging.handlers.RotatingFileHandler',
                'filename': str(LOG_DIR / 'app.log'),
                'maxBytes': 5 * 1024 * 1024,   # 5MB
                'backupCount': 5,
                'formatter': 'verbose',
                'encoding': 'utf-8',
            },

            # Error log — ERROR + CRITICAL only
            'error_file': {
                'level': 'ERROR',
                'class': 'logging.handlers.RotatingFileHandler',
                'filename': str(LOG_DIR / 'errors.log'),
                'maxBytes': 5 * 1024 * 1024,
                'backupCount': 10,
                'formatter': 'verbose',
                'encoding': 'utf-8',
            },

            # Request log — HTTP traffic
            'request_file': {
                'level': 'INFO',
                'class': 'logging.handlers.RotatingFileHandler',
                'filename': str(LOG_DIR / 'requests.log'),
                'maxBytes': 5 * 1024 * 1024,
                'backupCount': 3,
                'formatter': 'verbose',
                'encoding': 'utf-8',
            },

            # Security log — auth events
            'security_file': {
                'level': 'INFO',
                'class': 'logging.handlers.RotatingFileHandler',
                'filename': str(LOG_DIR / 'security.log'),
                'maxBytes': 5 * 1024 * 1024,
                'backupCount': 5,
                'formatter': 'verbose',
                'encoding': 'utf-8',
            },

            # Debug log — everything (dev only)
            'debug_file': {
                'level': 'DEBUG',
                'class': 'logging.handlers.RotatingFileHandler',
                'filename': str(LOG_DIR / 'debug.log'),
                'maxBytes': 10 * 1024 * 1024,
                'backupCount': 2,
                'formatter': 'verbose',
                'filters': ['require_debug_true'],
                'encoding': 'utf-8',
            },
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
