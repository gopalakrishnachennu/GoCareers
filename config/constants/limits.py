"""
==========================================================
LIMITS, THRESHOLDS & METRICS
==========================================================
All numeric limits, file sizes, pagination, and thresholds.
Change once here → applies everywhere.
"""

# --- Pagination ---
PAGINATION_DEFAULT = 10
PAGINATION_JOBS = 10
PAGINATION_CONSULTANTS = 12
PAGINATION_SUBMISSIONS = 10
PAGINATION_SAVED_JOBS = 10

# --- File Upload Limits ---
MAX_UPLOAD_SIZE_MB = 5                              # in MB
MAX_UPLOAD_SIZE = MAX_UPLOAD_SIZE_MB * 1024 * 1024  # in bytes
ALLOWED_UPLOAD_EXTENSIONS = ['pdf', 'png', 'jpg', 'jpeg', 'doc', 'docx']
MAX_CSV_SIZE_MB = 10
MAX_CSV_SIZE = MAX_CSV_SIZE_MB * 1024 * 1024

# --- Resume ---
RESUME_MAX_LENGTH = 5000          # max characters for generated resume content
RESUME_DUPLICATE_CHECK = True     # prevent duplicate resume generation

# --- Reviews ---
REVIEW_MIN_RATING = 1
REVIEW_MAX_RATING = 5

# --- Dashboard ---
DASHBOARD_RECENT_ITEMS = 5       # how many recent items to show on dashboards
DASHBOARD_RECENT_JOBS = 5

# --- Jobs ---
JOB_TITLE_MAX_LENGTH = 200
JOB_DESCRIPTION_MAX_LENGTH = 5000

# --- CSV Bulk Upload ---
CSV_REQUIRED_HEADERS = ['title', 'company', 'location', 'description']

# --- Security ---
MAX_LOGIN_ATTEMPTS = 5
SESSION_TIMEOUT_MINUTES = 60
