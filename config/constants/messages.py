"""
==========================================================
USER-FACING MESSAGES
==========================================================
All success, error, and info messages shown to users.
Change the wording once → updates across the entire app.
"""

from .branding import SITE_NAME

# --- Auth ---
MSG_LOGIN_HEADING = f"Login to {SITE_NAME}"
MSG_LOGIN_SUCCESS = "Welcome back!"
MSG_LOGOUT_SUCCESS = "You have been logged out."
MSG_LOGIN_REQUIRED = "Please log in to continue."

# --- Home Page ---
MSG_HOME_WELCOME = f"Welcome to {SITE_NAME}"
MSG_HOME_CTA = "Login to Get Started"

# --- Reviews ---
MSG_REVIEW_SUCCESS = "Review submitted successfully!"
MSG_REVIEW_DUPLICATE = "You have already reviewed this consultant."
MSG_REVIEW_SELF = "You cannot review yourself."

# --- Experience / Education / Certifications ---
MSG_EXPERIENCE_ADDED = "Experience added!"
MSG_EXPERIENCE_UPDATED = "Experience updated!"
MSG_EXPERIENCE_DELETED = "Experience deleted!"
MSG_EDUCATION_ADDED = "Education added!"
MSG_EDUCATION_UPDATED = "Education updated!"
MSG_EDUCATION_DELETED = "Education deleted!"
MSG_CERT_ADDED = "Certification added!"
MSG_CERT_UPDATED = "Certification updated!"
MSG_CERT_DELETED = "Certification deleted!"

# --- Jobs ---
MSG_JOB_CREATED = "Job posted successfully!"
MSG_JOB_UPDATED = "Job updated successfully!"
MSG_JOB_SAVED = 'Saved "{title}"!'
MSG_JOB_UNSAVED = 'Removed "{title}" from saved jobs.'
MSG_ONLY_CONSULTANTS_SAVE = "Only consultants can save jobs."

# --- Resumes ---
MSG_RESUME_GENERATED = "Resume generated successfully!"
MSG_RESUME_JOB_CLOSED = "Cannot generate resume for a closed or draft job."
MSG_RESUME_SELF_ONLY = "You can only generate resumes for yourself."
MSG_RESUME_DUPLICATE = "A resume for this job already exists."

# --- Submissions ---
MSG_SUBMISSION_SUCCESS = "Application submitted!"
MSG_SUBMISSION_MISMATCH = "Resume does not match the selected job or consultant."
MSG_SUBMISSION_SELF_ONLY = "You can only submit applications for yourself."
MSG_FILE_TOO_LARGE = "File size too large (max {max_mb}MB)."

# --- CSV Upload ---
MSG_CSV_INVALID_TYPE = "Please upload a CSV file."
MSG_CSV_SUCCESS = "{count} jobs uploaded successfully!"
MSG_CSV_ERROR = "An unexpected error occurred during processing."

# --- Messaging ---
MSG_THREAD_NO_CONSULTANT = "Consultants cannot start threads with other consultants."

# --- Generic ---
MSG_PERMISSION_DENIED = "You do not have permission to perform this action."
MSG_NOT_FOUND = "The requested resource was not found."
MSG_GENERIC_ERROR = "Something went wrong. Please try again."
