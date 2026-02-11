"""
Context Processor: Injects branding & config constants into every template.

Usage in templates:
    {{ SITE_NAME }}
    {{ SITE_TAGLINE }}
    {{ COPYRIGHT_TEXT }}
    etc.
"""

from config.constants.branding import (
    SITE_NAME, SITE_TAGLINE, SITE_DESCRIPTION, SITE_FULL_TITLE,
    COMPANY_NAME, COMPANY_EMAIL, COMPANY_PHONE,
    COPYRIGHT_TEXT, META_DESCRIPTION, META_KEYWORDS,
    SOCIAL_TWITTER, SOCIAL_LINKEDIN, SOCIAL_GITHUB,
)
from config.constants.messages import (
    MSG_LOGIN_HEADING, MSG_HOME_WELCOME, MSG_HOME_CTA,
)


def site_config(request):
    """Inject site-wide branding and config into all templates."""
    return {
        # Branding
        'SITE_NAME': SITE_NAME,
        'SITE_TAGLINE': SITE_TAGLINE,
        'SITE_DESCRIPTION': SITE_DESCRIPTION,
        'SITE_FULL_TITLE': SITE_FULL_TITLE,
        'COMPANY_NAME': COMPANY_NAME,
        'COMPANY_EMAIL': COMPANY_EMAIL,
        'COMPANY_PHONE': COMPANY_PHONE,
        'COPYRIGHT_TEXT': COPYRIGHT_TEXT,
        'META_DESCRIPTION': META_DESCRIPTION,
        'META_KEYWORDS': META_KEYWORDS,
        
        # Social
        'SOCIAL_TWITTER': SOCIAL_TWITTER,
        'SOCIAL_LINKEDIN': SOCIAL_LINKEDIN,
        'SOCIAL_GITHUB': SOCIAL_GITHUB,
        
        # Messages (for templates)
        'MSG_LOGIN_HEADING': MSG_LOGIN_HEADING,
        'MSG_HOME_WELCOME': MSG_HOME_WELCOME,
        'MSG_HOME_CTA': MSG_HOME_CTA,

        # Impersonate
        'is_impersonating': getattr(request, 'is_impersonating', False),
        'real_user': getattr(request, 'real_user', None),
    }
