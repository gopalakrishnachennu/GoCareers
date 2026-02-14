import base64
import hashlib
from django.conf import settings
from cryptography.fernet import Fernet, InvalidToken


def _derive_key() -> bytes:
    if settings.LLM_ENCRYPTION_KEY:
        return settings.LLM_ENCRYPTION_KEY.encode()
    # Fallback: derive from SECRET_KEY if explicit key not set
    digest = hashlib.sha256(settings.SECRET_KEY.encode()).digest()
    return base64.urlsafe_b64encode(digest)


def get_fernet() -> Fernet:
    return Fernet(_derive_key())


def encrypt_value(value: str) -> str:
    if not value:
        return ''
    f = get_fernet()
    return f.encrypt(value.encode()).decode()


def decrypt_value(value: str) -> str:
    if not value:
        return ''
    f = get_fernet()
    try:
        return f.decrypt(value.encode()).decode()
    except InvalidToken:
        return ''
