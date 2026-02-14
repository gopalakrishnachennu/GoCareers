from typing import Optional
from .models import Prompt
from users.models import ConsultantProfile
from jobs.models import Job


def get_active_prompt_for_job(job: Job, consultant: ConsultantProfile) -> Optional[Prompt]:
    from core.models import LLMConfig

    config = LLMConfig.load()
    if config.active_prompt and config.active_prompt.is_active:
        return config.active_prompt

    # Default prompt
    default_prompt = Prompt.objects.filter(is_default=True, is_active=True).first()
    if default_prompt:
        return default_prompt

    # Any active prompt
    any_prompt = Prompt.objects.filter(is_active=True).first()
    if any_prompt:
        return any_prompt

    return None
