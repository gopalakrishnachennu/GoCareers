from django.shortcuts import get_object_or_404, redirect
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse

from users.models import ConsultantProfile, User
from jobs.models import Job
from .services import LLMService, build_input_summary, get_system_prompt_text


@login_required
def draft_preview_llm(request, pk):
    user = request.user
    if not (user.is_superuser or user.role in ('ADMIN', 'EMPLOYEE')):
        return redirect('home')

    consultant_profile = get_object_or_404(ConsultantProfile, user__pk=pk)
    job_id = request.GET.get('job')
    if not job_id:
        return JsonResponse({'error': 'job_required'}, status=400)

    job = get_object_or_404(Job, pk=job_id)
    llm = LLMService()
    user_prompt = llm._build_prompt(job, consultant_profile)
    system_prompt = get_system_prompt_text(job, consultant_profile)
    summary = build_input_summary(job, consultant_profile)
    return JsonResponse({
        'system_prompt': system_prompt,
        'user_prompt': user_prompt,
        'summary': summary,
    })


 
