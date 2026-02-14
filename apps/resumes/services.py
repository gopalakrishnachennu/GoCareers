import openai
import time
from django.utils.html import strip_tags
from django.utils import timezone
from django.db.models import Sum
from docx import Document
from io import BytesIO
from prompts_app.services import get_active_prompt_for_job
from django.utils.html import strip_tags
from core.models import LLMConfig, LLMUsageLog
from core.security import decrypt_value
from core.llm_services import calculate_cost


DEFAULT_SYSTEM_PROMPT = (
    "You are a professional resume writer specializing in consulting and IT staffing. "
    "Generate a polished, ATS-optimized resume tailored to the specific job description. "
    "Use structured Markdown with these sections:\n"
    "## Professional Summary\n"
    "## Key Skills\n"
    "## Relevant Experience\n"
    "## Notable Projects\n"
    "## Education & Certifications\n\n"
    "Be specific, quantify achievements where possible, and align the resume language "
    "with the job description keywords."
)

def get_system_prompt_text(job, consultant):
    prompt = get_active_prompt_for_job(job, consultant)
    if prompt:
        if prompt.system_text:
            return prompt.system_text
        if prompt.description:
            return strip_tags(prompt.description)
    return DEFAULT_SYSTEM_PROMPT


def build_input_summary(job, consultant):
    experiences = []
    for exp in consultant.experience.all():
        experiences.append({
            'title': exp.title,
            'company': exp.company,
            'start_year': exp.start_date.strftime('%Y') if exp.start_date else '',
            'end_year': '' if exp.is_current or not exp.end_date else exp.end_date.strftime('%Y'),
            'is_current': exp.is_current,
        })

    educations = []
    for edu in consultant.education.all():
        educations.append({
            'degree': edu.degree,
            'field_of_study': edu.field_of_study,
            'institution': edu.institution,
            'start_year': edu.start_date.strftime('%Y') if edu.start_date else '',
            'end_year': edu.end_date.strftime('%Y') if edu.end_date else 'Present',
        })

    return {
        'job_title': job.title,
        'job_company': job.company,
        'job_location': job.location or 'Not provided.',
        'job_description': job.description,
        'consultant_name': consultant.user.get_full_name() or consultant.user.username,
        'consultant_email': consultant.user.email or 'Not provided.',
        'consultant_phone': consultant.phone or 'Not provided.',
        'experience': experiences,
        'education': educations,
    }


class LLMService:
    def __init__(self):
        config = LLMConfig.load()
        self.config = config
        self.api_key = decrypt_value(config.encrypted_api_key)
        if self.api_key and not self.api_key.startswith('sk-your') and config.generation_enabled:
            self.client = openai.OpenAI(api_key=self.api_key)
        else:
            self.client = None

    def _build_prompt(self, job, consultant):
        """Build the user prompt from template or default."""
        # Gather contact info
        contact_name = consultant.user.get_full_name() or consultant.user.username
        contact_email = consultant.user.email or "Not provided."
        contact_phone = consultant.phone or "Not provided."

        # Gather experience summary
        experiences = consultant.experience.all()
        exp_summary = "\n".join(
            f"- {e.title} at {e.company} ({e.start_date.strftime('%Y')}–{'Present' if e.is_current else e.end_date.strftime('%Y') if e.end_date else ''})"
            for e in experiences
        ) or "No experience listed."

        # Gather education summary
        educations = consultant.education.all()
        edu_summary = "\n".join(
            f"- {e.degree} in {e.field_of_study} at {e.institution} ({e.start_date.strftime('%Y')}–{e.end_date.strftime('%Y') if e.end_date else 'Present'})"
            for e in educations
        ) or "No education listed."

        # Gather certifications
        certs = consultant.certifications.all()
        cert_summary = ", ".join(c.name for c in certs) or "None listed."

        prompt = get_active_prompt_for_job(job, consultant)
        if prompt:
            try:
                template_text = prompt.template_text
                base = template_text.format(
                    job_title=job.title,
                    company=job.company,
                    job_description=job.description,
                    consultant_name=consultant.user.get_full_name() or consultant.user.username,
                    consultant_bio=consultant.bio or "Not provided.",
                    consultant_skills=", ".join(consultant.skills) if consultant.skills else "Not provided.",
                    experience_summary=exp_summary,
                    certifications=cert_summary,
                )
                return (
                    f"{base}\n\n"
                    f"--- JOB DESCRIPTION ---\n"
                    f"{job.description or 'Not provided.'}\n"
                )
            except (KeyError, IndexError):
                pass  # Fall through to default

        return (
            f"Consultant Name: {contact_name}\n"
            f"Consultant Email: {contact_email}\n"
            f"Consultant Phone: {contact_phone}\n"
            f"Bio: {consultant.bio or 'Not provided.'}\n"
            f"Summary: {consultant.bio or 'Not provided.'}\n"
            f"Skills: {', '.join(consultant.skills) if consultant.skills else 'Not provided.'}\n"
            f"Experience:\n{exp_summary}\n"
            f"Education:\n{edu_summary}\n"
            f"Certifications: {cert_summary}\n\n"
            f"--- TARGET JOB ---\n"
            f"Title: {job.title}\n"
            f"Company: {job.company}\n"
            f"Job Location: {job.location or 'Not provided.'}\n"
            f"Location Rule: Consider roles within 15–30 miles of the job location.\n"
            f"Description:\n{job.description}\n"
            f"\nRequired Resume Sections: Summary, Skills, Experience, Education\n"
        )

    def generate_resume_content(self, job, consultant, actor=None):
        """Generate resume content. Returns (content, tokens_used, error)."""
        prompt_text = self._build_prompt(job, consultant)

        if not self.client:
            mock = (
                f"## Professional Summary\n\n"
                f"Results-driven professional with expertise in "
                f"{', '.join(consultant.skills[:3]) if consultant.skills else 'various technologies'}. "
                f"Seeking the **{job.title}** position at **{job.company}**.\n\n"
                f"## Key Skills\n\n"
            )
            if consultant.skills:
                for skill in consultant.skills:
                    mock += f"- {skill}\n"
            else:
                mock += "- Skills not listed\n"

            mock += (
                f"\n## Relevant Experience\n\n"
                f"*(Experience details from profile)*\n\n"
                f"## Notable Projects\n\n"
                f"*(Projects aligned with {job.title} role)*\n\n"
                f"---\n*Mock resume — install a valid OpenAI API key for real GPT-4o generation.*"
            )
            return mock, 0, None

        if self.config.monthly_token_cap:
            month_start = timezone.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            total_month_tokens = LLMUsageLog.objects.filter(created_at__gte=month_start).aggregate(
                total=Sum('total_tokens')
            )['total'] or 0
            if total_month_tokens >= self.config.monthly_token_cap and self.config.auto_disable_on_cap:
                self.config.generation_enabled = False
                self.config.save()
                return None, 0, "Monthly token cap reached. Generation disabled."

        try:
            start = time.time()
            system_prompt = get_system_prompt_text(job, consultant)
            response = self.client.chat.completions.create(
                model=self.config.active_model or "gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt_text}
                ],
                temperature=float(self.config.temperature),
                max_tokens=self.config.max_output_tokens,
            )
            latency_ms = int((time.time() - start) * 1000)
            content = response.choices[0].message.content
            prompt_tokens = response.usage.prompt_tokens if response.usage else 0
            completion_tokens = response.usage.completion_tokens if response.usage else 0
            tokens = response.usage.total_tokens if response.usage else 0
            costs = calculate_cost(self.config.active_model, prompt_tokens, completion_tokens)
            LLMUsageLog.objects.create(
                model_name=self.config.active_model,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                total_tokens=tokens,
                cost_input=costs['input'],
                cost_output=costs['output'],
                cost_total=costs['total'],
                latency_ms=latency_ms,
                success=True,
                job=job,
                consultant=consultant,
                actor=actor,
            )
            return content, tokens, None
        except Exception as e:
            LLMUsageLog.objects.create(
                model_name=self.config.active_model,
                success=False,
                error_message=str(e),
                job=job,
                consultant=consultant,
                actor=actor,
            )
            return None, 0, str(e)


class DocxService:
    def create_docx(self, content):
        """Convert markdown-ish text content into a simple DOCX document."""
        doc = Document()

        for line in content.split('\n'):
            stripped = line.strip()
            if stripped.startswith('## '):
                doc.add_heading(stripped[3:], level=2)
            elif stripped.startswith('# '):
                doc.add_heading(stripped[2:], level=1)
            elif stripped.startswith('- '):
                doc.add_paragraph(stripped[2:], style='List Bullet')
            elif stripped:
                doc.add_paragraph(stripped)

        buffer = BytesIO()
        doc.save(buffer)
        buffer.seek(0)
        return buffer
