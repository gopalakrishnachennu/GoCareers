import openai
from django.conf import settings
from docx import Document
from io import BytesIO
from .models import PromptTemplate


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


class LLMService:
    def __init__(self):
        self.api_key = getattr(settings, 'OPENAI_API_KEY', None)
        if self.api_key and not self.api_key.startswith('sk-your'):
            self.client = openai.OpenAI(api_key=self.api_key)
        else:
            self.client = None

    def _build_prompt(self, job, consultant):
        """Build the user prompt from template or default."""
        # Gather experience summary
        experiences = consultant.experience.all()
        exp_summary = "\n".join(
            f"- {e.title} at {e.company} ({e.start_date.strftime('%Y')}–{'Present' if e.is_current else e.end_date.strftime('%Y') if e.end_date else ''})"
            for e in experiences
        ) or "No experience listed."

        # Gather certifications
        certs = consultant.certifications.all()
        cert_summary = ", ".join(c.name for c in certs) or "None listed."

        template = PromptTemplate.objects.filter(is_active=True).first()

        if template:
            try:
                return template.template.format(
                    job_title=job.title,
                    company=job.company,
                    job_description=job.description,
                    consultant_name=consultant.user.get_full_name() or consultant.user.username,
                    consultant_bio=consultant.bio or "Not provided.",
                    consultant_skills=", ".join(consultant.skills) if consultant.skills else "Not provided.",
                    experience_summary=exp_summary,
                    certifications=cert_summary,
                )
            except (KeyError, IndexError):
                pass  # Fall through to default

        return (
            f"Consultant Name: {consultant.user.get_full_name() or consultant.user.username}\n"
            f"Bio: {consultant.bio or 'Not provided.'}\n"
            f"Skills: {', '.join(consultant.skills) if consultant.skills else 'Not provided.'}\n"
            f"Experience:\n{exp_summary}\n"
            f"Certifications: {cert_summary}\n\n"
            f"--- TARGET JOB ---\n"
            f"Title: {job.title}\n"
            f"Company: {job.company}\n"
            f"Description:\n{job.description}\n"
        )

    def generate_resume_content(self, job, consultant):
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

        try:
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": DEFAULT_SYSTEM_PROMPT},
                    {"role": "user", "content": prompt_text}
                ],
                temperature=0.7,
                max_tokens=2000,
            )
            content = response.choices[0].message.content
            tokens = response.usage.total_tokens if response.usage else 0
            return content, tokens, None
        except Exception as e:
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
