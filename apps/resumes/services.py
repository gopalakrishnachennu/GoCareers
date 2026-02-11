import openai
from django.conf import settings
from docx import Document
from io import BytesIO
from .models import PromptTemplate

class LLMService:
    def __init__(self):
        self.api_key = settings.OPENAI_API_KEY if hasattr(settings, 'OPENAI_API_KEY') else None
        if self.api_key:
            self.client = openai.OpenAI(api_key=self.api_key)
        else:
            self.client = None

    def generate_resume_content(self, job, consultant):
        template = PromptTemplate.objects.filter(is_active=True).first()
        if not template:
            prompt_text = f"Generate a professional resume for {consultant.user.get_full_name()} applying for {job.title} at {job.company}. Skills: {consultant.skills}. Bio: {consultant.bio}. Job Description: {job.description}."
        else:
            prompt_text = template.template.format(
                job_title=job.title,
                company=job.company,
                job_description=job.description,
                consultant_name=consultant.user.get_full_name(),
                consultant_bio=consultant.bio,
                consultant_skills=consultant.skills
            )

        if not self.client:
            return f"[MOCK RESUME CONTENT]\n\nGenerated for {consultant.user.username} applying to {job.title}.\n\nSkills: {consultant.skills}\n\n(Install OpenAI key to generate real content)"

        try:
            response = self.client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are a professional resume writer."},
                    {"role": "user", "content": prompt_text}
                ]
            )
            return response.choices[0].message.content
        except Exception as e:
            return f"Error generating resume: {str(e)}"

class DocxService:
    def create_docx(self, content):
        doc = Document()
        doc.add_paragraph(content)
        
        buffer = BytesIO()
        doc.save(buffer)
        buffer.seek(0)
        return buffer
