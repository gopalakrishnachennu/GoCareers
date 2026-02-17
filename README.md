# GoCareers — EduTech Pro Consulting Platform

> A full-stack Django platform for managing consultants, employees, job postings, and **AI-powered resume generation** with real-time ATS validation.

---

## ✨ Key Features

### 👤 Consultant & Employee Management
- Full consultant profiles with bio, skills, certifications, and base resume text
- Employee dashboard with Role-Based Access Control (RBAC)
- Audit logging for all user actions

### 📄 AI Resume Generation (v2.0)
- **ATS-Validated Drafts** — AI generates resume drafts scored against Applicant Tracking System standards
- **Resume Templates & Template Packs** — Reusable, structured layouts tied to marketing roles
- **LLM Input Preferences** — Users control which data sections feed into the AI prompt
- **Validation Warnings & Errors** — Real-time feedback on keyword gaps, formatting issues, and ATS score
- **Full Request Payload Logging** — Every LLM call is logged with system prompt, user prompt, and input summary for auditability

### 🤖 LLM Configuration & Prompt Management
- Centralized LLM config (model selection, temperature, max tokens)
- Encrypted API key storage
- **Prompt Library** — Create, version, and manage prompt templates with CRUD operations
- **LLM Usage Logs** — Track every AI generation including tokens used, model, and request payload
- Config versioning with automatic snapshots on every change

### 💼 Job Postings & Tracking
- Job creation and management
- Bulk job upload support
- Consultant-to-job matching for targeted resume generation

### 📊 System Administration
- System health monitoring dashboard
- Platform configuration management
- LLM usage log viewer with detailed request inspection

---

## 🛠 Tech Stack

| Layer | Technology |
| :--- | :--- |
| **Backend** | Python 3.10+, Django 5.2 |
| **Frontend** | Django Templates, HTMX, django-browser-reload |
| **Database** | SQLite (dev) / PostgreSQL (prod via `dj-database-url`) |
| **AI/LLM** | OpenAI API (GPT-4o-mini default) |
| **Task Queue** | Celery + Redis |
| **Styling** | Tailwind CSS via `django-tailwind` |
| **Deployment** | Docker, Docker Compose, Gunicorn, WhiteNoise |

---

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- pip

### Setup

```bash
# Clone
git clone https://github.com/gopalakrishnachennu/GoCareers.git
cd consulting

# Virtual environment
python3 -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Environment variables
cp .env.example .env  # Edit with your OpenAI API key

# Database
python manage.py migrate

# Run
python manage.py runserver
```

Access the application at `http://127.0.0.1:8000/`

### Docker

```bash
docker-compose up --build
```

---

## 📁 Project Structure

```
consulting/
├── apps/
│   ├── core/           # System config, LLM config, audit logs, health checks
│   ├── jobs/           # Job postings and bulk upload
│   ├── messaging/      # Internal messaging
│   ├── prompts_app/    # AI prompt template library (CRUD)
│   ├── resumes/        # AI resume generation, templates, ATS validation
│   ├── submissions/    # Job application submissions
│   └── users/          # User profiles, consultant management, RBAC
├── config/             # Django settings, URLs, WSGI/ASGI
├── templates/          # Django HTML templates
├── static/             # Static assets
├── theme/              # Tailwind CSS theme
└── manage.py
```

---

## 🔖 Releases

### v2.0.0 — AI Resume Engine (Current)
- ATS-validated AI resume drafts with scoring
- Resume Templates & Template Packs
- LLM Input Preferences for controlled AI generation
- Prompt Library with full CRUD
- LLM Usage Logging with request payload inspection
- Consultant profile enhancements (base resume text)
- Streamlined LLM config (removed legacy prompt template coupling)

### v1.0.0 — Foundation
- Consultant & Employee management
- Job postings and tracking
- Basic AI resume generation
- System health monitoring
- Platform configuration & audit logging

---

## 📄 License

This project is private and proprietary.
