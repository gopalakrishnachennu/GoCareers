# EduTech Pro Consulting Platform

A comprehensive platform for managing consultants, employees, and job postings.

## Installation

### Prerequisites
- Python 3.10+
- pip (Python Package Installer)

### Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd consulting
    ```

2.  **Create a virtual environment:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```

3.  **Install dependencies:**
    This project uses `pip` to manage dependencies. Run the following command to install all required packages:
    ```bash
    pip install -r requirements.txt
    ```

    Running this will adhere to the pinned versions specified in `requirements.txt` to ensure compatibility.

### Running the Application

1.  **Apply migrations:**
    ```bash
    python manage.py migrate
    ```

2.  **Run the development server:**
    ```bash
    python manage.py runserver
    ```

3.  Access the application at `http://127.0.0.1:8000/`.

## Features
- Consultant Management
- Employee Dashboard & RBAC
- Job Postings & Tracking
- AI Resume Draft Generation
- System Health Monitoring
