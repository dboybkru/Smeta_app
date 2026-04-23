# Smeta App

Smeta App is a business tool for preparing estimates, commercial offers, and equipment selections.

## What it does

- manages estimates with branching and access sharing
- keeps equipment, installation, and commissioning items in separate sections
- supports AI-assisted estimate creation and editing
- imports price lists from Excel, PDF, and supplier pages
- exports estimates to Excel and PDF
- includes authentication, admin access, and user permissions

## Stack

- Frontend: React
- Backend: FastAPI
- Database: SQLite, ready to move to PostgreSQL later

## Main features

- estimate tree with base estimates and branches
- per-section percent adjustments and tax modes
- automatic matching of installation and commissioning works
- admin panel for AI settings, users, and access control
- audit log for AI actions and system responses

## Local run

Backend:

```powershell
cd backend
venv\Scripts\python.exe -m uvicorn app:app --host 127.0.0.1 --port 8000
```

Frontend:

```powershell
cd frontend
npm start
```

## Notes

The project is under active development. The current codebase includes AI workflows, estimate branching, export templates, and admin-level controls for system behavior.
