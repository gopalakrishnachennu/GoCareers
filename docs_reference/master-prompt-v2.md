# Master resume prompt (v2.0)

## Source of truth

- **Code:** `apps/resumes/management/commands/seed_master_prompt.py` (`SYSTEM_PROMPT` + `GENERATION_RULES`)
- **Apply to database:**  
  `python manage.py seed_master_prompt --force`  
  (Requires an active superuser for create; edit/activate also in **Settings → Master prompt** in the app.)

## What v2.0 changes (vs older seeded text)

1. **Authenticity** — Explicit preference for plain verbs (Built, Led, Implemented, …) and avoidance of AI-resume clichés (Architected, Spearheaded, Leveraged, …) except rare justified use.
2. **Honesty for unknown JD tech** — No mandatory fake bullets for missing stack; adjacent experience only when defensible; NOTES warnings when fit is poor.
3. **Bullet counts** — Targets with acceptable ranges (e.g. 6–8 for current role) instead of rigid “exactly 7 / exactly 6” that encouraged filler.
4. **Domain alignment** — Experience bullets must stay true to each employer; JD industry language concentrated in Summary/Skills when industries differ.
5. **Examples** — Sample bullets use “Built” instead of “Architected” to match the same rules.

## First-time installs

`seed_data` may create a **short** default prompt if none exists. For production-quality generation, run `seed_master_prompt` (with or without `--force`) so the full rules are active.
