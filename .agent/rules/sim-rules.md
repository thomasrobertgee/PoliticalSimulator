ALWAYS use the Python interpreter in ./.venv for all executions.

NEVER perform complex math or data processing inside the LLM context; always write a Python script in /engine or /scrapers and execute it.

Maintain a project_log.json to track which years of Victorian data have been successfully ingested.
