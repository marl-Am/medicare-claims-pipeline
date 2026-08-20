"""
conftest.py

Sits at the project root so pytest puts the root on `sys.path`, which lets the
test modules import `ingestion.pipeline` and `app.main`.

It also supplies placeholder database credentials. `app/database.py` reads
PG_HOST and friends from the environment at import time, so importing the API
would fail on a machine without a `.env` file. The tests never open a real
connection: `get_db` is overridden with an in-memory SQLite session, so these
values only need to exist, not point anywhere.
"""

import os

_DB_PLACEHOLDERS: dict[str, str] = {
    "PG_HOST": "localhost",
    "PG_PORT": "5432",
    "PG_DB": "test_db",
    "PG_USER": "test_user",
    "PG_PASSWORD": "test_password",
}

for _key, _value in _DB_PLACEHOLDERS.items():
    os.environ.setdefault(_key, _value)
