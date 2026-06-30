"""db.py — one shared connection pool to the Supabase Postgres database."""
import os
from dotenv import load_dotenv
from psycopg_pool import ConnectionPool

load_dotenv()  # reads backend/.env

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL is not set. Copy backend/.env.example to backend/.env "
        "and paste your Supabase connection string."
    )

# A small pool of reusable connections. open=True connects immediately so a
# bad connection string fails loudly at startup instead of on first request.
pool = ConnectionPool(DATABASE_URL, min_size=1, max_size=5, open=True)