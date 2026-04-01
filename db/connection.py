"""
db/connection.py — v2
Adds mention_rate, passes_run, passes_mentioned columns to insert_result.
Schema migration required: see schema_v2_migration.sql
"""
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from config.settings import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD


def get_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


@contextmanager
def db_cursor():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_active_engines():
    with db_cursor() as cur:
        cur.execute("SELECT * FROM engines WHERE active = true ORDER BY id")
        return cur.fetchall()

def get_prompts_for_brand(brand_id):
    with db_cursor() as cur:
        cur.execute("SELECT * FROM prompts WHERE brand_id=%s AND active=true ORDER BY id", (brand_id,))
        return cur.fetchall()

def get_brand(brand_id):
    with db_cursor() as cur:
        cur.execute("SELECT * FROM brands WHERE id=%s", (brand_id,))
        return cur.fetchone()

def get_active_brands():
    with db_cursor() as cur:
        cur.execute("SELECT * FROM brands WHERE active=true ORDER BY id")
        return cur.fetchall()

def create_run(brand_id, triggered_by, prompt_count, engine_count):
    with db_cursor() as cur:
        cur.execute("""
            INSERT INTO runs (brand_id, triggered_by, prompt_count, engine_count, status)
            VALUES (%s,%s,%s,%s,'running') RETURNING id
        """, (brand_id, triggered_by, prompt_count, engine_count))
        return cur.fetchone()["id"]

def complete_run(run_id, status="complete"):
    with db_cursor() as cur:
        cur.execute("UPDATE runs SET status=%s, completed_at=NOW() WHERE id=%s", (status, run_id))

def insert_result(run_id, prompt_id, engine_id, raw_response,
                  brand_mentioned, mention_count, position, sentiment,
                  competitor_mentioned,
                  mention_rate=None, passes_run=1, passes_mentioned=None):
    with db_cursor() as cur:
        cur.execute("""
            INSERT INTO results (
                run_id, prompt_id, engine_id, raw_response,
                brand_mentioned, mention_count, position, sentiment,
                competitor_mentioned, mention_rate, passes_run, passes_mentioned
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
        """, (
            run_id, prompt_id, engine_id, raw_response,
            brand_mentioned, mention_count, position, sentiment,
            competitor_mentioned,
            mention_rate if mention_rate is not None else (1.0 if brand_mentioned else 0.0),
            passes_run,
            passes_mentioned if passes_mentioned is not None else (1 if brand_mentioned else 0),
        ))
        return cur.fetchone()["id"]

def insert_citations(result_id, citations):
    if not citations: return
    with db_cursor() as cur:
        psycopg2.extras.execute_batch(cur, """
            INSERT INTO citations (result_id, domain, url, is_client_domain, is_competitor)
            VALUES (%s,%s,%s,%s,%s)
        """, [(result_id, c["domain"], c.get("url"), c.get("is_client_domain",False),
               c.get("is_competitor",False)) for c in citations])
