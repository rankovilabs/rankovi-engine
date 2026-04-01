"""
api/main.py — Rankovi FastAPI Backend
Supports local development and Cloud Run production.

Local:      uvicorn api.main:app --reload --port 8000
Production: Cloud Run reads PORT env var, uses Cloud SQL via unix socket
"""

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from contextlib import asynccontextmanager
import psycopg2
import psycopg2.extras
import os
import subprocess
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

ENV = os.getenv('RANKOVI_ENV', 'local')
IS_PROD = ENV == 'production'


# ── DB connection ──────────────────────────────────────────────────────────────

def get_conn():
    """
    Local:      connects via TCP to localhost:5432
    Production: connects via Cloud SQL unix socket (/cloudsql/...)
    """
    host = os.getenv('DB_HOST', 'localhost')
    port = int(os.getenv('DB_PORT', 5432))
    dbname = os.getenv('DB_NAME', 'rankovi')
    user = os.getenv('DB_USER', 'rankovi')
    password = os.getenv('DB_PASSWORD', '')

    if host.startswith('/cloudsql'):
        # Cloud SQL unix socket connection
        return psycopg2.connect(
            host=host,
            dbname=dbname,
            user=user,
            password=password,
            cursor_factory=psycopg2.extras.RealDictCursor,
        )
    else:
        # TCP connection (local dev)
        return psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            cursor_factory=psycopg2.extras.RealDictCursor,
        )


# ── App setup ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        conn = get_conn()
        conn.close()
        print(f"✅ DB connection verified [{ENV}]")
    except Exception as e:
        print(f"⚠️  DB connection failed: {e}")
    yield

app = FastAPI(title="Rankovi API", version="2.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Static files & console ─────────────────────────────────────────────────────

DASHBOARD_DIR = os.path.join(os.path.dirname(__file__), '..', 'dashboard')

@app.get("/")
@app.get("/console")
@app.get("/console/")
async def serve_console():
    """Serve the Rankovi GEO Intelligence Console."""
    path = os.path.join(DASHBOARD_DIR, 'console.html')
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Console not found")
    return FileResponse(path, media_type='text/html')


# ── Health ─────────────────────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "service": "rankovi-api",
        "version": "2.0.0",
        "env": ENV,
    }

@app.get("/health")
def health_detailed():
    """Detailed health check — used by Cloud Run health probes."""
    db_ok = False
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        conn.close()
        db_ok = True
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={"status": "degraded", "db": False, "error": str(e)}
        )
    return {"status": "ok", "db": db_ok, "env": ENV}


# ── Engine trigger ─────────────────────────────────────────────────────────────

def _run_engine_job(brand_id: Optional[int]):
    """
    In production: triggers the Cloud Run Job via gcloud CLI.
    Locally: runs python main.py directly as a subprocess.
    """
    if IS_PROD:
        project = os.getenv('GOOGLE_CLOUD_PROJECT', 'rankovi-prod')
        region = os.getenv('CLOUD_RUN_REGION', 'us-central1')
        cmd = [
            'gcloud', 'run', 'jobs', 'execute', 'rankovi-engine-job',
            f'--region={region}',
            f'--project={project}',
        ]
        if brand_id:
            cmd += ['--update-env-vars', f'BRAND_ID={brand_id}']
        subprocess.Popen(cmd)
    else:
        cmd = ['python', 'main.py']
        if brand_id:
            cmd += ['--brand-id', str(brand_id)]
        else:
            cmd += ['--all-brands']
        subprocess.Popen(cmd)


@app.post("/engine/run")
def trigger_engine_run(
    background_tasks: BackgroundTasks,
    brand_id: Optional[int] = Query(None, description="Brand ID to run. Omit for all active brands.")
):
    """
    Trigger a GEO engine run. Runs asynchronously — returns immediately.
    In production this fires a Cloud Run Job.
    """
    background_tasks.add_task(_run_engine_job, brand_id)
    return {
        "status": "triggered",
        "brand_id": brand_id or "all",
        "message": "Engine run triggered. Check Run History in the console for progress."
    }


# ── Brands ─────────────────────────────────────────────────────────────────────

@app.get("/brands")
def list_brands():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT b.id, b.name, b.domain, b.active,
                       c.name as client_name
                FROM brands b
                JOIN clients c ON c.id = b.client_id
                ORDER BY b.id
            """)
            return {"brands": cur.fetchall()}
    finally:
        conn.close()


@app.get("/brands/{brand_id}/summary")
def brand_summary(brand_id: int):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT r.id as run_id, r.started_at as run_at,
                       ROUND(AVG(res.mention_rate)::numeric, 4) as avg_mention_rate,
                       COUNT(DISTINCT res.engine_id) as engines_used,
                       COUNT(res.id) as total_results,
                       SUM(CASE WHEN res.brand_mentioned THEN 1 ELSE 0 END) as total_mentions
                FROM runs r
                JOIN results res ON res.run_id = r.id
                WHERE r.brand_id = %s AND r.status = 'complete'
                GROUP BY r.id
                ORDER BY r.started_at DESC
                LIMIT 1
            """, (brand_id,))
            latest_run = cur.fetchone()

            cur.execute("""
                SELECT r.id as run_id, r.started_at as run_at,
                       ROUND(AVG(res.mention_rate)::numeric, 4) as avg_mention_rate
                FROM runs r
                JOIN results res ON res.run_id = r.id
                WHERE r.brand_id = %s AND r.status = 'complete'
                GROUP BY r.id
                ORDER BY r.started_at DESC
                LIMIT 5
            """, (brand_id,))
            trend = list(reversed(cur.fetchall()))

            cur.execute("""
                SELECT e.name as engine, COUNT(cit.id) as citation_count
                FROM citations cit
                JOIN results res ON res.id = cit.result_id
                JOIN engines e ON e.id = res.engine_id
                JOIN runs r ON r.id = res.run_id
                WHERE r.brand_id = %s
                GROUP BY e.name
                ORDER BY citation_count DESC
            """, (brand_id,))
            citations_by_engine = cur.fetchall()

            return {
                "brand_id": brand_id,
                "latest_run": latest_run,
                "trend": trend,
                "citations_by_engine": citations_by_engine,
            }
    finally:
        conn.close()


@app.get("/brands/{brand_id}/runs")
def brand_runs(brand_id: int):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT r.id, r.started_at, r.completed_at, r.status,
                       COUNT(DISTINCT res.id) as total_results,
                       COUNT(DISTINCT res.engine_id) as engines_used,
                       SUM(CASE WHEN res.brand_mentioned THEN 1 ELSE 0 END) as brand_mentions,
                       ROUND(AVG(res.mention_rate)::numeric, 4) as avg_mention_rate
                FROM runs r
                LEFT JOIN results res ON res.run_id = r.id
                WHERE r.brand_id = %s
                GROUP BY r.id
                ORDER BY r.started_at DESC
            """, (brand_id,))
            return {"runs": cur.fetchall()}
    finally:
        conn.close()


@app.get("/runs/{run_id}/results")
def run_results(run_id: int, group_by: str = Query('engine', regex='^(engine|prompt)$')):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if group_by == 'engine':
                cur.execute("""
                    SELECT e.name as engine,
                           ROUND(AVG(res.mention_rate)::numeric, 4) as avg_mention_rate,
                           COUNT(res.id) as total_results,
                           SUM(res.passes_run) as total_passes,
                           SUM(res.passes_mentioned) as total_mentions,
                           SUM(CASE WHEN res.brand_mentioned THEN 1 ELSE 0 END) as brand_mentioned_count
                    FROM results res
                    JOIN engines e ON e.id = res.engine_id
                    WHERE res.run_id = %s
                    GROUP BY e.name
                    ORDER BY avg_mention_rate DESC
                """, (run_id,))
            else:
                cur.execute("""
                    SELECT p.prompt_text,
                           p.intent, p.scope,
                           ROUND(AVG(res.mention_rate)::numeric, 4) as avg_mention_rate,
                           COUNT(DISTINCT res.engine_id) as engines_count,
                           SUM(CASE WHEN res.brand_mentioned THEN 1 ELSE 0 END) as brand_mentioned_count
                    FROM results res
                    JOIN prompts p ON p.id = res.prompt_id
                    WHERE res.run_id = %s
                    GROUP BY p.id, p.prompt_text, p.intent, p.scope
                    ORDER BY avg_mention_rate DESC
                """, (run_id,))
            return {"results": cur.fetchall(), "group_by": group_by}
    finally:
        conn.close()


@app.get("/brands/{brand_id}/citations")
def brand_citations(brand_id: int, limit: int = Query(50, le=200)):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT cit.domain, cit.url, cit.is_client_domain, cit.is_competitor,
                       e.name as engine, p.prompt_text as prompt,
                       r.started_at as run_at, r.id as run_id
                FROM citations cit
                JOIN results res ON res.id = cit.result_id
                JOIN engines e   ON e.id   = res.engine_id
                JOIN prompts p   ON p.id   = res.prompt_id
                JOIN runs r      ON r.id   = res.run_id
                WHERE r.brand_id = %s
                ORDER BY r.started_at DESC, cit.id DESC
                LIMIT %s
            """, (brand_id, limit))
            rows = cur.fetchall()

            cur.execute("""
                SELECT e.name as engine,
                       COUNT(cit.id) as citation_count,
                       COUNT(DISTINCT cit.domain) as unique_domains
                FROM citations cit
                JOIN results res ON res.id = cit.result_id
                JOIN engines e   ON e.id   = res.engine_id
                JOIN runs r      ON r.id   = res.run_id
                WHERE r.brand_id = %s
                GROUP BY e.name
                ORDER BY citation_count DESC
            """, (brand_id,))
            by_engine = cur.fetchall()

            cur.execute("""
                SELECT cit.domain, COUNT(*) as citation_count
                FROM citations cit
                JOIN results res ON res.id = cit.result_id
                JOIN runs r      ON r.id   = res.run_id
                WHERE r.brand_id = %s
                GROUP BY cit.domain
                ORDER BY citation_count DESC
                LIMIT 10
            """, (brand_id,))
            top_domains = cur.fetchall()

            return {
                "citations": rows,
                "by_engine": by_engine,
                "top_domains": top_domains,
                "total": len(rows),
            }
    finally:
        conn.close()


# ── Sentiment ──────────────────────────────────────────────────────────────────

@app.get("/brands/{brand_id}/sentiment")
def brand_sentiment(brand_id: int):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT sentiment,
                       COUNT(*) AS count,
                       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
                FROM results res
                JOIN runs r ON r.id = res.run_id
                WHERE r.brand_id = %s AND res.brand_mentioned = true
                GROUP BY sentiment
                ORDER BY count DESC
            """, (brand_id,))
            overall = cur.fetchall()

            cur.execute("""
                SELECT e.name AS engine,
                       res.sentiment,
                       COUNT(*) AS count
                FROM results res
                JOIN engines e ON e.id = res.engine_id
                JOIN runs r ON r.id = res.run_id
                WHERE r.brand_id = %s AND res.brand_mentioned = true
                GROUP BY e.name, res.sentiment
                ORDER BY e.name, count DESC
            """, (brand_id,))
            by_engine = cur.fetchall()

            cur.execute("""
                SELECT p.prompt_text AS prompt,
                       COUNT(*) FILTER (WHERE res.sentiment = 'positive') AS positive,
                       COUNT(*) FILTER (WHERE res.sentiment = 'neutral')  AS neutral,
                       COUNT(*) FILTER (WHERE res.sentiment = 'negative') AS negative,
                       COUNT(*) AS total_mentioned
                FROM results res
                JOIN prompts p ON p.id = res.prompt_id
                JOIN runs r ON r.id = res.run_id
                WHERE r.brand_id = %s AND res.brand_mentioned = true
                GROUP BY p.id, p.prompt_text
                HAVING COUNT(*) >= 2
                ORDER BY positive DESC, negative ASC
            """, (brand_id,))
            by_prompt = cur.fetchall()

            return {
                "overall": overall,
                "by_engine": by_engine,
                "by_prompt": by_prompt,
            }
    finally:
        conn.close()


@app.get("/brands/{brand_id}/sentiment/detail")
def sentiment_detail(brand_id: int, prompt: str = Query(...), sentiment: str = Query(None)):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            query = """
                SELECT res.id,
                       res.raw_response,
                       res.sentiment,
                       res.mention_count,
                       res.position,
                       res.brand_mentioned,
                       e.name       AS engine,
                       r.started_at AS run_at,
                       r.id         AS run_id
                FROM results res
                JOIN engines e ON e.id = res.engine_id
                JOIN prompts p ON p.id = res.prompt_id
                JOIN runs r    ON r.id = res.run_id
                WHERE r.brand_id = %s
                  AND p.prompt_text = %s
                  AND res.brand_mentioned = true
            """
            params = [brand_id, prompt]
            if sentiment:
                query += " AND res.sentiment = %s"
                params.append(sentiment)
            query += " ORDER BY res.sentiment, r.started_at DESC"
            cur.execute(query, params)
            rows = cur.fetchall()
            return {"results": rows, "count": len(rows)}
    finally:
        conn.close()


# ── Competitor Intelligence ────────────────────────────────────────────────────

@app.get("/brands/{brand_id}/competitors")
def brand_competitors(brand_id: int):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) AS total_results,
                       SUM(CASE WHEN res.competitor_mentioned THEN 1 ELSE 0 END) AS competitor_results,
                       ROUND(SUM(CASE WHEN res.competitor_mentioned THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS competitor_pct
                FROM results res
                JOIN runs r ON r.id = res.run_id
                WHERE r.brand_id = %s
            """, (brand_id,))
            overview = cur.fetchone()

            cur.execute("""
                SELECT cit.domain,
                       COUNT(*)                                              AS citation_count,
                       COUNT(DISTINCT e.name)                               AS engines_citing,
                       COUNT(DISTINCT p.id)                                 AS prompts_citing,
                       STRING_AGG(DISTINCT e.name, ', ' ORDER BY e.name)   AS engines_list
                FROM citations cit
                JOIN results res ON res.id = cit.result_id
                JOIN engines e   ON e.id  = res.engine_id
                JOIN prompts p   ON p.id  = res.prompt_id
                JOIN runs r      ON r.id  = res.run_id
                WHERE r.brand_id = %s AND cit.is_competitor = true
                GROUP BY cit.domain
                ORDER BY citation_count DESC
            """, (brand_id,))
            competitor_domains = cur.fetchall()

            cur.execute("""
                SELECT p.prompt_text AS prompt,
                       COUNT(*) AS total_runs,
                       SUM(CASE WHEN res.competitor_mentioned THEN 1 ELSE 0 END) AS competitor_mentions,
                       SUM(CASE WHEN res.brand_mentioned THEN 1 ELSE 0 END) AS brand_mentions,
                       ROUND(SUM(CASE WHEN res.competitor_mentioned THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 0) AS competitor_pct
                FROM results res
                JOIN prompts p ON p.id = res.prompt_id
                JOIN runs r    ON r.id = res.run_id
                WHERE r.brand_id = %s
                GROUP BY p.id, p.prompt_text
                ORDER BY competitor_pct DESC, competitor_mentions DESC
                LIMIT 10
            """, (brand_id,))
            by_prompt = cur.fetchall()

            cur.execute("""
                SELECT e.name AS engine,
                       COUNT(*) AS total,
                       SUM(CASE WHEN res.brand_mentioned THEN 1 ELSE 0 END)      AS brand_mentions,
                       SUM(CASE WHEN res.competitor_mentioned THEN 1 ELSE 0 END) AS competitor_mentions,
                       ROUND(SUM(CASE WHEN res.brand_mentioned THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 0)      AS brand_pct,
                       ROUND(SUM(CASE WHEN res.competitor_mentioned THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 0) AS competitor_pct
                FROM results res
                JOIN engines e ON e.id = res.engine_id
                JOIN runs r    ON r.id = res.run_id
                WHERE r.brand_id = %s
                GROUP BY e.name
                ORDER BY brand_pct DESC
            """, (brand_id,))
            head_to_head = cur.fetchall()

            return {
                "overview": overview,
                "competitor_domains": competitor_domains,
                "by_prompt": by_prompt,
                "head_to_head": head_to_head,
            }
    finally:
        conn.close()


# ── Position / Recommendation Rate ────────────────────────────────────────────

@app.get("/brands/{brand_id}/position")
def brand_position(brand_id: int):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT res.position,
                       COUNT(*) AS count,
                       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
                FROM results res
                JOIN runs r ON r.id = res.run_id
                WHERE r.brand_id = %s
                GROUP BY res.position
                ORDER BY count DESC
            """, (brand_id,))
            overall = cur.fetchall()

            cur.execute("""
                SELECT e.name AS engine, res.position,
                       COUNT(*) AS count,
                       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY e.name), 1) AS pct
                FROM results res
                JOIN engines e ON e.id = res.engine_id
                JOIN runs r    ON r.id = res.run_id
                WHERE r.brand_id = %s
                GROUP BY e.name, res.position
                ORDER BY e.name, count DESC
            """, (brand_id,))
            by_engine = cur.fetchall()

            cur.execute("""
                SELECT p.prompt_text AS prompt, p.intent,
                       COUNT(*) FILTER (WHERE res.position = 'first')   AS first_count,
                       COUNT(*) FILTER (WHERE res.position = 'second')  AS second_count,
                       COUNT(*) FILTER (WHERE res.position = 'passing') AS passing_count,
                       COUNT(*) FILTER (WHERE res.position = 'none')    AS none_count,
                       COUNT(*) AS total,
                       ROUND(COUNT(*) FILTER (WHERE res.position = 'first') * 100.0 / COUNT(*), 0) AS first_pct
                FROM results res
                JOIN prompts p ON p.id = res.prompt_id
                JOIN runs r    ON r.id = res.run_id
                WHERE r.brand_id = %s
                GROUP BY p.id, p.prompt_text, p.intent
                ORDER BY first_pct DESC, first_count DESC
            """, (brand_id,))
            by_prompt = cur.fetchall()

            return {"overall": overall, "by_engine": by_engine, "by_prompt": by_prompt}
    finally:
        conn.close()


# ── Benchmark & Verdict ────────────────────────────────────────────────────────

@app.get("/brands/{brand_id}/benchmark")
def brand_benchmark(brand_id: int):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ROUND(AVG(res.mention_rate)::numeric * 100, 1) AS brand_score
                FROM results res
                JOIN runs r ON r.id = res.run_id
                WHERE r.brand_id = %s AND r.status = 'complete'
                  AND r.id = (
                      SELECT id FROM runs WHERE brand_id = %s AND status = 'complete'
                      ORDER BY started_at DESC LIMIT 1
                  )
            """, (brand_id, brand_id))
            latest = cur.fetchone()
            brand_score = float(latest['brand_score'] or 0)

            cur.execute("""
                SELECT ROUND(
                    SUM(CASE WHEN res.competitor_mentioned THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1
                ) AS competitor_score
                FROM results res
                JOIN runs r ON r.id = res.run_id
                WHERE r.brand_id = %s
            """, (brand_id,))
            comp_row = cur.fetchone()
            competitor_score = float(comp_row['competitor_score'] or 0)

            cur.execute("""
                SELECT ROUND(AVG(res.mention_rate)::numeric * 100, 1) AS score, r.id AS run_id
                FROM results res
                JOIN runs r ON r.id = res.run_id
                WHERE r.brand_id = %s AND r.status = 'complete'
                GROUP BY r.id ORDER BY r.id DESC LIMIT 2
            """, (brand_id,))
            trend_rows = cur.fetchall()
            trend_direction = 'stable'
            trend_delta = 0
            if len(trend_rows) >= 2:
                delta = float(trend_rows[0]['score']) - float(trend_rows[1]['score'])
                trend_delta = round(delta, 1)
                trend_direction = 'up' if delta > 2 else ('down' if delta < -2 else 'stable')

            cur.execute("""
                SELECT COUNT(DISTINCT p.id) AS zero_prompts
                FROM prompts p
                JOIN results res ON res.prompt_id = p.id
                JOIN runs r      ON r.id = res.run_id
                WHERE r.brand_id = %s
                GROUP BY p.id HAVING AVG(res.mention_rate) = 0
            """, (brand_id,))
            zero_count = len(cur.fetchall())

            if brand_score >= 70:
                verdict, verdict_text, verdict_color = "dominant", "You are dominating AI search in your category.", "green"
            elif brand_score >= 50:
                verdict, verdict_text, verdict_color = "strong", "You have strong AI visibility — above average for your category.", "green"
            elif brand_score >= 35:
                verdict, verdict_text, verdict_color = "average", "Your AI visibility is average. Significant opportunity to grow.", "amber"
            elif brand_score >= 20:
                verdict, verdict_text, verdict_color = "below", "You are below average. Competitors are more visible than you in AI search.", "amber"
            else:
                verdict, verdict_text, verdict_color = "losing", "You are losing AI search visibility to competitors.", "red"

            return {
                "brand_score": brand_score, "competitor_score": competitor_score,
                "trend_direction": trend_direction, "trend_delta": trend_delta,
                "zero_visibility_prompts": zero_count,
                "verdict": verdict, "verdict_text": verdict_text, "verdict_color": verdict_color,
            }
    finally:
        conn.close()


# ── Opportunities ──────────────────────────────────────────────────────────────

@app.get("/brands/{brand_id}/opportunities")
def brand_opportunities(brand_id: int):
    INTENT_WEIGHTS = {'comparison': 1.0, 'solution-aware': 0.9, 'problem-aware': 0.8, 'vendor-aware': 0.6}
    ACTION_TEMPLATES = {
        'comparison': ["Create a dedicated comparison page targeting this query", "Add a structured comparison table with pricing, turnaround, and features", "Ensure your brand appears with clear differentiators vs competitors"],
        'solution-aware': ["Create solution-specific landing pages targeting this query", "Add FAQ content directly answering this question with specific details", "Include cited statistics and named data sources to increase AI citability"],
        'problem-aware': ["Create content that answers this problem directly — lead with the solution", "Add step-by-step guidance with your brand as the recommended answer", "Target this query with a dedicated blog post or service page"],
        'vendor-aware': ["Strengthen E-E-A-T signals: author credentials, review counts, and case studies", "Add specific pricing, turnaround times, and service details AI can extract", "Ensure schema markup clearly identifies your brand entity"],
        'default': ["Create focused content targeting this specific query", "Add FAQ sections with direct answers AI systems can cite", "Build topical authority around this query category"],
    }
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT p.id, p.prompt_text, p.intent, p.scope,
                       ROUND(AVG(res.mention_rate)::numeric, 4) AS avg_mention_rate,
                       ROUND(AVG(CASE WHEN res.competitor_mentioned THEN 1.0 ELSE 0.0 END)::numeric, 4) AS competitor_rate,
                       COUNT(*) FILTER (WHERE res.position = 'first') AS first_count,
                       COUNT(*) FILTER (WHERE res.position = 'none')  AS none_count,
                       COUNT(*) AS total_results
                FROM results res
                JOIN prompts p ON p.id = res.prompt_id
                JOIN runs r    ON r.id = res.run_id
                WHERE r.brand_id = %s
                GROUP BY p.id, p.prompt_text, p.intent, p.scope
            """, (brand_id,))
            rows = cur.fetchall()

        opportunities = []
        for r in rows:
            mention_rate = float(r['avg_mention_rate'] or 0)
            comp_rate    = float(r['competitor_rate'] or 0)
            intent       = r['intent'] or 'default'
            weight       = INTENT_WEIGHTS.get(intent, 0.7)
            opp_score    = round((1.0 - mention_rate) * weight * (1 + comp_rate) * 100, 1)
            priority     = 'high' if opp_score >= 80 else ('medium' if opp_score >= 50 else 'low')
            opportunities.append({
                "prompt": r['prompt_text'], "intent": intent, "scope": r['scope'],
                "mention_rate": mention_rate, "competitor_rate": comp_rate,
                "first_count": r['first_count'], "none_count": r['none_count'],
                "opp_score": opp_score, "priority": priority,
                "actions": ACTION_TEMPLATES.get(intent, ACTION_TEMPLATES['default']),
            })

        opportunities.sort(key=lambda x: x['opp_score'], reverse=True)
        return {
            "opportunities": opportunities[:15],
            "total_prompts": len(opportunities),
            "high_priority":   sum(1 for o in opportunities if o['priority'] == 'high'),
            "medium_priority": sum(1 for o in opportunities if o['priority'] == 'medium'),
            "zero_visibility": sum(1 for o in opportunities if o['mention_rate'] == 0),
        }
    finally:
        conn.close()


