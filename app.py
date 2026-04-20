"""
SDA Dashboard - Flask API Backend
Clean schema: measures(quarter_id, dataset_id, geo_id, measure_name, value)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
REQUIRED ENVIRONMENT VARIABLES (set in Railway → Variables)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  DASHBOARD_PASSWORD  Regular user login password.
                      Falls back to 'sda2026' for local dev only.

  ADMIN_PASSWORD      Separate admin-only password for /admin routes.
                      Must be set independently — there is no fallback
                      (admin upload is disabled if this is empty).

  SECRET_KEY          Flask session signing secret. Set to a long random
                      hex string. Generated fresh each restart if unset
                      (sessions are lost on redeploy — set this in prod).

  DB_PATH             Absolute path to the SQLite database file on the
                      Railway persistent volume, e.g.:
                        /data/sda_dashboard_clean.db
                      Falls back to sda_dashboard_clean.db next to app.py
                      for local dev. Mount a persistent volume at /data
                      in Railway to survive redeploys.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
import os
import re
import secrets
import tempfile
from datetime import datetime, timezone
from functools import wraps
from flask import Flask, jsonify, request, send_from_directory, abort, session, redirect, url_for
from flask_cors import CORS
import sqlite3

# ── Password config ────────────────────────────────────────
# Set DASHBOARD_PASSWORD as an environment variable in Railway.
# Falls back to 'sda2026' for local dev only.
DASHBOARD_PASSWORD = os.environ.get('DASHBOARD_PASSWORD', 'sda2026')

# Admin password — no fallback; empty string disables admin upload.
ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', '')

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', secrets.token_hex(32))
CORS(app)

# DB_PATH: use DB_PATH env var (Railway persistent volume) or default local path.
_LOCAL_DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sda_dashboard_clean.db')
DB_PATH = os.environ.get('DB_PATH', _LOCAL_DB)

# If DB_PATH points to a volume path that is missing OR empty (e.g. first
# Railway deploy, or a previous bad deploy that created an empty DB file),
# seed it by copying the packaged database from the container.
# Set FORCE_RESEED=true in Railway env vars to force a reseed from the
# packaged DB (use once to fix a corrupted/wrong production database, then remove).
def _db_is_empty():
    """Return True if DB_PATH has no measures data (empty or uninitialised)."""
    try:
        _c = sqlite3.connect(DB_PATH)
        n = _c.execute("SELECT COUNT(*) FROM measures").fetchone()[0]
        _c.close()
        return n == 0
    except Exception:
        return True

_FORCE_RESEED = os.environ.get('FORCE_RESEED', '').lower() in ('1', 'true', 'yes')

if DB_PATH != _LOCAL_DB and (_FORCE_RESEED or not os.path.exists(DB_PATH) or _db_is_empty()):
    if os.path.exists(_LOCAL_DB):
        import shutil
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        shutil.copy2(_LOCAL_DB, DB_PATH)
        print("DB_PATH seed: copied {} → {}".format(_LOCAL_DB, DB_PATH))
    else:
        print("WARNING: DB_PATH={} does not exist and no local DB to seed from.".format(DB_PATH))

# ── NDIA demand projections (SDA Pricing Review 2022-23, June 2023) ────────
# Source: Exhibits 4 and 5, NDIA SDA Pricing Review – Demand Projections, June 2023
NDIA_DEMAND_PROJECTIONS = [
    {"year": 2022, "total": 22873, "IL": 10026, "FA": 4350, "Robust": 1866, "HPS": 6631},
    {"year": 2027, "total": 27022, "IL": 10987, "FA": 5304, "Robust": 2420, "HPS": 8311},
    {"year": 2032, "total": 29742, "IL": 11543, "FA": 5983, "Robust": 2911, "HPS": 9305},
    {"year": 2037, "total": 32976, "IL": 12325, "FA": 6776, "Robust": 3534, "HPS": 10341},
    {"year": 2042, "total": 36684, "IL": 13256, "FA": 7715, "Robust": 4239, "HPS": 11474},
]

# ── Last admin upload result (in-process cache) ────────────
_last_upload_result = None


# ── DB schema init + backfill on startup ────────────────────
def _init_db():
    """
    Ensure the DB file exists with the full schema.
    Safe to run on every startup — all statements use IF NOT EXISTS / OR IGNORE.

    Also backfills sda_capacity from P.6 measures data for any quarters
    not yet present (handles DBs that predate the sda_capacity feature).
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS quarters (
                quarter_id   INTEGER PRIMARY KEY AUTOINCREMENT,
                year         INTEGER NOT NULL,
                quarter_num  INTEGER NOT NULL,
                quarter_str  TEXT    NOT NULL UNIQUE,
                UNIQUE(year, quarter_num)
            );
            CREATE TABLE IF NOT EXISTS datasets (
                dataset_id   INTEGER PRIMARY KEY AUTOINCREMENT,
                source_table TEXT NOT NULL UNIQUE,
                description  TEXT
            );
            CREATE TABLE IF NOT EXISTS geographies (
                geo_id    INTEGER PRIMARY KEY AUTOINCREMENT,
                geo_type  TEXT NOT NULL,
                name      TEXT NOT NULL,
                state     TEXT,
                parent_id INTEGER REFERENCES geographies(geo_id),
                UNIQUE(geo_type, name, state)
            );
            CREATE TABLE IF NOT EXISTS measures (
                measure_id INTEGER PRIMARY KEY AUTOINCREMENT,
                quarter_id INTEGER NOT NULL REFERENCES quarters(quarter_id),
                dataset_id INTEGER NOT NULL REFERENCES datasets(dataset_id),
                geo_id     INTEGER NOT NULL REFERENCES geographies(geo_id),
                measure_name TEXT,
                value        REAL,
                UNIQUE(quarter_id, dataset_id, geo_id, measure_name)
            );
            CREATE TABLE IF NOT EXISTS sda_capacity (
                quarter          TEXT PRIMARY KEY,
                total_dwellings  INTEGER,
                total_places     INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_measures_quarter   ON measures(quarter_id);
            CREATE INDEX IF NOT EXISTS idx_measures_dataset   ON measures(dataset_id);
            CREATE INDEX IF NOT EXISTS idx_measures_geo       ON measures(geo_id);
            CREATE INDEX IF NOT EXISTS idx_measures_composite ON measures(quarter_id, dataset_id, geo_id);
        """)

        # Seed base geographies if the table is empty (fresh DB)
        if conn.execute("SELECT COUNT(*) FROM geographies").fetchone()[0] == 0:
            conn.executescript("""
                INSERT OR IGNORE INTO geographies (geo_type, name, state, parent_id)
                VALUES ('Australia', 'Australia', NULL, NULL);
                INSERT OR IGNORE INTO geographies (geo_type, name, state, parent_id) VALUES
                  ('State', 'New South Wales',              'NSW', 1),
                  ('State', 'Victoria',                     'VIC', 1),
                  ('State', 'Queensland',                   'QLD', 1),
                  ('State', 'South Australia',              'SA',  1),
                  ('State', 'Western Australia',            'WA',  1),
                  ('State', 'Tasmania',                     'TAS', 1),
                  ('State', 'Northern Territory',           'NT',  1),
                  ('State', 'Australian Capital Territory', 'ACT', 1);
            """)

        # Backfill sda_capacity from existing P.6 measures for any missing quarters
        rows = conn.execute("""
            SELECT q.quarter_str, m.measure_name, CAST(m.value AS INTEGER) AS v
            FROM measures m
            JOIN datasets    d  ON m.dataset_id = d.dataset_id
            JOIN quarters    q  ON m.quarter_id = q.quarter_id
            JOIN geographies g  ON m.geo_id     = g.geo_id
            LEFT JOIN sda_capacity sc ON sc.quarter = q.quarter_str
            WHERE d.source_table = 'Table P.6'
              AND g.geo_type     = 'Australia'
              AND sc.quarter IS NULL
            ORDER BY q.quarter_str
        """).fetchall()

        quarters_p6 = {}
        for quarter_str, measure_name, value in rows:
            quarters_p6.setdefault(quarter_str, {})[measure_name] = value or 0

        RESIDENT_MULT = {
            '1 Resident': 1, '2 Residents': 2, '3 Residents': 3,
            '4 Residents': 4, '5 Residents': 5, '6+ Residents': 6,
        }
        for quarter_str, m in quarters_p6.items():
            conn.execute(
                "INSERT OR IGNORE INTO sda_capacity (quarter, total_dwellings, total_places) VALUES (?,?,?)",
                (quarter_str, m.get('Total', 0),
                 sum(m.get(k, 0) * mult for k, mult in RESIDENT_MULT.items())),
            )

        conn.commit()
        conn.close()
    except Exception as e:
        print("WARNING: DB init error: {}".format(e))

_init_db()

# ── Auth helpers ───────────────────────────────────────────

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('authenticated'):
            # API routes return 401, page routes redirect to login
            if request.path.startswith('/api/') or request.path.startswith('/data/'):
                return jsonify({'error': 'Unauthorised'}), 401
            return redirect(url_for('login_page'))
        return f(*args, **kwargs)
    return decorated


@app.route('/login', methods=['GET'])
def login_page():
    if session.get('authenticated'):
        return redirect('/')
    return send_from_directory(
        os.path.dirname(os.path.abspath(__file__)), 'login.html'
    )


@app.route('/login', methods=['POST'])
def do_login():
    data = request.get_json() or {}
    if data.get('password') == DASHBOARD_PASSWORD:
        session['authenticated'] = True
        return jsonify({'ok': True})
    return jsonify({'ok': False, 'error': 'Incorrect password'}), 401


@app.route('/logout')
def logout():
    session.clear()
    return redirect('/login')


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ═══════════════════════════════════════════════════════════
# BOUNDARY FILES  (sa4.geojson / sa3.geojson)
# ═══════════════════════════════════════════════════════════

@app.route('/')
@login_required
def index():
    folder = os.path.dirname(os.path.abspath(__file__))
    return send_from_directory(folder, 'dashboard_v7.html')


@app.route('/data/<path:filename>', methods=['GET'])
@login_required
def serve_geojson(filename):
    """
    Serve sa4.geojson and sa3.geojson from the same folder as app.py.
    Run download_boundaries.py first to generate these files.
    """
    allowed = {'sa4.geojson', 'sa3.geojson'}
    if filename not in allowed:
        abort(404)
    folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    full_path = os.path.join(folder, filename)
    if not os.path.exists(full_path):
        abort(404, description=(
            f"{filename} not found. "
            "Run download_boundaries.py first, then restart Flask."
        ))
    return send_from_directory(folder, filename, mimetype='application/geo+json')


# ═══════════════════════════════════════════════════════════
# CORE API
# ═══════════════════════════════════════════════════════════

@login_required
@app.route('/api/health', methods=['GET'])
def health_check():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) as total FROM measures")
    total = cursor.fetchone()['total']
    cursor.execute("SELECT MAX(quarter_str) as latest FROM quarters")
    latest = cursor.fetchone()['latest']
    conn.close()
    return jsonify({'status': 'ok', 'measures': total, 'quarter_range': latest})


@login_required
@app.route('/api/filters', methods=['GET'])
def get_filters():
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT DISTINCT d.dataset_id, d.source_table, d.description
        FROM datasets d
        JOIN measures m ON d.dataset_id = m.dataset_id
        WHERE d.source_table LIKE 'Table P.%'
        ORDER BY d.source_table
    """)
    datasets = [dict(row) for row in cursor.fetchall()]

    cursor.execute("""
        SELECT quarter_id, quarter_str, year, quarter_num
        FROM quarters ORDER BY year, quarter_num
    """)
    quarters = [dict(row) for row in cursor.fetchall()]

    cursor.execute("""
        SELECT geo_id, geo_type, name, state
        FROM geographies
        ORDER BY
            CASE geo_type
                WHEN 'Australia' THEN 1
                WHEN 'State'     THEN 2
                WHEN 'SA4'       THEN 3
                ELSE 4
            END, state, name
    """)
    geographies = [dict(row) for row in cursor.fetchall()]

    conn.close()
    return jsonify({'datasets': datasets, 'quarters': quarters, 'geographies': geographies})


@login_required
@app.route('/api/summary', methods=['GET'])
def get_summary():
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) as n FROM measures")
    total_measures = cursor.fetchone()['n']

    cursor.execute("SELECT COUNT(*) as n FROM quarters")
    total_quarters = cursor.fetchone()['n']

    cursor.execute("SELECT COUNT(*) as n FROM geographies WHERE geo_type='SA4'")
    total_sa4 = cursor.fetchone()['n']

    cursor.execute("""
        SELECT quarter_str, year, quarter_num
        FROM quarters ORDER BY year DESC, quarter_num DESC LIMIT 1
    """)
    _row = cursor.fetchone()
    if _row is None:
        conn.close()
        return jsonify({'error': 'No data loaded yet. Please upload Supplement P data via the admin portal.'}), 503
    latest_q = dict(_row)

    cursor.execute("""
        SELECT SUM(m.value) as total
        FROM measures m
        JOIN datasets    d ON m.dataset_id = d.dataset_id
        JOIN quarters    q ON m.quarter_id = q.quarter_id
        JOIN geographies g ON m.geo_id     = g.geo_id
        WHERE d.source_table = 'Table P.4'
          AND m.measure_name = 'Total'
          AND g.geo_type     = 'Australia'
          AND q.quarter_str  = ?
    """, (latest_q['quarter_str'],))
    enrolled = cursor.fetchone()['total'] or 0

    conn.close()
    return jsonify({
        'total_measures':           total_measures,
        'total_quarters':           total_quarters,
        'total_sa4_regions':        total_sa4,
        'latest_quarter':           latest_q,
        'total_enrolled_dwellings': int(enrolled),
    })


@login_required
@app.route('/api/data/timeseries', methods=['POST'])
def get_timeseries():
    f = request.json or {}
    conn = get_db()
    cursor = conn.cursor()

    query = """
        SELECT
            q.quarter_str,
            q.year,
            q.quarter_num,
            g.geo_type,
            g.name  AS geo_name,
            g.state,
            m.measure_name,
            SUM(m.value) AS value
        FROM measures m
        JOIN quarters    q ON m.quarter_id = q.quarter_id
        JOIN geographies g ON m.geo_id     = g.geo_id
        JOIN datasets    d ON m.dataset_id = d.dataset_id
        WHERE 1=1
    """
    params = []

    if f.get('dataset_id'):
        query += " AND m.dataset_id = ?"
        params.append(f['dataset_id'])

    geo_type = f.get('geo_type', 'Australia')
    query += " AND g.geo_type = ?"
    params.append(geo_type)

    if f.get('measure_name'):
        query += " AND m.measure_name = ?"
        params.append(f['measure_name'])

    if f.get('measure_names'):
        ph = ','.join('?' * len(f['measure_names']))
        query += f" AND m.measure_name IN ({ph})"
        params.extend(f['measure_names'])

    if f.get('exclude_ever'):
        query += " AND LOWER(m.measure_name) NOT LIKE '%ever%'"

    if f.get('active_providers_only'):
        query += " AND LOWER(m.measure_name) LIKE '%active in%'"
        query += " AND LOWER(m.measure_name) NOT LIKE '%ever%'"

    if f.get('quarter_str'):
        query += " AND q.quarter_str = ?"
        params.append(f['quarter_str'])

    query += """
        GROUP BY q.quarter_str, q.year, q.quarter_num,
                 g.geo_type, g.name, g.state, m.measure_name
        ORDER BY q.year, q.quarter_num, g.name
    """

    cursor.execute(query, params)
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify({'data': results})


# ═══════════════════════════════════════════════════════════
# PROVIDERS
# ═══════════════════════════════════════════════════════════

@login_required
@app.route('/api/providers', methods=['GET'])
def get_providers():
    """Active SDA providers from Table P.3."""
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT q.quarter_str, q.year, q.quarter_num, m.value
        FROM measures m
        JOIN datasets    d ON m.dataset_id = d.dataset_id
        JOIN quarters    q ON m.quarter_id = q.quarter_id
        JOIN geographies g ON m.geo_id     = g.geo_id
        WHERE d.source_table = 'Table P.3'
          AND g.geo_type     = 'Australia'
          AND g.name         = 'Australia'
          AND LOWER(m.measure_name) LIKE '%active in%'
          AND LOWER(m.measure_name) NOT LIKE '%ever%'
        ORDER BY q.year, q.quarter_num
    """)
    timeseries = [dict(row) for row in cursor.fetchall()]

    cursor.execute("""
        SELECT q.quarter_str, q.year, q.quarter_num, g.state, g.name AS geo_name, m.value
        FROM measures m
        JOIN datasets    d ON m.dataset_id = d.dataset_id
        JOIN quarters    q ON m.quarter_id = q.quarter_id
        JOIN geographies g ON m.geo_id     = g.geo_id
        WHERE d.source_table = 'Table P.3'
          AND g.geo_type     = 'State'
          AND LOWER(m.measure_name) LIKE '%active in%'
          AND LOWER(m.measure_name) NOT LIKE '%ever%'
        ORDER BY q.year, q.quarter_num, m.value DESC
    """)
    by_state = [dict(row) for row in cursor.fetchall()]

    conn.close()
    return jsonify({'timeseries': timeseries, 'by_state': by_state})


# ═══════════════════════════════════════════════════════════
# PARTICIPANT NEED  (P.9 SA4 / P.17 SA3)
# ═══════════════════════════════════════════════════════════

@login_required
@app.route('/api/participants', methods=['GET'])
def get_participants():
    """
    Participant need from P.9 (SA4) and P.17 (SA3).
    Returns timeseries (Australia total), split cohorts (2024-Q1+),
    by_sa4 and by_sa3 geographic breakdowns.
    """
    conn = get_db()
    cursor = conn.cursor()

    # Australia total — all quarters
    cursor.execute("""
        SELECT q.quarter_str, q.year, q.quarter_num,
               d.source_table, m.measure_name, m.value
        FROM measures m
        JOIN datasets    d ON m.dataset_id = d.dataset_id
        JOIN quarters    q ON m.quarter_id = q.quarter_id
        JOIN geographies g ON m.geo_id     = g.geo_id
        WHERE d.source_table IN ('Table P.9', 'Table P.17')
          AND g.geo_type = 'Australia'
          AND m.measure_name = 'Total participants with SDA need'
        ORDER BY d.source_table, q.year, q.quarter_num
    """)
    timeseries = [dict(row) for row in cursor.fetchall()]

    # Split cohorts (2024-Q1+)
    cursor.execute("""
        SELECT q.quarter_str, q.year, q.quarter_num,
               m.measure_name, m.value
        FROM measures m
        JOIN datasets    d ON m.dataset_id = d.dataset_id
        JOIN quarters    q ON m.quarter_id = q.quarter_id
        JOIN geographies g ON m.geo_id     = g.geo_id
        WHERE d.source_table = 'Table P.9'
          AND g.geo_type = 'Australia'
          AND m.measure_name IN (
              'Participants with SDA in use',
              'Participants SDA eligible, not yet using SDA'
          )
        ORDER BY q.year, q.quarter_num, m.measure_name
    """)
    split = [dict(row) for row in cursor.fetchall()]

    # SA4 breakdown
    cursor.execute("""
        SELECT q.quarter_str, q.year, q.quarter_num,
               g.state, g.name AS geo_name, g.geo_type, m.value
        FROM measures m
        JOIN datasets    d ON m.dataset_id = d.dataset_id
        JOIN quarters    q ON m.quarter_id = q.quarter_id
        JOIN geographies g ON m.geo_id     = g.geo_id
        WHERE d.source_table = 'Table P.9'
          AND g.geo_type IN ('SA4', 'State')
          AND m.measure_name = 'Total participants with SDA need'
        ORDER BY q.year, q.quarter_num, g.state, g.name
    """)
    by_sa4 = [dict(row) for row in cursor.fetchall()]

    # SA3 breakdown
    cursor.execute("""
        SELECT q.quarter_str, q.year, q.quarter_num,
               g.state, g.name AS geo_name, g.geo_type, m.value
        FROM measures m
        JOIN datasets    d ON m.dataset_id = d.dataset_id
        JOIN quarters    q ON m.quarter_id = q.quarter_id
        JOIN geographies g ON m.geo_id     = g.geo_id
        WHERE d.source_table = 'Table P.17'
          AND g.geo_type IN ('SA3', 'State')
          AND m.measure_name = 'Total participants with SDA need'
        ORDER BY q.year, q.quarter_num, g.state, g.name
    """)
    by_sa3 = [dict(row) for row in cursor.fetchall()]

    conn.close()
    return jsonify({
        'timeseries': timeseries,
        'split':      split,
        'by_sa4':     by_sa4,
        'by_sa3':     by_sa3,
    })


# ═══════════════════════════════════════════════════════════
# PARTICIPANT DC  (P.10 SA4 / P.18 SA3)
# ═══════════════════════════════════════════════════════════

@login_required
@app.route('/api/participants/dc', methods=['GET'])
def get_participants_dc():
    """
    Participant need by Design Category from P.10 (SA4) and P.18 (SA3).
    Available from 2024-Q2 onward only.
    """
    conn = get_db()
    cursor = conn.cursor()

    DC_MEASURES = (
        'DC: IL', 'DC: HPS', 'DC: Robust', 'DC: FA',
        'DC: Missing', 'DC: Basic', 'DC: Total'
    )
    placeholders = ','.join('?' * len(DC_MEASURES))

    sa4_rows, sa3_rows = [], []
    for table, geo_level, target in [
        ('Table P.10', 'SA4', sa4_rows),
        ('Table P.18', 'SA3', sa3_rows),
    ]:
        cursor.execute(f"""
            SELECT q.quarter_str, q.year, q.quarter_num,
                   g.state, g.name AS geo_name, g.geo_type,
                   m.measure_name, m.value
            FROM measures m
            JOIN datasets    d ON m.dataset_id = d.dataset_id
            JOIN quarters    q ON m.quarter_id = q.quarter_id
            JOIN geographies g ON m.geo_id     = g.geo_id
            WHERE d.source_table = ?
              AND g.geo_type IN (?, 'State', 'Australia')
              AND m.measure_name IN ({placeholders})
            ORDER BY q.year, q.quarter_num, g.geo_type, g.name, m.measure_name
        """, (table, geo_level, *DC_MEASURES))
        target.extend([dict(r) for r in cursor.fetchall()])

    conn.close()
    return jsonify({'by_sa4': sa4_rows, 'by_sa3': sa3_rows})


# ═══════════════════════════════════════════════════════════
# FINANCIAL  (P.2)
# ═══════════════════════════════════════════════════════════

@login_required
@app.route('/api/dwellings/sa3', methods=['GET'])
def get_dwellings_sa3():
    """
    Enrolled dwellings at SA3 level from P.13 (Build Type), P.14 (Design Category),
    P.15 (Max Residents). Returns all three dimensions for all quarters so the
    dashboard can filter client-side by the selected dimension and value.
    """
    conn = get_db()
    cursor = conn.cursor()

    results = {}
    table_map = {
        'bt':     'Table P.13',
        'dc':     'Table P.14',
        'maxres': 'Table P.15',
    }

    for key, source_table in table_map.items():
        cursor.execute("""
            SELECT q.quarter_str, q.year, q.quarter_num,
                   g.geo_type, g.state, g.name AS geo_name,
                   m.measure_name, m.value
            FROM measures m
            JOIN datasets    d ON m.dataset_id = d.dataset_id
            JOIN quarters    q ON m.quarter_id = q.quarter_id
            JOIN geographies g ON m.geo_id     = g.geo_id
            WHERE d.source_table = ?
              AND g.geo_type IN ('SA3', 'State', 'Australia')
            ORDER BY q.year, q.quarter_num, g.geo_type, g.state, g.name, m.measure_name
        """, (source_table,))
        results[key] = [dict(row) for row in cursor.fetchall()]

    conn.close()
    return jsonify(results)


@login_required
@app.route('/api/financial', methods=['GET'])
def get_financial():
    """Annualised committed supports from Table P.2 — all quarters, all states."""
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT q.quarter_str, q.year, q.quarter_num,
               g.geo_type, g.state, g.name AS geo_name,
               m.measure_name, m.value
        FROM measures m
        JOIN datasets    d ON m.dataset_id = d.dataset_id
        JOIN quarters    q ON m.quarter_id = q.quarter_id
        JOIN geographies g ON m.geo_id     = g.geo_id
        WHERE d.source_table = 'Table P.2'
          AND m.measure_name IN (
              'SDA Committed ($)',
              'SIL Committed ($)',
              'Total Committed ($)'
          )
        ORDER BY q.year, q.quarter_num, g.geo_type, g.name, m.measure_name
    """)
    rows = [dict(row) for row in cursor.fetchall()]

    conn.close()
    return jsonify({'rows': rows})


# ═══════════════════════════════════════════════════════════
# UTILISATION  (P.9 / P.17 — in use vs eligible)
# ═══════════════════════════════════════════════════════════

_SUPPRESSED = {'<11', '<20', '<40'}


def _safe_numeric(v):
    """Return float value, or None if suppressed/NULL/non-numeric."""
    if v is None:
        return None
    if isinstance(v, str) and v.strip() in _SUPPRESSED:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


@login_required
@app.route('/api/utilisation', methods=['GET'])
def get_utilisation():
    """
    SDA utilisation rates derived from P.9 (Australia/State/SA4 level).

    Source detected by description containing the canonical table title so the
    endpoint remains stable if table numbers change in future uploads.

    Suppressed NDIA values (<11, <20, <40) are silently excluded from all
    aggregations and rate calculations.

    Utilisation rate = Participants with SDA in use /
                       (in use + eligible, not yet using) * 100
    The pre-calculated percentage column in the source data uses a different
    denominator and is NOT used here.

    Returns:
        {
          "quarters": ["2024-Q1", ...],
          "rows": [
            {
              "quarter_str": "2024-Q1",
              "geo_type":    "Australia" | "State" | "SA4",
              "geo_name":    "Australia" | "NSW" | "Sydney - Inner",
              "state":       null | "NSW" | "NSW",
              "in_use":      12345,
              "not_yet_using": 5678,
              "total":       18023,
              "utilisation_rate": 68.5   // null if either component suppressed
            },
            ...
          ]
        }
    """
    UTIL_MEASURES = (
        'Participants with SDA in use',
        'Participants SDA eligible, not yet using SDA',
    )
    placeholders = ','.join('?' * len(UTIL_MEASURES))

    conn = get_db()
    cursor = conn.cursor()

    # Three separate focused queries — one per geo_type — each restricted to
    # the SA4-level participation table (P.9 / description contains 'SA4').
    #
    # WHY: The original single query included both the SA4-titled table (P.9)
    # and the SA3-titled table (P.17) via the description LIKE / source_table OR.
    # P.17 also contains State-level rollup rows. When those were aggregated
    # with P.9's State rows under the same (quarter, 'State', state, geo_name)
    # key, State totals doubled.  Separating by geo_type and restricting each
    # query to the SA4 source only eliminates the double-count entirely.

    SA4_SRC = """(
        d.description LIKE '%SDA needs by status and SA4%'
        OR d.source_table = 'Table P.9'
    )"""

    base_select = """
        SELECT q.quarter_str, q.year, q.quarter_num,
               g.geo_type, g.state, g.name AS geo_name,
               m.measure_name, m.value
        FROM measures m
        JOIN datasets    d ON m.dataset_id = d.dataset_id
        JOIN quarters    q ON m.quarter_id = q.quarter_id
        JOIN geographies g ON m.geo_id     = g.geo_id
        WHERE {src}
          AND m.measure_name IN ({ph})
          AND g.geo_type = '{geo}'
        ORDER BY q.year, q.quarter_num, g.state, g.name
    """

    raw_rows = []
    for geo_type in ('Australia', 'State', 'SA4'):
        cursor.execute(
            base_select.format(src=SA4_SRC, ph=placeholders, geo=geo_type),
            UTIL_MEASURES,
        )
        raw_rows.extend(cursor.fetchall())

    cursor.execute("SELECT DISTINCT quarter_str FROM quarters ORDER BY year, quarter_num")
    all_quarters = [r['quarter_str'] for r in cursor.fetchall()]
    conn.close()

    # Aggregate: group by (quarter_str, geo_type, state, geo_name)
    data = {}
    for r in raw_rows:
        val = _safe_numeric(r['value'])
        if val is None:
            continue  # silently skip suppressed / non-numeric values
        key = (r['quarter_str'], r['geo_type'], r['state'] or '', r['geo_name'])
        if key not in data:
            data[key] = {'in_use': 0.0, 'eligible': 0.0,
                         'in_use_ok': False, 'eligible_ok': False}
        m = r['measure_name']
        if m == 'Participants with SDA in use':
            data[key]['in_use'] += val
            data[key]['in_use_ok'] = True
        elif m == 'Participants SDA eligible, not yet using SDA':
            data[key]['eligible'] += val
            data[key]['eligible_ok'] = True

    rows = []
    for (qstr, geo_type, state, geo_name), v in data.items():
        in_use   = v['in_use']
        eligible = v['eligible']
        total    = in_use + eligible
        # Only compute rate if both components have non-suppressed values
        rate = (round(in_use / total * 100, 1) if total > 0 and
                v['in_use_ok'] and v['eligible_ok'] else None)
        rows.append({
            'quarter_str':      qstr,
            'geo_type':         geo_type,
            'geo_name':         geo_name,
            'state':            state or None,
            'in_use':           int(in_use),
            'not_yet_using':    int(eligible),
            'total':            int(total),
            'utilisation_rate': rate,
        })

    # Sort for consistent front-end consumption
    geo_order = {'Australia': 0, 'State': 1, 'SA4': 2}
    rows.sort(key=lambda r: (r['quarter_str'],
                             geo_order.get(r['geo_type'], 9),
                             r['state'] or '',
                             r['geo_name']))

    return jsonify({'quarters': all_quarters, 'rows': rows})


@app.route('/api/sda-capacity')
@login_required
def sda_capacity():
    conn = get_db()
    rows = conn.execute(
        'SELECT quarter, total_dwellings, total_places FROM sda_capacity ORDER BY quarter'
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/demand-projections')
@login_required
def demand_projections():
    return jsonify(NDIA_DEMAND_PROJECTIONS)


# ═══════════════════════════════════════════════════════════
# ADMIN  (independent session from regular dashboard)
# ═══════════════════════════════════════════════════════════

def admin_required(f):
    """Decorator for admin-only routes. Returns 401 JSON if not authenticated."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('admin_authenticated'):
            return jsonify({'error': 'Admin authorisation required'}), 401
        return f(*args, **kwargs)
    return decorated


def fy_to_quarter_str(fy, fyq):
    """
    Convert a financial year string + FY quarter label to the internal
    calendar-based quarter_str used in the database.

    Financial year layout (Australian):
      Q1 = Jul–Sep  (calendar Q3 of start year)  → {start_year}-Q3
      Q2 = Oct–Dec  (calendar Q4 of start year)  → {start_year}-Q4
      Q3 = Jan–Mar  (calendar Q1 of end year)    → {end_year}-Q1
      Q4 = Apr–Jun  (calendar Q2 of end year)    → {end_year}-Q2

    Examples:
      '2024-25', 'Q1' → '2024-Q3'   (Jul–Sep 2024)
      '2024-25', 'Q3' → '2025-Q1'   (Jan–Mar 2025)
      '2025-26', 'Q2' → '2025-Q4'   (Oct–Dec 2025)

    Returns None for invalid inputs.
    """
    m = re.match(r'^(\d{4})-(\d{2})$', fy)
    if not m:
        return None
    start = int(m.group(1))
    end = start + 1
    mapping = {
        'Q1': '{}-Q3'.format(start),
        'Q2': '{}-Q4'.format(start),
        'Q3': '{}-Q1'.format(end),
        'Q4': '{}-Q2'.format(end),
    }
    return mapping.get(fyq)


@app.route('/admin', methods=['GET'])
def admin_page():
    """Serve the admin upload page (auth is handled client-side via /admin/status)."""
    folder = os.path.dirname(os.path.abspath(__file__))
    return send_from_directory(folder, 'admin.html')


@app.route('/admin/login', methods=['POST'])
def admin_login():
    data = request.get_json() or {}
    if not ADMIN_PASSWORD:
        return jsonify({'ok': False, 'error': 'Admin upload is not configured on this server.'}), 403
    if data.get('password') == ADMIN_PASSWORD:
        session['admin_authenticated'] = True
        return jsonify({'ok': True})
    return jsonify({'ok': False, 'error': 'Incorrect admin password.'}), 401


@app.route('/admin/logout', methods=['GET'])
def admin_logout():
    session.pop('admin_authenticated', None)
    return redirect('/admin')


@app.route('/admin/status', methods=['GET'])
@admin_required
def admin_status():
    """Return the result of the last upload attempt, or a ready message."""
    if _last_upload_result:
        return jsonify(_last_upload_result)
    return jsonify({'status': 'ready', 'message': 'No upload has been processed yet.'})


@app.route('/admin/upload', methods=['POST'])
@admin_required
def admin_upload():
    """
    Accept a Supplement P .xlsx file, validate inputs, run the loader,
    and return a JSON result. Temp file is always deleted after processing.
    """
    from load_supplement_p_v4 import run_loader  # lazy import — pandas/openpyxl

    global _last_upload_result

    now = datetime.now(timezone.utc).isoformat()

    # ── Validate file ──────────────────────────────────────
    if 'file' not in request.files:
        return jsonify({'success': False, 'message': 'No file provided.', 'timestamp': now}), 400

    f = request.files['file']
    if not f.filename or not f.filename.lower().endswith('.xlsx'):
        return jsonify({'success': False, 'message': 'File must be an Excel .xlsx file.', 'timestamp': now}), 400

    # ── Validate mode ──────────────────────────────────────
    mode = request.form.get('mode', '').strip()
    if mode not in ('new', 'replace'):
        return jsonify({'success': False, 'message': 'Invalid mode. Must be "new" or "replace".', 'timestamp': now}), 400

    # ── Validate and compute quarter_str ──────────────────
    fy  = request.form.get('fy', '').strip()
    fyq = request.form.get('fyq', '').strip()
    quarter_str = fy_to_quarter_str(fy, fyq)
    if not quarter_str:
        return jsonify({
            'success': False,
            'message': 'Invalid financial year/quarter combination: fy={!r} fyq={!r}'.format(fy, fyq),
            'timestamp': now,
        }), 400

    # ── Save to temp file, run loader, always clean up ────
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            tmp_path = tmp.name
            f.save(tmp_path)

        result = run_loader(tmp_path, quarter_str, mode, db_path=DB_PATH)
        result['timestamp'] = now

    except Exception as exc:
        result = {
            'success': False,
            'quarter': quarter_str,
            'rows_loaded': 0,
            'message': str(exc),
            'timestamp': now,
        }
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except Exception:
                pass

    _last_upload_result = result
    status_code = 200 if result.get('success') else 500
    return jsonify(result), status_code


@app.route('/admin/quarters', methods=['GET'])
@admin_required
def admin_list_quarters():
    """Return all loaded quarters with meaningful SDA metrics for visual verification."""
    conn = get_db()
    rows = conn.execute("""
        SELECT
            q.quarter_str,
            COALESCE(p.value,  0) AS total_participants,
            COALESCE(d.value,  0) AS total_enrolled_dwellings,
            COALESCE(pl.value, 0) AS pipeline
        FROM quarters q
        LEFT JOIN (
            SELECT m.quarter_id, m.value FROM measures m
            JOIN datasets ds ON ds.dataset_id = m.dataset_id
            JOIN geographies g  ON g.geo_id   = m.geo_id
            WHERE ds.source_table = 'Table P.9'
              AND m.measure_name  = 'Total participants with SDA need'
              AND g.geo_type      = 'Australia'
        ) p  ON p.quarter_id  = q.quarter_id
        LEFT JOIN (
            SELECT m.quarter_id, m.value FROM measures m
            JOIN datasets ds ON ds.dataset_id = m.dataset_id
            JOIN geographies g  ON g.geo_id   = m.geo_id
            WHERE ds.source_table = 'Table P.4'
              AND m.measure_name  = 'Total'
              AND g.geo_type      = 'Australia'
        ) d  ON d.quarter_id  = q.quarter_id
        LEFT JOIN (
            SELECT m.quarter_id, m.value FROM measures m
            JOIN datasets ds ON ds.dataset_id = m.dataset_id
            JOIN geographies g  ON g.geo_id   = m.geo_id
            WHERE ds.source_table = 'Table P.8'
              AND m.measure_name  = 'Total'
              AND g.geo_type      = 'Australia'
        ) pl ON pl.quarter_id = q.quarter_id
        ORDER BY q.quarter_str
    """).fetchall()
    conn.close()
    return jsonify({'quarters': [
        {
            'quarter_str':              r['quarter_str'],
            'total_participants':       int(r['total_participants']),
            'total_enrolled_dwellings': int(r['total_enrolled_dwellings']),
            'pipeline':                 int(r['pipeline']),
        }
        for r in rows
    ]})


@app.route('/admin/delete-quarter', methods=['POST'])
@admin_required
def admin_delete_quarter():
    """Delete all data for a given quarter_str."""
    quarter_str = request.form.get('quarter_str', '').strip()
    if not re.match(r'^\d{4}-Q[1-4]$', quarter_str):
        return jsonify({'success': False, 'message': 'Invalid quarter_str: {!r}'.format(quarter_str)}), 400

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute("SELECT quarter_id FROM quarters WHERE quarter_str = ?", (quarter_str,)).fetchone()
        if row is None:
            conn.close()
            return jsonify({'success': False, 'message': 'Quarter not found: {}'.format(quarter_str)}), 404

        qid = row['quarter_id']
        conn.execute("DELETE FROM measures WHERE quarter_id = ?", (qid,))
        conn.execute("DELETE FROM sda_capacity WHERE quarter = ?", (quarter_str,))
        conn.execute("DELETE FROM quarters WHERE quarter_id = ?", (qid,))
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'message': 'Deleted quarter {}'.format(quarter_str)})
    except Exception as exc:
        conn.close()
        return jsonify({'success': False, 'message': str(exc)}), 500


# ═══════════════════════════════════════════════════════════
# STARTUP
# ═══════════════════════════════════════════════════════════

if __name__ == '__main__':
    print("=" * 60)
    print("SDA Dashboard API Server")
    print("=" * 60)
    print(f"\nDatabase: {DB_PATH}")
    print("Endpoints:")
    print("  /data/sa4.geojson   /data/sa3.geojson")
    print("  /api/health         /api/filters       /api/summary")
    print("  /api/data/timeseries  (POST)")
    print("  /api/providers      /api/participants   /api/participants/dc")
    print("  /api/financial")
    print("\nCtrl+C to stop\n" + "=" * 60)
    app.run(debug=True, port=5000)
