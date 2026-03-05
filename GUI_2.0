"""
F-35 Pilot Training Scheduler
- Loads pilot currency data from Excel (VMFAT-502 format) or falls back to sample data
- Warning/Alert threshold highlighting
- Pyomo-based flight/sim scheduling optimization
Run with: streamlit run f35_scheduler.py
"""

import streamlit as st
import pandas as pd
import plotly.figure_factory as ff
import plotly.express as px
from datetime import datetime, timedelta, date
import random
import io

# ─────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="F-35 Pilot Training Scheduler",
    page_icon="✈️",
    layout="wide",
)

# ─────────────────────────────────────────────
# CURRENCY THRESHOLDS (from VMFAT-502 spreadsheet)
# ─────────────────────────────────────────────
# Format: col_name: {"warning": (operator, value), "alert": (operator, value)}
# operator: "lte" = <=, "gte" = >=
CURRENCY_THRESHOLDS = {
    "EP Sim":          {"warning": ("lte", 30),  "alert": ("lte", 60)},   # days since last EP sim
    "Last Flight":     {"warning": ("lte", 14),  "alert": ("lte", 31)},   # days since last flight
    "Last Night Flight":{"warning": ("gte", 90), "alert": ("gte", 120)},  # days since last night flt
    "30":              {"warning": ("lte", 10.0),"alert": ("lte", 0.0)},  # 30-day hours
    "Sim 30":          {"warning": None,         "alert": None},
    "60":              {"warning": ("lte", 20.0),"alert": ("lte", 10.0)},
    "90":              {"warning": ("lte", 30.0),"alert": ("lte", 15.0)},
    "FY":              {"warning": ("lte", 100.0),"alert": ("lte", 60.0)},
    "FY Sim":          {"warning": ("lte", 50.0),"alert": ("lte", 30.0)},
    "FY Instr":        {"warning": ("lte", 12.0),"alert": ("lte", 6.0)},
    "FY Night":        {"warning": ("lte", 12.0),"alert": ("lte", 6.0)},
    "Last VL":         {"warning": ("gte", 30),  "alert": ("gte", 60)},
    "Last Night VL":   {"warning": ("gte", 30),  "alert": ("gte", 60)},
    "Last AAR (Day)":  {"warning": ("gte", 30),  "alert": ("gte", 60)},
    "Last AAR (Night)":{"warning": ("gte", 30),  "alert": ("gte", 60)},
}

EXCEL_PILOT_COLUMNS = [
    "Name", "EP Sim", "Last Flight", "Last Night Flight", "30", "Sim 30",
    "60", "90", "FY", "FY Sim", "FY Instr", "FY Night",
    "Career F-35", "FTR time", "All T/M/S",
    "Last VL", "Last Night VL", "PFO", "Last AAR (Day)", "Last AAR (Night)"
]

# ─────────────────────────────────────────────
# CONSTANTS / DOMAIN DATA
# ─────────────────────────────────────────────
TRAINING_TYPES = {
    "SIM-BFM": {"color": "#1f77b4", "hours": 2,   "prereq": None,       "desc": "Basic Fighter Maneuvers (Simulator)"},
    "SIM-AAR": {"color": "#ff7f0e", "hours": 2,   "prereq": "SIM-BFM",  "desc": "Air-to-Air Refueling (Simulator)"},
    "SIM-STR": {"color": "#2ca02c", "hours": 3,   "prereq": None,       "desc": "Strike Mission (Simulator)"},
    "FLT-SOL": {"color": "#d62728", "hours": 1.5, "prereq": "SIM-BFM",  "desc": "Solo Flight"},
    "FLT-FOR": {"color": "#9467bd", "hours": 2,   "prereq": "FLT-SOL",  "desc": "Formation Flying"},
    "FLT-NVG": {"color": "#8c564b", "hours": 2,   "prereq": "FLT-SOL",  "desc": "Night Vision Goggle Flight", "night_only": True},
    "GND-WPN": {"color": "#e377c2", "hours": 4,   "prereq": None,       "desc": "Ground Weapons Systems"},
    "GND-SYS": {"color": "#7f7f7f", "hours": 3,   "prereq": None,       "desc": "Aircraft Systems Knowledge"},
    "EVAL":    {"color": "#bcbd22", "hours": 2,   "prereq": "FLT-FOR",  "desc": "Evaluation / Check Ride"},
}

QUALIFICATIONS = ["Phase 1 – Ground", "Phase 2 – Simulator", "Phase 3 – Flight", "Phase 4 – Advanced", "Mission Qualified"]
SQUADRONS      = ["VFA-101", "VFA-125", "VMFAT-501", "VMFAT-502"]
INSTRUCTORS    = ["Maj. Carter", "Capt. Rhodes", "Lt. Col. Torres", "Maj. Singh", "Capt. Kim"]
SIMULATORS     = ["SIM-1 (AECM)", "SIM-2 (AECM)", "SIM-3 (Full Mission)", "SIM-4 (Weapons)"]
AIRCRAFT       = ["F-35A #001", "F-35A #002", "F-35B #003", "F-35C #004"]

# ─────────────────────────────────────────────
# EXCEL LOADING
# ─────────────────────────────────────────────
def load_pilots_from_excel(file_bytes):
    """
    Parse VMFAT-502 Excel format into pilot dicts.

    Expected layout (from screenshot):
      Row 0 (or near top): title / blank
      Row N-2: Warning threshold labels (yellow)
      Row N-1: Alert threshold labels  (red)
      Row N:   Column headers — Name, EP Sim, Last Flight, ...
      Row N+1+: Pilot data rows
                Section-header rows (e.g. "Permanent") have a name but no numeric data.

    Cell values may be formatted as "1/26/2026 / 36" (date / numeric) — we extract
    the numeric part where possible, or parse dates.
    """
    GROUP_KEYWORDS = {
        "permanent", "student", "students", "attached", "tdy", "fnaeb",
        "iac", "instr", "instructor", "instructors", "tac demo",
        "on orders", "transient", "party"
    }

    def extract_numeric(val):
        """
        Handle cells like '1/26/2026 / 36' → 36.0
        or plain numbers, or dates.
        Returns float or None.
        """
        if val is None:
            return None
        s = str(val).strip()
        if s in ("", "nan", "NaN", "-", "--"):
            return None
        # If it contains ' / ' treat part after last slash as the number
        if " / " in s:
            parts = s.split(" / ")
            # Try rightmost part first
            for part in reversed(parts):
                try:
                    return float(part.strip())
                except ValueError:
                    pass
        # Plain float/int
        try:
            return float(s)
        except ValueError:
            pass
        # Excel date serial (numeric type stored as float by openpyxl)
        try:
            f = float(s)
            return f
        except Exception:
            pass
        return None

    def extract_days_ago(val):
        """
        For 'Last Flight' style cells: '2/18/2026 / 13' → 13 (days ago).
        Falls back to extract_numeric.
        """
        return extract_numeric(val)

    try:
        xl = pd.ExcelFile(io.BytesIO(file_bytes))

        # Pick the best sheet
        sheet = xl.sheet_names[0]
        for s in xl.sheet_names:
            if any(kw in s.lower() for kw in ["pilot", "502", "f-35", "hotboard", "student", "currency"]):
                sheet = s
                break

        df = xl.parse(sheet, header=None)

        # ── Find the header row: must contain "Name" AND at least 3 other known col names ──
        known_cols = {c.lower() for c in EXCEL_PILOT_COLUMNS}
        header_row = None
        for i, row in df.iterrows():
            vals = [str(v).strip().lower() for v in row.values]
            matches = sum(1 for v in vals if v in known_cols)
            if "name" in vals and matches >= 3:
                header_row = i
                break
        # Fallback: any row with "name"
        if header_row is None:
            for i, row in df.iterrows():
                vals = [str(v).strip().lower() for v in row.values]
                if "name" in vals:
                    header_row = i
                    break

        if header_row is None:
            st.warning("⚠️ Could not find header row (Name, EP Sim, ...) in Excel.")
            return None

        # ── Build column names from header row ──
        raw_headers = list(df.iloc[header_row])
        col_names = []
        seen = {}
        for v in raw_headers:
            s = str(v).strip()
            s = "" if s.lower() in ("nan", "") else s
            if s in seen:
                seen[s] += 1
                s = f"{s}_{seen[s]}"
            else:
                seen[s] = 0
            col_names.append(s)
        df.columns = col_names
        df = df.iloc[header_row + 1:].reset_index(drop=True)

        # ── Identify Name column ──
        name_col = next((c for c in df.columns if c.lower() == "name"), None)
        if name_col is None:
            st.warning("⚠️ No 'Name' column found after header row.")
            return None

        # ── Map EXCEL_PILOT_COLUMNS to actual df columns (fuzzy) ──
        col_map = {}
        for target in EXCEL_PILOT_COLUMNS:
            if target == "Name":
                col_map[target] = name_col
                continue
            # Exact match first
            if target in df.columns:
                col_map[target] = target
                continue
            # Case-insensitive
            for c in df.columns:
                if c.lower() == target.lower():
                    col_map[target] = c
                    break

        currency_cols = [t for t in EXCEL_PILOT_COLUMNS if t != "Name" and t in col_map]

        def is_section_header(row, name):
            """True if row is a group/category label, not a real pilot."""
            nl = name.lower()
            # Direct keyword match
            if nl in GROUP_KEYWORDS:
                return True
            for kw in GROUP_KEYWORDS:
                if kw in nl:
                    return True
            # No numeric data anywhere in row → label row
            has_data = any(
                extract_numeric(row.get(col_map.get(c, c))) is not None
                for c in currency_cols
            )
            return not has_data

        pilots = []
        current_group = "Permanent"

        for _, row in df.iterrows():
            raw_name = str(row.get(name_col, "")).strip()
            if not raw_name or raw_name.lower() in ("nan", ""):
                continue

            if is_section_header(row, raw_name):
                current_group = raw_name
                continue

            # Build a nice callsign: last meaningful word (usually last name initial)
            parts = [p for p in raw_name.replace(",", " ").replace(".", " ").split() if p]
            callsign = parts[-1][:8].upper() if parts else raw_name[:6].upper()

            currency = {}
            for col in currency_cols:
                actual_col = col_map.get(col, col)
                currency[col] = extract_numeric(row.get(actual_col))

            pilot = {
                "id":          f"P{len(pilots):03d}",
                "name":        raw_name,
                "callsign":    callsign,
                "squadron":    "VMFAT-502",
                "phase":       current_group,
                "group":       current_group,
                "total_hours": currency.get("Career F-35") or 0,
                "sim_hours":   currency.get("FY Sim") or 0,
                "currency":    currency,
            }
            pilots.append(pilot)

        if not pilots:
            st.warning("⚠️ No pilot rows found. Check that the file matches the VMFAT-502 format.")
            return None

        return pilots

    except Exception as e:
        import traceback
        st.warning(f"⚠️ Could not parse Excel: {e}")
        st.code(traceback.format_exc())
        return None


def _safe_float(val):
    try:
        return float(val) if val is not None and str(val).strip() not in ["", "nan", "NaN"] else None
    except:
        return None


def sample_pilots():
    """Generate sample pilots with mock currency data."""
    pilots = []
    names_cs = [
        ("Lt. Adams",   "VIPER"),  ("Lt. Brooks",  "GHOST"),
        ("Capt. Chen",  "FALCON"), ("Lt. Davis",   "NOVA"),
        ("Lt. Evans",   "ECHO"),   ("Capt. Foster","TITAN"),
    ]
    for i, (name, cs) in enumerate(names_cs):
        pilots.append({
            "id": f"P{i:03d}",
            "name": name,
            "callsign": cs,
            "squadron": random.choice(SQUADRONS),
            "phase": QUALIFICATIONS[i % len(QUALIFICATIONS)],
            "total_hours": round(random.uniform(10, 250), 1),
            "sim_hours":   round(random.uniform(5, 80), 1),
            "currency": {
                "EP Sim":           random.randint(5, 90),
                "Last Flight":      random.randint(1, 60),
                "Last Night Flight":random.randint(10, 150),
                "30":               round(random.uniform(0, 20), 1),
                "Sim 30":           round(random.uniform(0, 15), 1),
                "60":               round(random.uniform(5, 40), 1),
                "90":               round(random.uniform(10, 60), 1),
                "FY":               round(random.uniform(20, 180), 1),
                "FY Sim":           round(random.uniform(10, 80), 1),
                "FY Instr":         round(random.uniform(0, 25), 1),
                "FY Night":         round(random.uniform(0, 20), 1),
                "Career F-35":      round(random.uniform(50, 500), 1),
                "FTR time":         round(random.uniform(10, 100), 1),
                "All T/M/S":        round(random.uniform(50, 300), 1),
                "Last VL":          random.randint(5, 90),
                "Last Night VL":    random.randint(5, 90),
                "PFO":              random.randint(0, 12),
                "Last AAR (Day)":   random.randint(5, 90),
                "Last AAR (Night)": random.randint(5, 90),
            }
        })
    return pilots


# ─────────────────────────────────────────────
# THRESHOLD HELPERS
# ─────────────────────────────────────────────
def get_cell_status(col, value):
    """Return 'alert', 'warning', or 'ok' for a currency value."""
    if value is None or col not in CURRENCY_THRESHOLDS:
        return "ok"
    thresholds = CURRENCY_THRESHOLDS[col]

    def check(rule, val):
        if rule is None:
            return False
        op, threshold = rule
        if op == "lte":
            return val <= threshold
        if op == "gte":
            return val >= threshold
        return False

    if check(thresholds.get("alert"), value):
        return "alert"
    if check(thresholds.get("warning"), value):
        return "warning"
    return "ok"


def pilot_overall_status(pilot):
    currency = pilot.get("currency", {})
    statuses = [get_cell_status(col, currency.get(col)) for col in CURRENCY_THRESHOLDS]
    if "alert" in statuses:
        return "alert"
    if "warning" in statuses:
        return "warning"
    return "ok"


STATUS_COLORS = {"alert": "#d62728", "warning": "#FFD700", "ok": "#2ca02c"}
STATUS_LABELS = {"alert": "🔴 ALERT", "warning": "🟡 WARNING", "ok": "🟢 CURRENT"}


# ─────────────────────────────────────────────
# SESSION STATE INIT
# ─────────────────────────────────────────────
def init_state():
    if "pilots" not in st.session_state:
        st.session_state.pilots = sample_pilots()
    if "excel_loaded" not in st.session_state:
        st.session_state.excel_loaded = False
    if "sessions" not in st.session_state:
        st.session_state.sessions = _generate_sample_sessions()
    if "constraints" not in st.session_state:
        st.session_state.constraints = {
            "max_flight_hours_per_day":    4,
            "max_sim_hours_per_day":       6,
            "min_rest_hours":             12,
            "max_consecutive_flight_days": 5,
            "instructor_max_daily_hours":  8,
        }
    if "opt_result" not in st.session_state:
        st.session_state.opt_result = None
    if "opt_cache_key" not in st.session_state:
        st.session_state.opt_cache_key = None
    if "ranges" not in st.session_state:
        # Each range: {id, name, date, start_hour, end_hour, slots, notes}
        st.session_state.ranges = []
    if "paused_pilots" not in st.session_state:
        # {pilot_id: {reason, paused_on, resume_on (None=indefinite), original_sessions}}
        st.session_state.paused_pilots = {}


def _generate_sample_sessions():
    sessions, sid, base = [], 1, date.today()
    for pilot in ["P000", "P001", "P002", "P003", "P004", "P005"]:
        offset = 0
        for ttype in random.sample(list(TRAINING_TYPES.keys()), k=4):
            info  = TRAINING_TYPES[ttype]
            day   = base + timedelta(days=offset)
            start = datetime(day.year, day.month, day.day, random.choice([7, 8, 9, 10, 13, 14]))
            end   = start + timedelta(hours=info["hours"])
            sessions.append({
                "id": f"S{sid:04d}", "pilot_id": pilot,
                "training_type": ttype, "start": start, "end": end,
                "instructor": random.choice(INSTRUCTORS),
                "resource": random.choice(SIMULATORS if ttype.startswith("SIM") else AIRCRAFT),
                "status": random.choice(["Scheduled", "Scheduled", "Completed", "Pending"]),
                "notes": "",
            })
            sid += 1
            offset += random.randint(1, 3)
    return sessions


init_state()


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def pilot_name(pid):
    for p in st.session_state.pilots:
        if p["id"] == pid:
            return f"{p['name']} ({p['callsign']})"
    return pid


def sessions_df():
    if not st.session_state.sessions:
        return pd.DataFrame()
    df = pd.DataFrame(st.session_state.sessions)
    df["pilot_name"]  = df["pilot_id"].apply(pilot_name)
    df["duration_h"]  = (df["end"] - df["start"]).dt.total_seconds() / 3600
    return df


def conflict_check(new_sess):
    conflicts = []
    pid, ns, ne = new_sess["pilot_id"], new_sess["start"], new_sess["end"]
    res, inst   = new_sess["resource"], new_sess["instructor"]
    for s in st.session_state.sessions:
        if s["id"] == new_sess.get("id"):
            continue
        ss, se  = s["start"], s["end"]
        overlap = ns < se and ne > ss
        if overlap and s["pilot_id"] == pid:
            conflicts.append(f"⚠️ Pilot already scheduled: {s['training_type']} ({ss.strftime('%H:%M')}–{se.strftime('%H:%M')})")
        if overlap and s["resource"] == res:
            conflicts.append(f"⚠️ Resource conflict: {res} booked {ss.strftime('%H:%M')}–{se.strftime('%H:%M')}")
        if overlap and s["instructor"] == inst:
            conflicts.append(f"⚠️ Instructor conflict: {inst} booked {ss.strftime('%H:%M')}–{se.strftime('%H:%M')}")
    prereq = TRAINING_TYPES[new_sess["training_type"]]["prereq"]
    if prereq:
        done = [s["training_type"] for s in st.session_state.sessions
                if s["pilot_id"] == pid and s["status"] == "Completed"]
        if prereq not in done:
            conflicts.append(f"⚠️ Prerequisite not met: '{prereq}' must be completed first")
    return conflicts


# ─────────────────────────────────────────────
# OPT CACHE HELPERS
# ─────────────────────────────────────────────
def compute_opt_cache_key(planning_days, start_date, constraints):
    """Hash of all inputs that affect the optimal solution.
    If this matches the last run, no need to re-solve."""
    import hashlib, json
    # Booked sessions per pilot (sorted for stability)
    booked_summary = {
        p["id"]: sorted(
            s["training_type"] for s in st.session_state.sessions
            if s["pilot_id"] == p["id"] and
               s["status"] in ("Scheduled","Completed","Paused","Scheduled (Optimized)")
        )
        for p in st.session_state.pilots
    }
    payload = {
        "planning_days":  planning_days,
        "start_date":     str(start_date),
        "constraints":    constraints,
        "paused":         sorted(st.session_state.paused_pilots.keys()),
        "ranges":         [(r["date"].isoformat(), r["start_hour"], r["end_hour"])
                           for r in st.session_state.ranges],
        "booked":         booked_summary,
        "pilots":         [p["id"] for p in st.session_state.pilots],
    }
    return hashlib.md5(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()


# ─────────────────────────────────────────────
# RANGE HELPERS
# ─────────────────────────────────────────────
def range_open_on(day):
    """Return list of (start_hour, end_hour) windows for ranges open on `day`."""
    return [(r["start_hour"], r["end_hour"])
            for r in st.session_state.ranges if r["date"] == day]


def flight_day_allowed(cal_day):
    """True if any range window is open on cal_day (or no ranges defined)."""
    if not st.session_state.ranges:
        return True
    return len(range_open_on(cal_day)) > 0


def pick_start_hour(cal_day, tt):
    """Pick a display start hour for a session on cal_day."""
    is_night = TRAINING_TYPES[tt].get("night_only", False)
    if is_night:
        return 20
    windows = range_open_on(cal_day)
    if windows:
        return windows[0][0]   # open at range start
    return 8                   # default day start


# ─────────────────────────────────────────────
# PYOMO OPTIMIZER  — day-level formulation
# ─────────────────────────────────────────────
# Variable reduction vs old version:
#   OLD: pilots × ttypes × days × 11 slots  (e.g. 6×9×21×11 = 12,474 binaries)
#   NEW: pilots × ttypes × days             (e.g. 6×9×21   =  1,134 binaries)
#   Prereq constraints: OLD O(d²×s²), NEW O(d) per pair — ~100× fewer rows
#
# Formulation:
#   x[p,t,d] ∈ {0,1}   — pilot p does training t on day d
#   D[p,t]   ∈ [0,D]   — day index when pilot p does training t (continuous)
#   C[p]     ∈ [0,D]   — completion day for pilot p
#   makespan ∈ [0,D]   — latest completion across all pilots
#
#   Objective: min makespan − ε·Σx   (compress + maximise coverage)
#
#   D[p,t] = Σ_d  d·x[p,t,d]          (day-index linking)
#   C[p]  >= D[p,t]  ∀t                (completion = latest event)
#   makespan >= C[p]  ∀p
#   Σ_d x[p,t,d] <= 1  ∀p,t           (each event at most once)
#   Σ_t x[p,t,d] <= cap_per_day  ∀p,d (daily session cap)
#   Σ_p x[p,t,d] <= resource_cap ∀t,d (resource cap per day)
#   D[p,t2] >= D[p,t1] + 1  ∀ prereq pairs  (ordering — compact!)
#   x[p,t,d] = 0  for blocked days    (range / night constraints)
# ─────────────────────────────────────────────
def run_pyomo_optimization(pilots, planning_days, start_date, constraints,
                           time_limit_sec=5):
    try:
        from pyomo.environ import (
            ConcreteModel, Var, Binary, Objective,
            SolverFactory, value, minimize, ConstraintList, NonNegativeReals
        )
        from pyomo.opt import SolverStatus, TerminationCondition
    except ImportError:
        return None, "Pyomo not installed. Run: pip install pyomo", 0, {}

    # ── Setup ─────────────────────────────────────────────────────────────
    paused_ids    = set(st.session_state.paused_pilots.keys())
    active_pilots = [p for p in pilots if p["id"] not in paused_ids]
    if not active_pilots:
        return None, "All pilots are currently paused.", 0, {}

    pilot_ids  = [p["id"] for p in active_pilots]
    all_ttypes = list(TRAINING_TYPES.keys())
    days       = list(range(planning_days))
    prereq_map = {tt: info.get("prereq") for tt, info in TRAINING_TYPES.items()}
    start_d    = start_date if isinstance(start_date, date) else start_date

    # ── Build per-pilot set of already-booked training types ──────────────
    # A training type is "done" if it has a Scheduled, Completed, or Paused
    # session already in the schedule for that pilot.
    already_booked = {}   # pid -> set of training types already scheduled
    for pid in pilot_ids:
        booked = set()
        for s in st.session_state.sessions:
            if (s["pilot_id"] == pid and
                    s["status"] in ("Scheduled", "Completed", "Paused",
                                    "Scheduled (Optimized)")):
                booked.add(s["training_type"])
        already_booked[pid] = booked

    # Only optimise training types not yet booked for at least one pilot
    # (we still keep all_ttypes for prereq reference but x vars only for needed)
    ttypes = all_ttypes  # full list needed for prereq graph

    # Pre-compute blocked days per training category
    # Flight events blocked if no range open AND ranges are defined
    use_ranges = len(st.session_state.ranges) > 0
    flight_blocked = set()   # day indices blocked for flight
    night_blocked  = set()   # day indices blocked for night (not used, handled via D)
    if use_ranges:
        for d in days:
            cal_day = start_d + timedelta(days=d)
            if not flight_day_allowed(cal_day):
                flight_blocked.add(d)

    # Max sessions per pilot per day (derived from hour limits)
    max_sess_per_day = max(1, int(
        constraints["max_flight_hours_per_day"] +
        constraints["max_sim_hours_per_day"]
    ) // 2)

    M = planning_days  # big-M constant

    # ── Build model ───────────────────────────────────────────────────────
    m = ConcreteModel()

    # Binary: x[p,t,d] = 1 if pilot p does training t on day d
    # Only create variables for training types not already booked per pilot
    x_indices = [
        (pid, tt, d)
        for pid in pilot_ids
        for tt in ttypes
        for d in days
        if tt not in already_booked[pid]
    ]
    m.x = Var(x_indices, domain=Binary, initialize=0)

    def get_x(pid, tt, d):
        """Return x variable or 0 if already booked / not in model."""
        if tt in already_booked.get(pid, set()):
            return 0
        try:
            return m.x[pid, tt, d]
        except KeyError:
            return 0

    # Continuous day-index: D[p,t] = day on which pilot p does training t
    m.D = Var(
        [(pid, tt) for pid in pilot_ids for tt in ttypes],
        domain=NonNegativeReals, bounds=(0, M)
    )

    # Completion day per pilot
    m.C = Var(pilot_ids, domain=NonNegativeReals, bounds=(0, M))

    # Makespan
    m.makespan = Var(domain=NonNegativeReals, bounds=(0, M))

    m.cons = ConstraintList()

    # ── Objective ─────────────────────────────────────────────────────────
    total = sum(get_x(pid, tt, d)
                for pid in pilot_ids for tt in ttypes for d in days)
    m.obj = Objective(expr=m.makespan - 0.001 * total, sense=minimize)

    # ── Helper: safe constraint addition ─────────────────────────────────
    def has_var(terms):
        """True if any term is a Pyomo object (not plain int/float)."""
        return any(not isinstance(t, (int, float)) for t in terms)

    def safe_add(expr):
        """Add constraint only if it is not a trivial Python bool."""
        try:
            if isinstance(expr, bool):
                return
            m.cons.add(expr)
        except Exception:
            pass

    # ── Constraints ───────────────────────────────────────────────────────
    for pid in pilot_ids:
        booked = already_booked.get(pid, set())
        # training types still needed for this pilot
        needed = [tt for tt in ttypes if tt not in booked]

        for tt in ttypes:
            terms = [get_x(pid, tt, d) for d in days]
            # skip if all terms are plain 0 (already booked)
            if not has_var(terms):
                continue

            # Each training type at most once
            safe_add(sum(terms) <= 1)

            # Link D[p,t] = weighted sum of day indices
            safe_add(m.D[pid, tt] == sum(d * get_x(pid, tt, d) for d in days))

            # C[p] >= D[p,t]
            safe_add(m.C[pid] >= m.D[pid, tt])

        # makespan >= C[p]
        safe_add(m.makespan >= m.C[pid])

        # Daily session cap per pilot (only if pilot has anything to schedule)
        if needed:
            for d in days:
                terms = [get_x(pid, tt, d) for tt in ttypes]
                if has_var(terms):
                    safe_add(sum(terms) <= max_sess_per_day)

        # Daily flight hour cap
        for d in days:
            flt_terms = [TRAINING_TYPES[tt]["hours"] * get_x(pid, tt, d)
                         for tt in ttypes if tt.startswith("FLT")]
            if has_var(flt_terms):
                safe_add(sum(flt_terms) <= constraints["max_flight_hours_per_day"])

        # Daily sim hour cap
        for d in days:
            sim_terms = [TRAINING_TYPES[tt]["hours"] * get_x(pid, tt, d)
                         for tt in ttypes if tt.startswith("SIM")]
            if has_var(sim_terms):
                safe_add(sum(sim_terms) <= constraints["max_sim_hours_per_day"])

        # ── Prerequisite ordering ─────────────────────────────────────────
        for tt2, tt1 in prereq_map.items():
            if tt1 is None:
                continue
            # Only enforce if tt2 is not already booked
            if tt2 in booked:
                continue
            done_tt2_terms = [get_x(pid, tt2, d) for d in days]
            if not has_var(done_tt2_terms):
                continue
            done_tt2 = sum(done_tt2_terms)
            safe_add(m.D[pid, tt2] >= m.D[pid, tt1] + 1 - M * (1 - done_tt2))

        # ── Range: block flight events on closed days ─────────────────────
        for tt in ttypes:
            if tt.startswith("FLT") and tt not in booked:
                for d in flight_blocked:
                    v = get_x(pid, tt, d)
                    if v != 0:
                        safe_add(v == 0)

    # ── Resource cap per day ──────────────────────────────────────────────
    for d in days:
        sim_terms = [get_x(pid, tt, d)
                     for pid in pilot_ids
                     for tt in ttypes if tt.startswith("SIM")]
        if has_var(sim_terms):
            safe_add(sum(sim_terms) <= len(SIMULATORS))

        flt_cap = 0 if d in flight_blocked else len(AIRCRAFT)
        flt_terms = [get_x(pid, tt, d)
                     for pid in pilot_ids
                     for tt in ttypes if tt.startswith("FLT")]
        if has_var(flt_terms):
            safe_add(sum(flt_terms) <= flt_cap)

    # ── Instructor daily load ─────────────────────────────────────────────
    for d in days:
        inst_terms = [TRAINING_TYPES[tt]["hours"] * get_x(pid, tt, d)
                      for pid in pilot_ids for tt in ttypes]
        if has_var(inst_terms):
            safe_add(sum(inst_terms) <=
                     constraints["instructor_max_daily_hours"] * len(INSTRUCTORS))

    # ── Solve with time limit ─────────────────────────────────────────────
    solved      = False
    used_solver = None
    for solver_name in ["cbc", "glpk"]:
        solver = SolverFactory(solver_name)
        if not solver.available():
            continue
        # Set time limit
        opts = {}
        if solver_name == "cbc":
            opts = {"seconds": time_limit_sec}
        elif solver_name == "glpk":
            opts = {"tmlim": time_limit_sec}
        result = solver.solve(m, tee=False, options=opts)
        used_solver = solver_name
        if result.solver.status in (SolverStatus.ok, SolverStatus.aborted):
            solved = True
            break

    if not solved:
        return (None,
                "No solver found. Install: `pip install pyomo` then "
                "`sudo apt install glpk-utils` (or `brew install glpk`)",
                0, {})

    # ── Extract solution ──────────────────────────────────────────────────
    # Step 1: collect (pid, tt, d) triples from solver
    raw = []
    pilot_completions = {pid: 0 for pid in pilot_ids}
    for pid in pilot_ids:
        for tt in ttypes:
            for d in days:
                try:
                    if tt not in already_booked.get(pid, set()) and value(get_x(pid, tt, d)) > 0.5:
                        raw.append((pid, tt, d))
                        pilot_completions[pid] = max(pilot_completions[pid], d + 1)
                except Exception:
                    pass

    # Step 2: assign non-overlapping start times per (pilot, day)
    # Track next available hour per (pid, day) separately for day/night sessions
    # Day window: 07:00–17:00  |  Night window: 18:00–23:00
    DAY_START   = 7
    NIGHT_START = 18
    next_hour = {}   # (pid, d, "day"|"night") -> next available hour

    def get_next_hour(pid, d, is_night):
        key = (pid, d, "night" if is_night else "day")
        default = NIGHT_START if is_night else DAY_START
        return next_hour.get(key, default)

    def advance_hour(pid, d, is_night, duration):
        key = (pid, d, "night" if is_night else "day")
        current = get_next_hour(pid, d, is_night)
        next_hour[key] = current + duration

    # Also account for already-existing sessions on those days so new sessions
    # don't overlap with anything already on the schedule
    for s in st.session_state.sessions:
        existing_pid = s["pilot_id"]
        if existing_pid not in pilot_ids:
            continue
        s_day = (s["start"].date() - start_d).days
        if 0 <= s_day < planning_days:
            is_night = s["start"].hour >= NIGHT_START
            key = (existing_pid, s_day, "night" if is_night else "day")
            end_hour = s["end"].hour + (1 if s["end"].minute > 0 else 0)
            next_hour[key] = max(next_hour.get(key, NIGHT_START if is_night else DAY_START), end_hour)

    # Sort by (pid, day) so we fill sequentially
    raw.sort(key=lambda x: (x[0], x[2]))

    scheduled = []
    for pid, tt, d in raw:
        cal_day  = start_d + timedelta(days=d)
        duration = TRAINING_TYPES[tt]["hours"]
        is_night = TRAINING_TYPES[tt].get("night_only", False)

        # Determine start from range window or default
        window_start = NIGHT_START if is_night else DAY_START
        if not is_night:
            windows = range_open_on(cal_day)
            if windows:
                window_start = windows[0][0]

        # Ensure we start no earlier than the window
        sh = max(get_next_hour(pid, d, is_night), window_start)

        # Clamp to avoid running past end of day (22:00 latest end)
        if sh + duration > 22:
            sh = max(window_start, 22 - duration)

        start_dt = datetime(cal_day.year, cal_day.month, cal_day.day, int(sh))
        end_dt   = start_dt + timedelta(hours=duration)

        advance_hour(pid, d, is_night, duration)

        scheduled.append({
            "id":            f"OPT-{pid}-{tt}-D{d}",
            "pilot_id":      pid,
            "training_type": tt,
            "start":         start_dt,
            "end":           end_dt,
            "instructor":    random.choice(INSTRUCTORS),
            "resource":      (random.choice(SIMULATORS) if tt.startswith("SIM")
                              else random.choice(AIRCRAFT)),
            "status":        "Scheduled (Optimized)",
            "notes":         f"Optimised | day {d+1} | solver: {used_solver}",
        })

    makespan_val = max(pilot_completions.values()) if pilot_completions else 0
    return scheduled, None, makespan_val, pilot_completions


# ─────────────────────────────────────────────
# SIDEBAR + FILE UPLOAD
# ─────────────────────────────────────────────
with st.sidebar:
    st.title("✈️ F-35 Training Scheduler")
    st.divider()

    st.subheader("📂 Pilot Data Source")
    uploaded = st.file_uploader("Upload VMFAT-502 Excel (.xlsx)", type=["xlsx", "xls"])
    if uploaded:
        pilots_from_excel = load_pilots_from_excel(uploaded.read())
        if pilots_from_excel:
            st.session_state.pilots = pilots_from_excel
            st.session_state.excel_loaded = True
            st.success(f"✅ Loaded {len(pilots_from_excel)} pilots from Excel")
    
    if st.session_state.excel_loaded:
        st.caption("🟢 Using Excel data")
        if st.button("Reset to Sample Data"):
            st.session_state.pilots = sample_pilots()
            st.session_state.excel_loaded = False
            st.rerun()
    else:
        st.caption("🔵 Using sample data — upload Excel to load real pilots")

    st.divider()
    page = st.radio("Navigation", [
        "📅 Schedule",
        "➕ Add Session",
        "👨‍✈️ Pilots",
        "📊 Currency Status",
        "⏸️ Pause Training",
        "🤖 Optimize (Pyomo)",
        "📊 Analytics",
        "⚙️ Constraints",
    ])

# ─────────────────────────────────────────────
# SHARED SCHEDULE CSS
# ─────────────────────────────────────────────
SCHED_CSS = """
<style>
.sched-grid { width:100%; border-collapse:collapse; font-family:'Courier New',monospace; font-size:12px; }
.sched-grid th {
    background:#1a1a2e; color:#e0e0ff;
    padding:8px 4px; text-align:center;
    border:1px solid #333; white-space:pre-line; line-height:1.3;
}
.sched-grid th.today-hdr    { background:#0f3460; color:#00d4ff; border-bottom:2px solid #00d4ff; }
.sched-grid th.curmonth-hdr { background:#0f3460; color:#00d4ff; }
.sched-grid td {
    border:1px solid #2a2a3e; padding:4px 6px;
    vertical-align:top; min-width:60px;
    background:#0d0d1a;
}
.sched-grid td.pilot-cell {
    background:#12122a; color:#ccd6f6;
    font-weight:bold; font-size:11px;
    min-width:110px; white-space:nowrap; padding:6px 8px;
}
.sched-grid td.today-col    { background:#0a1628; }
.sched-grid td.curmonth-col { background:#0a1628; }
.sched-grid td.dim-col      { background:#08080f; opacity:0.55; }
.sched-grid tr:hover td     { background:#16162e; }
.sched-grid tr:hover td.today-col { background:#0c1f3a; }
.sess-pill {
    display:block; border-radius:4px;
    padding:2px 5px; margin:1px 0;
    font-size:10px; font-weight:bold; color:white;
    white-space:nowrap; cursor:default;
    overflow:hidden; text-overflow:ellipsis;
}
.sess-dot {
    display:inline-block; width:9px; height:9px;
    border-radius:50%; margin:1px 1px 0 0;
    cursor:default; vertical-align:middle;
}
.status-alert   { border-left:3px solid #d62728; }
.status-warning { border-left:3px solid #FFD700; }
.status-ok      { border-left:3px solid #2ca02c; }
.month-day-num  { font-size:10px; color:#7f8c8d; margin-bottom:2px; }
.month-day-num.today-num { color:#00d4ff; font-weight:bold; }
</style>
"""

# ─────────────────────────────────────────────
# PAGE: SCHEDULE  (week / month / year tabs)
# ─────────────────────────────────────────────
if page == "📅 Schedule":
    st.header("📅 Training Schedule")

    # ── shared filters ────────────────────────
    with st.expander("🔍 Filters", expanded=False):
        fc1, fc2 = st.columns(2)
        filter_pilot = fc1.multiselect("Pilot", [p["name"] for p in st.session_state.pilots])
        filter_type  = fc2.multiselect("Training Type", list(TRAINING_TYPES.keys()))

    df = sessions_df()
    TODAY = date.today()

    def sessions_in_range(pilot_id, day_from, day_to):
        """Return sessions for a pilot overlapping [day_from, day_to]."""
        if df.empty:
            return []
        mask = (
            (df["pilot_id"] == pilot_id) &
            (df["start"].dt.date <= day_to) &
            (df["end"].dt.date   >= day_from)
        )
        rows = df[mask]
        if filter_type:
            rows = rows[rows["training_type"].isin(filter_type)]
        return rows.to_dict("records")

    pilots_to_show = st.session_state.pilots
    if filter_pilot:
        pilots_to_show = [p for p in pilots_to_show if p["name"] in filter_pilot]

    def pilot_name_cell(pilot):
        pstatus = pilot_overall_status(pilot)
        pcolor  = STATUS_COLORS[pstatus]
        return (
            f'<td class="pilot-cell status-{pstatus}">'
            f'<span style="color:{pcolor}">●</span> '
            f'{pilot["name"]}<br>'
            f'<span style="color:#7f8c8d;font-size:10px">{pilot["callsign"]}</span>'
            f'</td>'
        )

    def legend_html():
        bits = []
        for tt, info in TRAINING_TYPES.items():
            bits.append(
                f"<span style='background:{info['color']};color:white;"
                f"padding:2px 6px;border-radius:3px;font-size:11px;margin:2px'>{tt}</span>"
            )
        return " ".join(bits)

    # ── tabs ─────────────────────────────────
    tab_week, tab_month, tab_year, tab_range = st.tabs(["📅 Week", "🗓️ Month", "📆 Year", "🎯 Ranges"])

    # ── Range row helper: injects a "Ranges" row into any grid ──────────
    def range_rows_html(days_list):
        """Return HTML <tr> rows for each unique range name across days_list."""
        range_names = list(dict.fromkeys(r["name"] for r in st.session_state.ranges))
        rows = []
        for rname in range_names:
            row = (f'<tr><td class="pilot-cell" style="color:#0f7b6c">'
                   f'🎯 {rname}</td>')
            for d in days_list:
                windows = [(r["start_hour"], r["end_hour"])
                           for r in st.session_state.ranges
                           if r["name"] == rname and r["date"] == d]
                tcls = "today-col" if d == TODAY else ""
                row += f'<td class="{tcls}">'
                for ws_h, we_h in windows:
                    tip = f'{rname}: {ws_h:02d}:00–{we_h:02d}:00 OPEN'
                    row += (f'<div class="sess-pill" style="background:#0f7b6c;border:1px solid #1aaf9a" '
                            f'title="{tip}">🟢 {ws_h:02d}–{we_h:02d}</div>')
                row += '</td>'
            row += '</tr>'
            rows.append(row)
        return rows

    # ══════════════════════════════════════════
    # WEEK TAB
    # ══════════════════════════════════════════
    with tab_week:
        if "week_offset" not in st.session_state:
            st.session_state.week_offset = 0

        nav1, nav2, nav3, nav4 = st.columns([1, 1, 4, 1])
        with nav1:
            if st.button("◀ Prev", key="wprev"):
                st.session_state.week_offset -= 1
        with nav2:
            if st.button("Next ▶", key="wnext"):
                st.session_state.week_offset += 1
        with nav3:
            ws = TODAY + timedelta(weeks=st.session_state.week_offset)
            ws -= timedelta(days=ws.weekday())
            we  = ws + timedelta(days=6)
            st.markdown(f"### {ws.strftime('%b %d')} – {we.strftime('%b %d, %Y')}")
        with nav4:
            if st.button("Today", key="wtoday"):
                st.session_state.week_offset = 0

        days_in_week = [ws + timedelta(days=i) for i in range(7)]

        if not pilots_to_show:
            st.info("No pilots to display.")
        else:
            html = [SCHED_CSS, '<table class="sched-grid"><thead><tr><th>Pilot</th>']
            for d in days_in_week:
                cls = "today-hdr" if d == TODAY else ""
                html.append(f'<th class="{cls}">{d.strftime("%a")}<br>{d.strftime("%b %d")}</th>')
            html.append('</tr></thead><tbody>')

            # Range availability rows
            if st.session_state.ranges:
                for rrow in range_rows_html(days_in_week):
                    html.append(rrow)
                html.append('<tr><td colspan="8" style="height:4px;background:#1a1a2e;padding:0"></td></tr>')

            for pilot in pilots_to_show:
                html.append('<tr>' + pilot_name_cell(pilot))
                for d in days_in_week:
                    tcls     = "today-col" if d == TODAY else ""
                    sessions = sessions_in_range(pilot["id"], d, d)
                    html.append(f'<td class="{tcls}">')
                    for s in sessions:
                        tt    = s["training_type"]
                        color = TRAINING_TYPES.get(tt, {}).get("color", "#555")
                        tip   = f'{tt} | {s["start"].strftime("%H:%M")}–{s["end"].strftime("%H:%M")} | {s["instructor"]} | {s["status"]}'
                        html.append(
                            f'<div class="sess-pill" style="background:{color}" title="{tip}">'
                            f'{tt}<br>'
                            f'<span style="font-weight:normal;font-size:9px">'
                            f'{s["start"].strftime("%H:%M")}–{s["end"].strftime("%H:%M")}'
                            f'</span></div>'
                        )
                    html.append('</td>')
                html.append('</tr>')
            html.append('</tbody></table>')
            st.markdown("".join(html), unsafe_allow_html=True)

            st.divider()
            st.markdown(legend_html(), unsafe_allow_html=True)

            st.divider()
            st.subheader("Session Details — This Week")
            if not df.empty:
                wdf = df[(df["start"].dt.date >= ws) & (df["start"].dt.date <= we)]
                if filter_pilot:
                    wdf = wdf[wdf["pilot_name"].apply(lambda x: any(p in x for p in filter_pilot))]
                if filter_type:
                    wdf = wdf[wdf["training_type"].isin(filter_type)]
                if not wdf.empty:
                    wdf_sorted = wdf[["id","pilot_name","training_type","start","end","duration_h","instructor","resource","status"]].sort_values("start")
                    st.dataframe(wdf_sorted.drop(columns=["id"]), use_container_width=True, hide_index=True)
                    st.caption("To delete a session, use **➕ Add Session → 🗑️ Delete Sessions** tab.")
                else:
                    st.info("No sessions this week.")

    # ══════════════════════════════════════════
    # MONTH TAB
    # ══════════════════════════════════════════
    with tab_month:
        if "month_offset" not in st.session_state:
            st.session_state.month_offset = 0

        nav1, nav2, nav3, nav4 = st.columns([1, 1, 4, 1])
        with nav1:
            if st.button("◀ Prev", key="mprev"):
                st.session_state.month_offset -= 1
        with nav2:
            if st.button("Next ▶", key="mnext"):
                st.session_state.month_offset += 1
        with nav3:
            # Compute target month
            ref = TODAY.replace(day=1)
            mo  = st.session_state.month_offset
            yr, mn = divmod(ref.month - 1 + mo, 12)
            cur_month_date = ref.replace(year=ref.year + yr, month=mn + 1)
            st.markdown(f"### {cur_month_date.strftime('%B %Y')}")
        with nav4:
            if st.button("Today", key="mtoday"):
                st.session_state.month_offset = 0

        # Build weeks in the month
        import calendar
        first_day = cur_month_date
        last_day  = cur_month_date.replace(day=calendar.monthrange(cur_month_date.year, cur_month_date.month)[1])
        # pad to Monday start
        grid_start = first_day - timedelta(days=first_day.weekday())
        grid_end   = last_day  + timedelta(days=(6 - last_day.weekday()))
        all_days   = []
        d = grid_start
        while d <= grid_end:
            all_days.append(d)
            d += timedelta(days=1)
        weeks = [all_days[i:i+7] for i in range(0, len(all_days), 7)]

        if not pilots_to_show:
            st.info("No pilots to display.")
        else:
            # Month view: columns = weeks (Mon–Sun), rows = pilots
            # Each cell shows dots for each session
            week_labels = [f"{w[0].strftime('%b %d')}–{w[6].strftime('%b %d')}" for w in weeks]

            html = [SCHED_CSS, '<table class="sched-grid"><thead><tr><th>Pilot</th>']
            for wk in weeks:
                # highlight if current week falls in this range
                in_cur = any(d == TODAY for d in wk)
                cls = "today-hdr" if in_cur else ""
                html.append(f'<th class="{cls}">{wk[0].strftime("%b %d")}<br>–{wk[6].strftime("%b %d")}</th>')
            html.append('</tr></thead><tbody>')

            # Range rows — show a dot per day that has a window in this week
            if st.session_state.ranges:
                range_names = list(dict.fromkeys(r["name"] for r in st.session_state.ranges))
                for rname in range_names:
                    html.append(f'<tr><td class="pilot-cell" style="color:#0f7b6c">🎯 {rname}</td>')
                    for wk in weeks:
                        in_cur   = any(d == TODAY for d in wk)
                        in_month = any(d.month == cur_month_date.month for d in wk)
                        tcls     = "today-col" if in_cur else ("curmonth-col" if in_month else "dim-col")
                        open_days = [d for d in wk
                                     if any(r["name"] == rname and r["date"] == d
                                            for r in st.session_state.ranges)]
                        html.append(f'<td class="{tcls}">')
                        for od in open_days:
                            wins = [(r["start_hour"], r["end_hour"])
                                    for r in st.session_state.ranges
                                    if r["name"] == rname and r["date"] == od]
                            for ws_h, we_h in wins:
                                html.append(
                                    f'<div class="month-day-num">{od.day}</div>'
                                    f'<span class="sess-dot" style="background:#0f7b6c" '
                                    f'title="{rname} {ws_h:02d}:00–{we_h:02d}:00"></span>'
                                )
                        html.append('</td>')
                    html.append('</tr>')
                html.append('<tr><td colspan="20" style="height:4px;background:#1a1a2e;padding:0"></td></tr>')

            for pilot in pilots_to_show:
                html.append('<tr>' + pilot_name_cell(pilot))
                for wk in weeks:
                    in_cur  = any(d == TODAY for d in wk)
                    in_month= any(d.month == cur_month_date.month for d in wk)
                    tcls    = "today-col" if in_cur else ("curmonth-col" if in_month else "dim-col")
                    sessions = sessions_in_range(pilot["id"], wk[0], wk[6])
                    html.append(f'<td class="{tcls}" style="min-width:80px">')
                    # Show compact: day-number + colored dots
                    # Group sessions by day
                    from collections import defaultdict
                    by_day = defaultdict(list)
                    for s in sessions:
                        by_day[s["start"].date()].append(s)
                    for d in wk:
                        day_sess = by_day.get(d, [])
                        if day_sess:
                            num_cls = "today-num" if d == TODAY else ""
                            html.append(f'<div class="month-day-num {num_cls}">{d.day}</div>')
                            for s in day_sess:
                                tt    = s["training_type"]
                                color = TRAINING_TYPES.get(tt, {}).get("color", "#555")
                                tip   = f'{tt} | {s["start"].strftime("%H:%M")} | {s["instructor"]}'
                                html.append(f'<span class="sess-dot" style="background:{color}" title="{tip}"></span>')
                    html.append('</td>')
                html.append('</tr>')
            html.append('</tbody></table>')
            st.markdown("".join(html), unsafe_allow_html=True)

            st.divider()
            st.markdown(legend_html(), unsafe_allow_html=True)

            st.divider()
            st.subheader(f"Session Details — {cur_month_date.strftime('%B %Y')}")
            if not df.empty:
                mdf = df[
                    (df["start"].dt.year  == cur_month_date.year) &
                    (df["start"].dt.month == cur_month_date.month)
                ]
                if filter_pilot:
                    mdf = mdf[mdf["pilot_name"].apply(lambda x: any(p in x for p in filter_pilot))]
                if filter_type:
                    mdf = mdf[mdf["training_type"].isin(filter_type)]
                if not mdf.empty:
                    st.dataframe(
                        mdf[["pilot_name","training_type","start","end","duration_h","instructor","resource","status"]].sort_values("start"),
                        use_container_width=True, hide_index=True
                    )
                else:
                    st.info("No sessions this month.")

    # ══════════════════════════════════════════
    # YEAR TAB
    # ══════════════════════════════════════════
    with tab_year:
        if "year_offset" not in st.session_state:
            st.session_state.year_offset = 0

        nav1, nav2, nav3, nav4 = st.columns([1, 1, 4, 1])
        with nav1:
            if st.button("◀ Prev", key="yprev"):
                st.session_state.year_offset -= 1
        with nav2:
            if st.button("Next ▶", key="ynext"):
                st.session_state.year_offset += 1
        with nav3:
            cur_year = TODAY.year + st.session_state.year_offset
            st.markdown(f"### {cur_year}")
        with nav4:
            if st.button("Today", key="ytoday"):
                st.session_state.year_offset = 0

        MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

        if not pilots_to_show:
            st.info("No pilots to display.")
        else:
            html = [SCHED_CSS, '<table class="sched-grid"><thead><tr><th>Pilot</th>']
            for mn in MONTH_NAMES:
                idx = MONTH_NAMES.index(mn) + 1
                cls = "curmonth-hdr" if (idx == TODAY.month and cur_year == TODAY.year) else ""
                html.append(f'<th class="{cls}">{mn}</th>')
            html.append('</tr></thead><tbody>')

            # Range rows in year view — count open days per month
            if st.session_state.ranges:
                import calendar as cal
                range_names = list(dict.fromkeys(r["name"] for r in st.session_state.ranges))
                for rname in range_names:
                    html.append(f'<tr><td class="pilot-cell" style="color:#0f7b6c">🎯 {rname}</td>')
                    for month_idx in range(1, 13):
                        is_cur = (month_idx == TODAY.month and cur_year == TODAY.year)
                        tcls   = "curmonth-col" if is_cur else ""
                        m_start = date(cur_year, month_idx, 1)
                        m_end   = date(cur_year, month_idx, cal.monthrange(cur_year, month_idx)[1])
                        open_count = sum(1 for r in st.session_state.ranges
                                         if r["name"] == rname
                                         and m_start <= r["date"] <= m_end)
                        html.append(f'<td class="{tcls}" style="text-align:center;vertical-align:middle">')
                        if open_count:
                            html.append(
                                f'<div style="font-size:11px;color:#0f7b6c;font-weight:bold">{open_count}d</div>'
                                f'<span class="sess-dot" style="background:#0f7b6c" '
                                f'title="{rname}: {open_count} open day(s) in {MONTH_NAMES[month_idx-1]}"></span>'
                            )
                        html.append('</td>')
                    html.append('</tr>')
                html.append('<tr><td colspan="14" style="height:4px;background:#1a1a2e;padding:0"></td></tr>')

            for pilot in pilots_to_show:
                html.append('<tr>' + pilot_name_cell(pilot))
                for month_idx in range(1, 13):
                    import calendar as cal
                    m_start = date(cur_year, month_idx, 1)
                    m_end   = date(cur_year, month_idx, cal.monthrange(cur_year, month_idx)[1])
                    is_cur  = (month_idx == TODAY.month and cur_year == TODAY.year)
                    tcls    = "curmonth-col" if is_cur else ""
                    sessions = sessions_in_range(pilot["id"], m_start, m_end)
                    html.append(f'<td class="{tcls}" style="text-align:center;vertical-align:middle;min-width:55px">')
                    if sessions:
                        # Count by type, show dots
                        from collections import Counter
                        counts = Counter(s["training_type"] for s in sessions)
                        total  = sum(counts.values())
                        html.append(f'<div style="font-size:11px;color:#ccd6f6;font-weight:bold">{total}</div>')
                        for tt, cnt in counts.items():
                            color = TRAINING_TYPES.get(tt, {}).get("color", "#555")
                            tip   = f'{tt} × {cnt}'
                            html.append(f'<span class="sess-dot" style="background:{color}" title="{tip}"></span>')
                    html.append('</td>')
                html.append('</tr>')
            html.append('</tbody></table>')
            st.markdown("".join(html), unsafe_allow_html=True)

            st.divider()
            st.markdown(legend_html(), unsafe_allow_html=True)

            # Year summary bar chart
            st.divider()
            st.subheader(f"Training Volume — {cur_year}")
            if not df.empty:
                ydf = df[df["start"].dt.year == cur_year]
                if filter_pilot:
                    ydf = ydf[ydf["pilot_name"].apply(lambda x: any(p in x for p in filter_pilot))]
                if filter_type:
                    ydf = ydf[ydf["training_type"].isin(filter_type)]
                if not ydf.empty:
                    ydf["month"] = ydf["start"].dt.month
                    monthly = ydf.groupby(["month","training_type"])["duration_h"].sum().reset_index()
                    monthly["month_name"] = monthly["month"].apply(lambda m: MONTH_NAMES[m-1])
                    fig = px.bar(
                        monthly, x="month_name", y="duration_h", color="training_type",
                        color_discrete_map={k: v["color"] for k, v in TRAINING_TYPES.items()},
                        category_orders={"month_name": MONTH_NAMES},
                        labels={"duration_h": "Hours", "month_name": "Month", "training_type": "Type"},
                        title=f"Training Hours by Month — {cur_year}"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info(f"No sessions in {cur_year}.")

    # ══════════════════════════════════════════
    # RANGE TAB  (embedded in Schedule page)
    # ══════════════════════════════════════════
    with tab_range:
        st.subheader("🎯 Range Scheduling")
        st.caption("Define when ranges are open. Flight events are restricted to open range windows in the optimizer.")

        rtab_add, rtab_manage = st.tabs(["➕ Add Window", "📋 Manage"])

        with rtab_add:
            with st.form("add_range_sched"):
                rc1, rc2, rc3 = st.columns(3)
                r_name       = rc1.text_input("Range Name", value="Range Alpha")
                r_date       = rc2.date_input("Date", value=date.today(), key="rdate_sched")
                r_start_hour = rc1.slider("Open From (hour)", 6, 20, 8, key="rsh_sched")
                r_end_hour   = rc2.slider("Close At (hour)",  8, 23, 16, key="reh_sched")
                r_notes      = rc3.text_area("Notes", height=80, key="rnotes_sched")
                if st.form_submit_button("Add Range Window", type="primary"):
                    if r_end_hour <= r_start_hour:
                        st.error("Close time must be after open time.")
                    else:
                        # Duplicate check: same name + same date = double-booking
                        duplicate = any(
                            r["name"].strip().lower() == r_name.strip().lower()
                            and r["date"] == r_date
                            for r in st.session_state.ranges
                        )
                        if duplicate:
                            st.error(
                                f"⚠️ **{r_name}** is already booked on "
                                f"**{r_date.strftime('%b %d, %Y')}**. "
                                f"A range cannot be double-booked on the same day. "
                                f"Use a different name to book a second range on this date."
                            )
                        else:
                            rid = f"R{len(st.session_state.ranges)+1:03d}"
                            st.session_state.ranges.append({
                                "id": rid, "name": r_name, "date": r_date,
                                "start_hour": r_start_hour, "end_hour": r_end_hour,
                                "notes": r_notes,
                            })
                            st.success(f"✅ {r_name} added — {r_date.strftime('%b %d')} {r_start_hour:02d}:00–{r_end_hour:02d}:00.")
                            st.rerun()

        with rtab_manage:
            if not st.session_state.ranges:
                st.info("No range windows defined. Add one above.")
            else:
                st.caption(f"{len(st.session_state.ranges)} range window(s) defined. Click 🗑️ to delete individual rows, or use bulk delete below.")

                # Per-row display with individual delete buttons
                for r in list(st.session_state.ranges):
                    c1, c2, c3, c4, c5 = st.columns([2, 2, 2, 3, 1])
                    c1.markdown(f"**{r['name']}**")
                    c2.markdown(r["date"].strftime("%a %b %d, %Y"))
                    c3.markdown(f"`{r['start_hour']:02d}:00 – {r['end_hour']:02d}:00`")
                    c4.markdown(r["notes"] or "—")
                    if c5.button("🗑️", key=f"del_r_{r['id']}", help=f"Delete {r['name']} on {r['date']}"):
                        st.session_state.ranges = [x for x in st.session_state.ranges if x["id"] != r["id"]]
                        st.success(f"Deleted {r['name']} on {r['date'].strftime('%b %d')}.")
                        st.rerun()

                st.divider()
                # Bulk delete
                del_ids = st.multiselect(
                    "Bulk delete — select ranges",
                    [r["id"] for r in st.session_state.ranges],
                    format_func=lambda x: next(
                        f"{r['name']} — {r['date'].strftime('%b %d')} {r['start_hour']:02d}:00–{r['end_hour']:02d}:00"
                        for r in st.session_state.ranges if r["id"] == x
                    )
                )
                if del_ids and st.button("🗑️ Delete Selected", type="primary", key="del_ranges_bulk"):
                    st.session_state.ranges = [r for r in st.session_state.ranges if r["id"] not in del_ids]
                    st.success(f"Deleted {len(del_ids)} range(s).")
                    st.rerun()

# ─────────────────────────────────────────────
# PAGE: ADD / MANAGE SESSIONS
# ─────────────────────────────────────────────
elif page == "➕ Add Session":
    st.header("➕ Add & Manage Sessions")
    tab_add, tab_delete = st.tabs(["➕ Add Session", "🗑️ Delete Sessions"])

    # ── ADD TAB ──────────────────────────────────
    with tab_add:
        with st.form("add_session"):
            col1, col2 = st.columns(2)
            pilot_opts = {p["id"]: f"{p['name']} ({p['callsign']}) – {p['phase']}" for p in st.session_state.pilots}
            with col1:
                pilot_id     = st.selectbox("Pilot", list(pilot_opts), format_func=lambda x: pilot_opts[x])
                ttype        = st.selectbox("Training Type", list(TRAINING_TYPES),
                                            format_func=lambda t: f"{t} – {TRAINING_TYPES[t]['desc']}")
                session_date = st.date_input("Date", value=date.today())
                is_night     = TRAINING_TYPES[ttype].get("night_only", False)
                default_hour = 20 if is_night else 8
                default_time = datetime.now().replace(hour=default_hour, minute=0, second=0, microsecond=0).time()
                start_time   = st.time_input("Start Time", value=default_time)
            with col2:
                instructor = st.selectbox("Instructor", INSTRUCTORS)
                resource   = st.selectbox("Resource", SIMULATORS if ttype.startswith("SIM") else AIRCRAFT)
                status     = st.selectbox("Status", ["Scheduled", "Pending"])
                notes      = st.text_area("Notes", height=80)

            start_dt = datetime.combine(session_date, start_time)
            end_dt   = start_dt + timedelta(hours=TRAINING_TYPES[ttype]["hours"])
            prereq   = TRAINING_TYPES[ttype]["prereq"] or "None"

            # Night-only info banner
            if is_night:
                st.warning(f"🌙 **{ttype}** is a night-only event (must start at 18:00 or later).")
            st.info(f"⏱️ Duration: **{TRAINING_TYPES[ttype]['hours']} hrs** → Ends **{end_dt.strftime('%H:%M')}**  |  Prereq: **{prereq}**")

            if st.form_submit_button("Check & Schedule", type="primary"):
                # Night-only time check
                if is_night and start_dt.hour < 18:
                    st.error(f"🌙 {ttype} is night-only and must start at 18:00 or later (selected: {start_dt.strftime('%H:%M')}).")
                else:
                    new_sess = {"id": f"S{len(st.session_state.sessions)+1:04d}",
                                "pilot_id": pilot_id, "training_type": ttype,
                                "start": start_dt, "end": end_dt,
                                "instructor": instructor, "resource": resource,
                                "status": status, "notes": notes}
                    conflicts = conflict_check(new_sess)
                    if conflicts:
                        st.error("**Conflicts detected — session not saved:**")
                        for c in conflicts: st.write(c)
                    else:
                        st.session_state.sessions.append(new_sess)
                        st.success(f"✅ Session {new_sess['id']} scheduled for {pilot_opts[pilot_id]}!")

    # ── DELETE TAB ───────────────────────────────
    with tab_delete:
        st.subheader("🗑️ Delete Sessions")
        df_all = sessions_df()
        if df_all.empty:
            st.info("No sessions to delete.")
        else:
            # Filters
            dc1, dc2, dc3 = st.columns(3)
            del_filter_pilot  = dc1.multiselect("Filter by Pilot",  [p["name"] for p in st.session_state.pilots], key="del_fp")
            del_filter_type   = dc2.multiselect("Filter by Type",   list(TRAINING_TYPES.keys()), key="del_ft")
            del_filter_status = dc3.multiselect("Filter by Status", ["Scheduled","Completed","Pending","Scheduled (Optimized)"], key="del_fs")

            view = df_all.copy()
            if del_filter_pilot:  view = view[view["pilot_name"].apply(lambda x: any(p in x for p in del_filter_pilot))]
            if del_filter_type:   view = view[view["training_type"].isin(del_filter_type)]
            if del_filter_status: view = view[view["status"].isin(del_filter_status)]

            if view.empty:
                st.info("No sessions match filters.")
            else:
                st.caption(f"Showing {len(view)} session(s). Check boxes to select, then click Delete.")
                display_cols = ["id","pilot_name","training_type","start","end","instructor","status"]

                # Render rows with checkboxes
                if "delete_selected" not in st.session_state:
                    st.session_state.delete_selected = set()

                # Select all toggle
                sel_col, _ = st.columns([1, 5])
                if sel_col.button("Select All", key="sel_all"):
                    st.session_state.delete_selected = set(view["id"].tolist())
                    st.rerun()
                if sel_col.button("Clear", key="sel_clear"):
                    st.session_state.delete_selected = set()
                    st.rerun()

                for _, row in view.iterrows():
                    sid = row["id"]
                    checked = sid in st.session_state.delete_selected
                    c1, c2 = st.columns([0.3, 9.7])
                    new_checked = c1.checkbox("", value=checked, key=f"chk_{sid}")
                    if new_checked != checked:
                        if new_checked:
                            st.session_state.delete_selected.add(sid)
                        else:
                            st.session_state.delete_selected.discard(sid)
                    c2.markdown(
                        f"`{sid}` — **{row['pilot_name']}** | {row['training_type']} | "
                        f"{row['start'].strftime('%b %d %H:%M')}–{row['end'].strftime('%H:%M')} | "
                        f"{row['instructor']} | _{row['status']}_"
                    )

                st.divider()
                n_sel = len(st.session_state.delete_selected)
                if n_sel > 0:
                    st.warning(f"**{n_sel} session(s) selected for deletion.**")
                    if st.button(f"🗑️ Delete {n_sel} Session(s)", type="primary"):
                        st.session_state.sessions = [
                            s for s in st.session_state.sessions
                            if s["id"] not in st.session_state.delete_selected
                        ]
                        st.session_state.delete_selected = set()
                        st.success("✅ Sessions deleted.")
                        st.rerun()
                else:
                    st.info("No sessions selected.")

# ─────────────────────────────────────────────
# PAGE: PILOTS
# ─────────────────────────────────────────────
elif page == "👨‍✈️ Pilots":
    st.header("👨‍✈️ Pilot Roster & Readiness")
    df = sessions_df()

    for pilot in st.session_state.pilots:
        status = pilot_overall_status(pilot)
        color  = STATUS_COLORS[status]
        label  = STATUS_LABELS[status]
        with st.expander(f"**{pilot['name']}** ({pilot['callsign']}) — {pilot['squadron']} — {label}"):
            c1, c2, c3 = st.columns(3)
            c1.metric("Total Flight Hours", f"{pilot['total_hours']} h")
            c2.metric("Sim Hours",          f"{pilot['sim_hours']} h")
            if not df.empty:
                p_sess = df[df["pilot_id"] == pilot["id"]]
                c3.metric("Scheduled Sessions", len(p_sess))
                upcoming = p_sess[p_sess["start"] >= datetime.now()].sort_values("start")
                if not upcoming.empty:
                    st.caption("**Upcoming sessions:**")
                    for _, s in upcoming.head(3).iterrows():
                        st.write(f"• `{s['training_type']}` {s['start'].strftime('%b %d %H:%M')} – {s['instructor']} ({s['resource']})")

    st.divider()
    st.subheader("Add New Pilot")
    with st.form("add_pilot"):
        c1, c2, c3, c4 = st.columns(4)
        new_name = c1.text_input("Full Name")
        new_cs   = c2.text_input("Callsign")
        new_sq   = c3.selectbox("Squadron", SQUADRONS)
        new_ph   = c4.selectbox("Phase", QUALIFICATIONS)
        if st.form_submit_button("Add Pilot") and new_name and new_cs:
            pid = f"P{len(st.session_state.pilots):03d}"
            st.session_state.pilots.append({
                "id": pid, "name": new_name, "callsign": new_cs,
                "squadron": new_sq, "phase": new_ph,
                "total_hours": 0, "sim_hours": 0, "currency": {}
            })
            st.success(f"Pilot {new_name} added as {pid}")

# ─────────────────────────────────────────────
# PAGE: CURRENCY STATUS (Excel-style dashboard)
# ─────────────────────────────────────────────
elif page == "📊 Currency Status":
    st.header("📊 Pilot Currency Status")
    st.caption("Mirrors the VMFAT-502 Excel spreadsheet — yellow = warning, red = alert")

    display_cols = [c for c in EXCEL_PILOT_COLUMNS if c != "Name"]

    # Build display dataframe
    rows = []
    for pilot in st.session_state.pilots:
        currency = pilot.get("currency", {})
        row = {"Name": pilot["name"]}
        row.update({col: currency.get(col) for col in display_cols})
        rows.append(row)

    if not rows:
        st.info("No pilots loaded.")
    else:
        raw_df = pd.DataFrame(rows)

        def style_cell(val, col):
            status = get_cell_status(col, val)
            if status == "alert":
                return "background-color: #d62728; color: white; font-weight: bold"
            if status == "warning":
                return "background-color: #FFD700; color: black"
            return ""

        styled = raw_df.style
        for col in display_cols:
            if col in raw_df.columns:
                styled = styled.applymap(lambda v, c=col: style_cell(v, c), subset=[col])

        st.dataframe(styled, use_container_width=True, hide_index=True)

        st.divider()
        st.subheader("Fleet Currency Summary")
        alert_count   = sum(1 for p in st.session_state.pilots if pilot_overall_status(p) == "alert")
        warning_count = sum(1 for p in st.session_state.pilots if pilot_overall_status(p) == "warning")
        ok_count      = sum(1 for p in st.session_state.pilots if pilot_overall_status(p) == "ok")

        c1, c2, c3 = st.columns(3)
        c1.metric("🔴 Alert",   alert_count,   delta=None)
        c2.metric("🟡 Warning", warning_count, delta=None)
        c3.metric("🟢 Current", ok_count,      delta=None)

        # Bar chart of pilots by status
        status_data = pd.DataFrame([
            {"Pilot": p["name"], "Status": pilot_overall_status(p)}
            for p in st.session_state.pilots
        ])
        fig = px.bar(status_data, x="Pilot", color="Status",
                     color_discrete_map={"alert": "#d62728", "warning": "#FFD700", "ok": "#2ca02c"},
                     title="Pilot Currency Status Overview")
        st.plotly_chart(fig, use_container_width=True)

# ─────────────────────────────────────────────
# PAGE: PAUSE TRAINING
# ─────────────────────────────────────────────
elif page == "⏸️ Pause Training":
    st.header("⏸️ Pause / Resume Training")
    st.info("Paused students are excluded from the optimizer and their scheduled sessions are held. Resume restores them.")

    tab_pause, tab_active = st.tabs(["⏸️ Pause a Student", "▶️ Active Pauses"])

    with tab_pause:
        active_pilot_ids = [p["id"] for p in st.session_state.pilots
                            if p["id"] not in st.session_state.paused_pilots]
        if not active_pilot_ids:
            st.info("All pilots are currently paused.")
        else:
            with st.form("pause_form"):
                pilot_opts = {p["id"]: f"{p['name']} ({p['callsign']})"
                              for p in st.session_state.pilots
                              if p["id"] in active_pilot_ids}
                pc1, pc2 = st.columns(2)
                pause_pid    = pc1.selectbox("Pilot", list(pilot_opts), format_func=lambda x: pilot_opts[x])
                pause_reason = pc2.text_input("Reason (e.g. medical, TDY, admin)")
                pause_type   = pc1.radio("Pause Duration", ["Indefinite", "Fixed duration"])
                pause_days   = pc2.number_input("Days to pause", min_value=1, max_value=365, value=14,
                                                disabled=(pause_type == "Indefinite"))

                if st.form_submit_button("⏸️ Pause Training", type="primary"):
                    resume_on = None
                    if pause_type == "Fixed duration":
                        resume_on = date.today() + timedelta(days=int(pause_days))

                    # Hold the pilot's future scheduled sessions
                    future_sess = [
                        s["id"] for s in st.session_state.sessions
                        if s["pilot_id"] == pause_pid and s["start"].date() >= date.today()
                    ]
                    st.session_state.paused_pilots[pause_pid] = {
                        "reason":            pause_reason or "No reason given",
                        "paused_on":         date.today(),
                        "resume_on":         resume_on,
                        "held_session_ids":  future_sess,
                    }
                    # Mark those sessions as Paused
                    for s in st.session_state.sessions:
                        if s["id"] in future_sess:
                            s["status"] = "Paused"

                    pname = pilot_opts[pause_pid]
                    msg = f"⏸️ {pname} paused"
                    msg += f" until {resume_on.strftime('%b %d, %Y')}" if resume_on else " indefinitely"
                    msg += f". {len(future_sess)} session(s) placed on hold."
                    st.success(msg)
                    st.rerun()

    with tab_active:
        if not st.session_state.paused_pilots:
            st.info("No pilots are currently paused.")
        else:
            st.subheader(f"{len(st.session_state.paused_pilots)} Paused Pilot(s)")
            for pid, info in list(st.session_state.paused_pilots.items()):
                pname = next((p["name"] for p in st.session_state.pilots if p["id"] == pid), pid)
                resume_str = info["resume_on"].strftime("%b %d, %Y") if info["resume_on"] else "Indefinite"
                days_paused = (date.today() - info["paused_on"]).days

                with st.expander(f"**{pname}** — paused {days_paused} day(s) ago | Resume: {resume_str}"):
                    st.write(f"**Reason:** {info['reason']}")
                    st.write(f"**Paused on:** {info['paused_on'].strftime('%b %d, %Y')}")
                    st.write(f"**Resume date:** {resume_str}")
                    held = info.get("held_session_ids", [])
                    st.write(f"**Sessions on hold:** {len(held)}")

                    col_r1, col_r2, col_r3 = st.columns(3)

                    # Auto-resume check
                    if info["resume_on"] and date.today() >= info["resume_on"]:
                        st.warning("⚠️ This pilot's resume date has passed — click Resume to reactivate.")

                    if col_r1.button(f"▶️ Resume", key=f"resume_{pid}"):
                        # Re-activate held sessions
                        for s in st.session_state.sessions:
                            if s["id"] in held and s["status"] == "Paused":
                                s["status"] = "Scheduled"
                        del st.session_state.paused_pilots[pid]
                        st.success(f"▶️ {pname} resumed. {len(held)} session(s) restored.")
                        st.rerun()

                    new_resume = col_r2.date_input(f"Change resume date", value=info["resume_on"] or date.today(),
                                                    key=f"new_resume_{pid}")
                    if col_r2.button("Update Date", key=f"upd_{pid}"):
                        st.session_state.paused_pilots[pid]["resume_on"] = new_resume
                        st.success("Resume date updated.")
                        st.rerun()

                    if col_r3.button("🗑️ Cancel & Clear Sessions", key=f"cancel_{pid}"):
                        # Remove held sessions entirely
                        st.session_state.sessions = [
                            s for s in st.session_state.sessions if s["id"] not in held
                        ]
                        del st.session_state.paused_pilots[pid]
                        st.success(f"{pname} removed from pause. {len(held)} sessions cleared.")
                        st.rerun()

            # Auto-resume any pilots whose date has passed
            auto_resumed = []
            for pid, info in list(st.session_state.paused_pilots.items()):
                if info["resume_on"] and date.today() >= info["resume_on"]:
                    held = info.get("held_session_ids", [])
                    for s in st.session_state.sessions:
                        if s["id"] in held and s["status"] == "Paused":
                            s["status"] = "Scheduled"
                    del st.session_state.paused_pilots[pid]
                    auto_resumed.append(pid)
            if auto_resumed:
                st.info(f"Auto-resumed {len(auto_resumed)} pilot(s) whose pause period ended.")
                st.rerun()

# ─────────────────────────────────────────────
# PAGE: PYOMO OPTIMIZER
# ─────────────────────────────────────────────
elif page == "🤖 Optimize (Pyomo)":
    st.header("🤖 Graduation Optimizer")
    st.info("MILP optimizer: minimize the makespan (latest graduation day) across all active students while respecting prerequisites, range availability, night-only restrictions, and resource limits.")

    col1, col2 = st.columns(2)
    with col1:
        planning_days  = st.slider("Planning horizon (days)", 5, 60, 21)
        start_date_opt = st.date_input("Start date", value=date.today())
    with col2:
        st.subheader("Active Constraints")
        c = st.session_state.constraints
        st.write(f"• Max flight hrs/day: **{c['max_flight_hours_per_day']}**")
        st.write(f"• Max sim hrs/day: **{c['max_sim_hours_per_day']}**")
        st.write(f"• Instructor max hrs/day: **{c['instructor_max_daily_hours']}**")
        range_note = f"**{len(st.session_state.ranges)} windows defined**" if st.session_state.ranges else "None — flight events unrestricted"
        st.write(f"• Range windows: {range_note}")
        paused_note = f"**{len(st.session_state.paused_pilots)} paused** (excluded)" if st.session_state.paused_pilots else "None paused"
        st.write(f"• Paused pilots: {paused_note}")

    st.divider()
    st.subheader("Pilot Status")
    pilot_status_df = pd.DataFrame([{
        "Pilot": p["name"],
        "Group": p.get("group", p.get("phase", "—")),
        "Status": STATUS_LABELS[pilot_overall_status(p)],
        "Paused": "⏸️ Yes" if p["id"] in st.session_state.paused_pilots else "▶️ Active",
    } for p in st.session_state.pilots])
    st.dataframe(pilot_status_df, use_container_width=True, hide_index=True)

    st.divider()

    # ── Cache status indicator ────────────────────────────────────────────
    current_key = compute_opt_cache_key(planning_days, start_date_opt, st.session_state.constraints)
    cache_hit   = (current_key == st.session_state.opt_cache_key and
                   st.session_state.opt_result is not None)

    if cache_hit:
        st.info("✅ Schedule is already optimal for current inputs — no changes detected. Apply the existing result or adjust inputs to re-run.")
    
    def show_opt_result(result, makespan_val, pilot_completions):
        if makespan_val:
            st.success(f"✅ Optimal: **{len(result)} new sessions** — all students graduate by day **{makespan_val}**.")
        else:
            st.success(f"✅ Generated **{len(result)} new sessions**.")

        if pilot_completions:
            comp_df = pd.DataFrame([
                {"Pilot": pilot_name(pid),
                 "Graduation Day": day,
                 "Graduation Date": (start_date_opt + timedelta(days=day)).strftime("%b %d, %Y")}
                for pid, day in pilot_completions.items() if day > 0
            ])
            if not comp_df.empty:
                st.subheader("📅 Projected Graduation Dates")
                st.dataframe(comp_df, use_container_width=True, hide_index=True)

        if result:
            opt_df = pd.DataFrame(result)
            opt_df["pilot_name"] = opt_df["pilot_id"].apply(pilot_name)
            opt_df["duration_h"] = (opt_df["end"] - opt_df["start"]).dt.total_seconds() / 3600
            st.subheader("New Sessions to Add")
            st.dataframe(
                opt_df[["pilot_name","training_type","start","end","duration_h","instructor","resource"]],
                use_container_width=True, hide_index=True
            )
            c1, c2 = st.columns(2)
            with c1:
                if st.button("✅ Apply to Schedule", type="primary", key="apply_opt"):
                    st.session_state.sessions.extend(result)
                    # Update cache so re-running immediately detects no changes
                    st.session_state.opt_cache_key = compute_opt_cache_key(
                        planning_days, start_date_opt, st.session_state.constraints)
                    st.session_state.opt_result = None
                    st.success(f"✅ {len(result)} sessions added to schedule.")
                    st.rerun()
            with c2:
                if st.button("❌ Discard", key="discard_opt"):
                    st.session_state.opt_result = None
                    st.session_state.opt_cache_key = None
                    st.rerun()
        else:
            st.info("All training types are already scheduled for all active pilots — nothing new to add.")

    if not cache_hit:
        if st.button("🚀 Run Graduation Optimizer", type="primary"):
            with st.spinner("Running MILP graduation optimizer..."):
                opt_out = run_pyomo_optimization(
                    st.session_state.pilots,
                    planning_days,
                    start_date_opt,
                    st.session_state.constraints
                )

            result, error = opt_out[0], opt_out[1]
            makespan_val   = opt_out[2] if len(opt_out) > 2 else None
            pilot_completions = opt_out[3] if len(opt_out) > 3 else {}

            if error:
                st.error(f"Optimization failed: {error}")
                st.info("""
**To enable optimization, install a solver:**
```bash
pip install pyomo
sudo apt install glpk-utils   # Ubuntu/Debian
brew install glpk             # macOS
```
                """)
            else:
                st.session_state.opt_result      = result
                st.session_state.opt_cache_key   = current_key
                show_opt_result(result, makespan_val, pilot_completions)

        elif st.session_state.opt_result:
            show_opt_result(st.session_state.opt_result, None, {})
    else:
        show_opt_result(st.session_state.opt_result, None, {})

# ─────────────────────────────────────────────
# PAGE: ANALYTICS
# ─────────────────────────────────────────────
elif page == "📊 Analytics":
    st.header("📊 Training Analytics")
    df = sessions_df()

    if df.empty:
        st.info("No data yet.")
    else:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Sessions",       len(df))
        c2.metric("Total Training Hours", f"{df['duration_h'].sum():.1f} h")
        c3.metric("Scheduled",            len(df[df["status"].str.contains("Scheduled")]))
        c4.metric("Completed",            len(df[df["status"] == "Completed"]))

        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Hours by Training Type")
            type_h = df.groupby("training_type")["duration_h"].sum().reset_index()
            fig = px.bar(type_h, x="training_type", y="duration_h",
                         color="training_type",
                         color_discrete_map={k: v["color"] for k, v in TRAINING_TYPES.items()},
                         labels={"training_type": "Type", "duration_h": "Hours"})
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            st.subheader("Sessions by Pilot")
            pilot_counts = df.groupby("pilot_name").size().reset_index(name="count")
            fig2 = px.pie(pilot_counts, names="pilot_name", values="count")
            st.plotly_chart(fig2, use_container_width=True)

        st.subheader("Instructor Load")
        inst_load = df.groupby("instructor")["duration_h"].sum().reset_index()
        inst_load.columns = ["Instructor", "Total Hours"]
        st.dataframe(inst_load, use_container_width=True, hide_index=True)

        st.subheader("Resource Utilization")
        res_load = df.groupby("resource")["duration_h"].sum().reset_index()
        fig3 = px.bar(res_load, x="resource", y="duration_h",
                      labels={"resource": "Resource", "duration_h": "Hours Booked"})
        st.plotly_chart(fig3, use_container_width=True)

# ─────────────────────────────────────────────
# PAGE: CONSTRAINTS
# ─────────────────────────────────────────────
elif page == "⚙️ Constraints":
    st.header("⚙️ Scheduling Constraints")
    st.info("These constraints are passed directly into the Pyomo optimizer.")

    c = st.session_state.constraints
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Pilot Limits")
        c["max_flight_hours_per_day"]    = st.slider("Max flight hours/day per pilot",   1, 8,  c["max_flight_hours_per_day"])
        c["max_sim_hours_per_day"]       = st.slider("Max simulator hours/day per pilot", 1, 12, c["max_sim_hours_per_day"])
        c["min_rest_hours"]              = st.slider("Min rest hours between sessions",   8, 24, c["min_rest_hours"])
        c["max_consecutive_flight_days"] = st.slider("Max consecutive flight days",       1, 10, c["max_consecutive_flight_days"])

    with col2:
        st.subheader("Instructor Limits")
        c["instructor_max_daily_hours"] = st.slider("Max instructor hours/day", 4, 12, c["instructor_max_daily_hours"])

        st.subheader("Prerequisite Chain")
        for ttype, info in TRAINING_TYPES.items():
            st.write(f"**{ttype}** → requires: `{info['prereq'] or 'None'}`")

    st.session_state.constraints = c
    st.success("Constraints saved.")
