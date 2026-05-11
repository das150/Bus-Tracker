from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import requests
import xml.etree.ElementTree as ET
import urllib3
from functools import lru_cache
import time
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import sqlite3
from datetime import datetime, timedelta
import threading
from concurrent.futures import ThreadPoolExecutor

LATEST_TELEMETRY = {}
TELEMETRY_LOCK = threading.Lock()

def fetch_streets_chunk(chunk_guids):
    """Surgical burst to StreetsWeb for 3-5 routes."""
    url = f"{STREETS_BASE}RouteMap/GetVehicles"
    headers = {"X-Requested-With": "XMLHttpRequest", "RequestVerificationToken": STREETS_TOKEN, "Content-Type": "application/json"}
    try:
        # Short timeout: if a batch hangs, don't let it block the others
        resp = session.post(url, headers=headers, json={"routeKeys": chunk_guids}, timeout=4)
        return resp.json() if resp.status_code == 200 else []
    except:
        return []
# Create a global to communicate between the Brain and Muscle
ACTIVE_ROUTE_IDS = set()

def telemetry_worker():
    """Smart Background Engine: Only requests GUIDs for active routes."""
    print("🧵 Smart Telemetry Worker Active...")
    
    while True:
        try:
            # 1. Get the list of GUIDs only for routes currently seen by the Legacy API
            with TELEMETRY_LOCK:
                target_routes = list(ACTIVE_ROUTE_IDS)
            
            # --- Inside telemetry_worker ---
            if not target_routes or len(target_routes) < 2:
                # If legacy is failing or we just started, probe EVERY known GUID
                target_guids = [guid for sublist in STREETS_GUID_MAP.values() for guid in sublist]
                # Slow down slightly when probing everything to avoid a timeout
                batch_timeout = 25
            else:
                target_guids = []
                for rid in target_routes:
                    if rid in STREETS_GUID_MAP:
                        target_guids.extend(STREETS_GUID_MAP[rid])
            if not target_guids:
                time.sleep(1)
                continue

            url = f"{STREETS_BASE}RouteMap/GetVehicles"
            headers = {"X-Requested-With": "XMLHttpRequest", "RequestVerificationToken": STREETS_TOKEN, "Content-Type": "application/json"}
            
            # 2. Request only the needed data. Increased timeout to 20s to prevent 'Read timed out'.
            resp = session.post(url, headers=headers, json={"routeKeys": target_guids}, timeout=20)
            
            if resp.status_code == 200:
                raw_data = resp.json()
                new_telemetry = {}
                for route_obj in raw_data:
                    for direction in route_obj.get('vehiclesByDirections', []):
                        for bus in direction.get('vehicles', []):
                            loc = bus.get('location', {})
                            last_gps_str = loc.get('lastGpsDate', "")
                            
                            streets_ts = 0
                            if last_gps_str:
                                try:
                                    dt_obj = datetime.fromisoformat(last_gps_str)
                                    streets_ts = int(dt_obj.timestamp() * 1000)
                                except: pass

                            new_telemetry[bus['name']] = {
                                "lat": loc.get('latitude'),
                                "lon": loc.get('longitude'),
                                "heading": loc.get('heading'),
                                "speed": loc.get('speed', 0),
                                "pax": bus.get('passengersOnboard', 0),
                                "cap": bus.get('passengerCapacity', 70),
                                "ts": streets_ts,
                                "directionName": direction.get('directionName', 'Loop'),
                                "routeKey": bus.get('routeKey'),
                                "isExtra": bus.get('isExtraTrip', False),
                                "amenities": bus.get('amenities', []) # AC, Bike Racks, etc.
                            }
                
                with TELEMETRY_LOCK:
                    global LATEST_TELEMETRY
                    LATEST_TELEMETRY = new_telemetry
            
            # High-speed refresh
            time.sleep(0.7) 
                
        except Exception as e:
            # On error, don't sleep too long, just let the next loop try again
            print(f"🧵 Worker Lag/Timeout: {e}")
            time.sleep(0.5)

# Start the worker
thread = threading.Thread(target=telemetry_worker, daemon=True)
thread.start()

# Server-side Cache Store
PATTERN_CACHE = {
    "data": None,
    "last_updated": 0
}
CACHE_TTL = 3600 # Cache results for 1 hour (3600 seconds)

# --- STREETSWEB (MYRIDE) HYBRID CONFIG ---
STREETS_BASE = "http://216.252.195.248/StreetsWeb/MyRide/"
# Fresh token from your discovery
STREETS_TOKEN = "kDgRUwUoLE7dG9end2_3pvoa1ZIfFVH02w6YPbwg6djUqJKj068AHh4SHgBDSmZ_2a2R9YaZyLhItGZJu3UofTt0-MDmNZi0ejTMYrf7zII1:D9u0NsH3u8I9JHiqYQO4bNr6iU7E0RgMB-spM5-UQ30zzMR-ZHzlHKi6ZTcUU-GAJn0XUvjDMp7vcDQHmIIcVBlpSkPLWWflpL92eYLDtN01"

# Exhaustive GUID Map from your Reap
STREETS_GUID_MAP = {
    "BLU": ["d21da455-aeb1-476f-b1b6-bca9a0d35ad1"],
    "BMR": ["26568b26-7bb7-4e6f-af85-8fe4a90dedfc", "56d59483-95c3-4420-b47e-dba4dc54a1f6", "61d13e18-83e6-497e-bebc-f64ebec91a47"],
    "CAS": ["9f1d8690-9e44-49f1-a641-208c0fafc119", "58551424-0ad9-4f26-915a-c1a22c319e26", "f07d3ec4-8c6b-4cb6-b4fc-e5959c33ebb4"],
    "CRB": ["5c545da3-7ab6-4c1e-91b1-03aaa4907609", "41518ed8-29a7-4a38-b5f2-0e1503e817f7", "3c29bc76-5485-4e58-8e69-a1094d35eb6f"],
    "CRC": ["bbf0df3d-4aa0-404b-bd08-4f812481f80b", "83bad4a0-d646-49fa-94c3-d658fc78bdee", "d545f765-aa0e-4807-863f-eefe510465e0"],
    "GLD": ["77c9d8c3-e3b6-4a1e-962f-adf12580e951"],
    "GRN": ["a69d2dbe-7f67-498e-abfb-e65f5f4863c1"],
    "HDG": ["44afa298-8bb7-4352-bd06-1376f226fe0b", "4dfd0193-8282-4b8c-9f20-6c8aff24b97c", "31eea102-ed38-44d8-924b-c0c30cedc043"],
    "HWA": ["6c494bd3-b52f-447d-ad31-61e8132780a6", "bee621b5-050a-4b9c-a3b0-a4d3a92062a3", "260c1436-addd-4969-8b78-a738a5de663e"],
    "HWB": ["8e9bc7d7-19cc-45e8-bc10-381659714dd1", "f92ad5bc-c92a-449c-abc8-822e3cf8a69f", "cac44856-eaca-45d0-95fc-9426ed061fcf"],
    "HWC": ["b2be004d-9ca4-45d7-8b93-29fc227c692f", "c8163d6b-30b1-403b-9c0d-6e0f4b65d6ce", "d46b7d2f-3e72-4dd5-8358-f53fecc23abe"],
    "HXP": ["e92bac69-332f-480a-9d94-b4b99cfa5cb6", "7247efd4-6c35-4e2c-88f0-d525853690de", "76f61c6c-70ba-447c-8649-fbc1aab6b972"],
    "NMG": ["37fd4607-6401-4fe4-9ba4-4ef4e3024ebd", "b787a27f-c618-4e14-b905-56903fb56c22", "835937a2-561d-4839-8dae-f7df5dd9cee3"],
    "NMP": ["fade4e6f-a3e3-4cdc-8287-22c3d7e3ef48", "41cdfb36-22dc-4fe6-9633-8ad5afd4adaa", "cf0220bd-bd82-454c-b94b-d3e319dce6ec"],
    "PHB": ["5862bcf4-35a1-4c5f-81a4-4ec0f217929f", "e009d54e-561b-476e-a6de-7452c71361d6", "42e46b24-4e3b-49f1-9cb7-d3856ecf9977"],
    "PHD": ["782f1529-af4e-420e-bf9e-4e996a0442f7", "015be42e-9247-4a0a-abc7-85398a52e3b4", "b0606293-517c-49e9-ad5a-ab8a6b5fdbde"],
    "PRG": ["74f5d8ed-174e-4a70-bdcd-4a64bb24c08c", "721f5747-7237-4576-adc8-555617335344", "bf4c0c80-3f7c-448c-a351-f5b08f35b2fe"],
    "SMA": ["b5cdd60f-625e-4c7d-ad7e-2f4807eb0f6a", "47a31552-bd6f-451a-a7cd-bf4d9fca016b", "bd1e8519-4460-42c5-8728-ea31b55d3ba7"],
    "SME": ["1dc2d35e-e090-4ba1-88a6-0615583705b9", "032aa69c-ed27-44ac-8f1d-e5bcd84804ad", "daca5b59-dc85-4e92-a009-fc64b786a41e"],
    "SMS": ["6c10282e-35c3-416f-be9f-3dadbeb0237f", "a1ba2da6-1b82-4d52-8a24-9f00e2ea0bfd", "2c29b0c3-4418-49f8-a2f6-e75c3ec94c11"],
    "TCP": ["ebceefdb-5257-487b-b298-35bab610cba5", "6aa6523c-3cf1-4803-bdbf-973997969f1d", "3d09dd80-ec81-404c-8254-e2810b89759e"],
    "TCR": ["d312fbc0-b9e0-487d-9645-18246be780b1", "583d0c95-3ccb-4607-889a-86bfcf21ef41", "1722285c-17c9-46ea-bd7e-e4de3d0b6703"],
    "TTT": ["dee6f3e1-e679-4009-a99f-3d73d80b2d8d", "271e52f8-7ba6-49a2-bce7-879e800117e2", "911a0a6b-11c6-41f6-a637-a70ddc11bd1e"],
    "UCB": ["d9fe0408-275a-4b65-aa4f-031dee8759d2", "eaf4076f-d880-4f86-9946-6debdf2fce09", "8f20c966-25be-47fe-a140-8a9f9a050351"]
}

# --- DISASTER RECOVERY MAP ---
# Reverse-maps every known GUID to its ShortName
GUID_TO_ROUTE = {guid: r_id for r_id, guids in STREETS_GUID_MAP.items() for guid in guids}

def init_tracker_db():
    conn = sqlite3.connect('usage.db')
    c = conn.cursor()
    # Unique sessions with start and last seen times
    c.execute('''CREATE TABLE IF NOT EXISTS sessions 
                 (session_id TEXT PRIMARY KEY, user_id TEXT, 
                  start_time DATETIME, last_seen DATETIME)''')
    conn.commit()
    conn.close()

init_tracker_db()

session = requests.Session()
retries = Retry(total=3, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retries))
session.mount('http://', HTTPAdapter(max_retries=retries))

# Simple cache storage
cache = {}

def get_cached_data(key, max_age=15):
    """Returns (data, is_expired)"""
    if key in cache:
        timestamp, data = cache[key]
        is_expired = (time.time() - timestamp) > max_age
        return data, is_expired
    return None, True

def set_cached_data(key, data):
    if data is not None:
        cache[key] = (time.time(), data)
    return data

# Disable SSL warnings for the JSON API
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)
CORS(app)

# --- CONFIG ---
# 1. Primary API (JSON Wrapper) - Stable & Fast
JSON_BASE = "https://ridebt.org/index.php?option=com_ajax&module=bt_map&format=json"

# 2. Secondary API (Raw XML) - Used ONLY for Station Board
XML_BASE = "http://216.252.195.248/webservices/bt4u_webservice.asmx"

# --- STEP 1: OPTIMIZED TELEMETRY ENGINE ---

def fetch_streets_batch(route_keys):
    """Surgical fetch for a small batch of routes to prevent timeouts."""
    url = f"{STREETS_BASE}RouteMap/GetVehicles"
    headers = {
        "X-Requested-With": "XMLHttpRequest", 
        "RequestVerificationToken": STREETS_TOKEN, 
        "Content-Type": "application/json"
    }
    try:
        resp = session.post(url, headers=headers, json={"routeKeys": route_keys}, timeout=5)
        return resp.json() if resp.status_code == 200 else []
    except:
        return []

def fetch_streets_telemetry():
    """Threaded prober that fetches telemetry in small batches for speed and stability."""
    # Split GUIDs into batches of 5 to prevent server-side 'Read timed out'
    all_guids = [guid for sublist in STREETS_GUID_MAP.values() for guid in sublist]
    batches = [all_guids[i:i + 5] for i in range(0, len(all_guids), 5)]
    
    telemetry_dict = {}
    print(f"🚀 Probing StreetsWeb in {len(batches)} parallel batches...")
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(fetch_streets_batch, batches))
        
    for raw_data in results:
        if not raw_data: continue
        for route_obj in raw_data:
            for direction in route_obj.get('vehiclesByDirections', []):
                # --- Inside telemetry_worker loop ---
                # --- Inside telemetry_worker loop ---
                for bus in direction.get('vehicles', []):
                    loc = bus.get('location', {})
                    last_gps_str = loc.get('lastGpsDate', "")
                    
                    # NEW: Robust timestamp conversion
                    streets_ts = 0
                    if last_gps_str:
                        try:
                            dt_obj = datetime.fromisoformat(last_gps_str)
                            streets_ts = int(dt_obj.timestamp() * 1000)
                        except Exception as e:
                            print(f"⚠️ Timestamp error for bus {bus['name']}: {e}")

                    telemetry_dict[bus['name']] = {
                        "lat": loc.get('latitude'),
                        "lon": loc.get('longitude'),
                        "heading": loc.get('heading'),
                        "speed": loc.get('speed', 0),
                        "pax": bus.get('passengersOnboard', 0),
                        "cap": bus.get('passengerCapacity', 70),
                        "ts": streets_ts,
                                    "directionName": direction.get('directionName', 'Loop'),
                                    "routeKey": bus.get('routeKey'),
                                    "isExtra": bus.get('isExtraTrip', False),
                                    "amenities": bus.get('amenities', []) # AC, Bike Racks, etc.
                    }
    
    print(f"✅ Telemetry Sync Complete: {len(telemetry_dict)} units found.")
    return telemetry_dict

def fetch_json(method_name, extra_params=None):
    """Fetch from the stable JSON API (ridebt.org)"""
    try:
        headers = {
    'User-Agent': 'BTMap-App-Developer-Ref-Blacksburg-Transit',
    'Referer': 'https://ridebt.org/live-map',
    'X-Requested-With': 'XMLHttpRequest'
}
        params = {'method': method_name, 'Itemid': '189'}
        if extra_params: params.update(extra_params)
        
        resp = session.get(JSON_BASE, params=params, headers=headers, verify=False, timeout=4)
        return resp.json() if resp.status_code == 200 else None
    except Exception as e:
        print(f"❌ API Connection Error ({method_name}): {e}")
        return None

def fetch_xml_departures(stop_code):
    try:
        url = f"{XML_BASE}/GetNextDeparturesForStop"
        params = {'routeShortName': '', 'noOfTrips': '20', 'stopCode': stop_code}
        
        resp = session.get(url, params=params, timeout=5)
        if resp.status_code != 200: return []
            
        root = ET.fromstring(resp.content)
        departures = []
        
        def get_val(elem, tag):
            found = elem.find(tag)
            return found.text if found is not None else ""

        for node in root.iter():
            if "NextDepartures" in node.tag:
                route = get_val(node, "RouteShortName")
                dest = get_val(node, "PatternName")
                
                # GRAB BOTH TIMES
                adj_time = get_val(node, "AdjustedDepartureTime")   # Live estimate
                sched_time = get_val(node, "ScheduledDepartureTime") # Printed schedule
                
                if route and adj_time:
                    departures.append({
                        "route": route,
                        "dest": dest,
                        "time": adj_time,       # Keep this for your "Next Run" sorting
                        "scheduled": sched_time # Send this for "Late/Early" math
                    })
        return departures

    except Exception as e:
        print(f"❌ XML Parsing Error: {e}")
        return []

def fetch_xml_routes():
    try:
        url = f"{XML_BASE}/GetScheduledRoutes"
        params = {
            'stopCode': '', 
            'serviceDate': datetime.now().strftime("%Y-%m-%d")
        }
        
        resp = session.get(url, params=params, timeout=10)
        if resp.status_code != 200: return {}

        root = ET.fromstring(resp.content)
        routes_meta = {}

        def find_val(parent, tag_name):
            for child in parent:
                if child.tag.split('}')[-1] == tag_name:
                    return child.text
            return ""

        for node in root.iter():
            tag_local = node.tag.split('}')[-1]
            
            # The XML container is 'ScheduledRoutes'
            if tag_local == 'ScheduledRoutes':
                # Matching your RAW XML tags exactly now:
                s_name  = find_val(node, 'RouteShortName') # Capital N
                f_name  = find_val(node, 'RouteName')      # Not RouteFullname
                color   = find_val(node, 'RouteColor')     # Not PrimaryColor
                service = find_val(node, 'ServiceLevel')
                pdf     = find_val(node, 'RouteURL')       # Not SchedulePDF

                if s_name:
                    routes_meta[s_name] = {
                        "name": f_name or s_name,
                        "color": f"#{color}" if color else "#630031",
                        "serviceLevel": (service or "FULL SERVICE").upper(),
                        "pdf": pdf or "#"
                    }

        print(f"✅ Success! Parsed {len(routes_meta)} routes with correct casing.")
        return routes_meta

    except Exception as e:
        print(f"❌ Error: {e}")
        return {}

@app.route('/')
def index():
    return send_from_directory('.', 'bustracker.html')

@app.route('/bustracker.html')
def serve_file():
    return send_from_directory('.', 'bustracker.html')

# --- ENDPOINTS ---
@app.route('/log_session', methods=['POST'])
def log_session():
    data = request.json
    u_id = data.get('userId')
    s_id = data.get('sessionId')
    now = datetime.now()
    
    conn = sqlite3.connect('usage.db')
    c = conn.cursor()
    # Record new visit or update current one
    c.execute("INSERT OR REPLACE INTO sessions VALUES (?, ?, ?, ?)", 
              (s_id, u_id, now, now))
    conn.commit()
    conn.close()
    return jsonify({"status": "ok"})

@app.route('/heartbeat', methods=['POST'])
def heartbeat():
    s_id = request.json.get('sessionId')
    conn = sqlite3.connect('usage.db')
    c = conn.cursor()
    c.execute("UPDATE sessions SET last_seen = ? WHERE session_id = ?", 
              (datetime.now(), s_id))
    conn.commit()
    conn.close()
    return jsonify({"status": "pulsed"})

@app.route('/stats')
def get_usage_stats():
    conn = sqlite3.connect('usage.db')
    c = conn.cursor()
    # Total Views
    c.execute("SELECT COUNT(*) FROM sessions")
    total_views = c.fetchone()[0]
    # Unique Users
    c.execute("SELECT COUNT(DISTINCT user_id) FROM sessions")
    unique_users = c.fetchone()[0]
    # Average Time Spent (in minutes)
    c.execute("SELECT AVG((julianday(last_seen) - julianday(start_time)) * 1440) FROM sessions")
    avg_time = c.fetchone()[0] or 0
    conn.close()
    
    return f"<h1>BTMap Stats</h1><p>Unique Users: {unique_users}</p><p>Total Views: {total_views}</p><p>Avg Time on Map: {round(avg_time, 1)} mins</p>"

@app.route('/buses')
def get_buses():
    data, expired = get_cached_data('buses', 1)
    if data and not expired: return jsonify(data)
    
    standard_json = fetch_json("getBuses")
    standard_buses = standard_json.get('data', []) if standard_json else []
    
    with TELEMETRY_LOCK:
        telemetry = LATEST_TELEMETRY.copy()
        
    # Detect if Legacy is failing (0 or 1 bus while StreetsWeb sees many)
    is_legacy_failing = len(standard_buses) < 2 and len(telemetry) > 2
    final_fleet = []

    if not is_legacy_failing:
        # --- NORMAL HYBRID MODE: ENRICHED ---
        current_active_routes = {sb.get('routeId') for sb in standard_buses if sb.get('routeId')}
        with TELEMETRY_LOCK:
            global ACTIVE_ROUTE_IDS
            ACTIVE_ROUTE_IDS = current_active_routes

        for sb in standard_buses:
            bus_id = sb.get('id')
            sb['pattern'] = sb.get('patternName', 'Loop')
            if bus_id in telemetry:
                tel = telemetry[bus_id]
                # Inject High-Fi Physics
                sb['states'][0].update({
                    'latitude': tel['lat'], 'longitude': tel['lon'],
                    'direction': int(tel['heading']), 'speed': tel['speed'],
                    'version': tel['ts']
                })
                # Inject NEW MyRide-only features
                sb.update({
                    'pax': tel['pax'], 
                    'capacity': tel['cap'],
                    'isExtraTrip': tel['isExtra'],
                    'amenities': tel['amenities']
                })
            final_fleet.append(sb)
    else:
        # --- DISASTER RECOVERY MODE: AUTONOMOUS ---
        print("🚨 LEGACY API OFFLINE: Generating fleet from MyRide Telemetry...")
        for bus_id, tel in telemetry.items():
            # Identify the route using our reverse GUID map
            route_id = GUID_TO_ROUTE.get(tel['routeKey'], "UNK")
            
            final_fleet.append({
                "id": bus_id,
                "routeId": route_id,
                # During recovery, we use MyRide's direction name
                "pattern": tel['directionName'], 
                "pax": tel['pax'],
                "capacity": tel['cap'],
                "isExtraTrip": tel['isExtra'],
                "amenities": tel['amenities'],
                "states": [{
                    "latitude": tel['lat'], "longitude": tel['lon'],
                    "direction": int(tel['heading']), "speed": tel['speed'],
                    "version": tel['ts'], 
                    "isBusAtStop": "Y" if tel['speed'] < 0.1 else "N", 
                    "isTimePoint": "N" # Cannot infer timepoint without Legacy
                }]
            })

    return jsonify(set_cached_data('buses', {"data": final_fleet}))

@app.route('/summary')
def get_fleet_summary():
    """Bypasses the 8514 bug by checking the global system summary."""
    data = fetch_json("getSummary")
    if data and 'data' in data:
        # Sort by most recent update to see if the system is 'live'
        return jsonify(data['data'])
    return jsonify([])

@app.route('/active_patterns')
def get_active_patterns():
    global PATTERN_CACHE
    
    # 1. Check if we have valid cached data
    now = time.time()
    if PATTERN_CACHE["data"] and (now - PATTERN_CACHE["last_updated"] < CACHE_TTL):
        print("💾 Serving Patterns from Server Cache")
        return jsonify(PATTERN_CACHE["data"])

    # 2. If no cache, fetch fresh data
    route_ids = ["CAS", "BMR", "PRG", "SME", "HDG", "SMA", "HWA", "HWC", "HWB", 
                 "BLU", "PHD", "HXP", "PHB", "UCB", "CRB", "CRC", "TCP", "NMG", 
                 "TCR", "SMS", "TTT", "GRN"]
    
    today = datetime.now().strftime("%m/%d/%Y")
    all_patterns = []

    def fetch_route_patterns(r_id):
        patterns = []
        try:
            url = f"{XML_BASE}/GetPatternNamesForDate"
            params = {'routeShortName': r_id, 'serviceDate': today}
            resp = session.get(url, params=params, timeout=3)
            if resp.status_code == 200:
                root = ET.fromstring(resp.content)
                for node in root.findall('.//PatternNames'):
                    p_name = node.findtext('PatternName')
                    
                    # --- THE FILTER: Exclude FR (First Runs) and Duplicates ---
                    if not p_name: continue
                    p_clean = p_name.upper()
                    if " FR" in p_clean or p_clean.endswith(" FR") or p_clean.endswith("_"):
                        continue
                    
                    patterns.append({"rId": r_id, "pName": p_name})
        except: pass
        return patterns

    print(f"⚡ Parallel Fetching Fresh Patterns for {today}...")
    with ThreadPoolExecutor(max_workers=20) as executor:
        results = executor.map(fetch_route_patterns, route_ids)
        for batch in results:
            all_patterns.extend(batch)

    # 3. Update Cache
    PATTERN_CACHE["data"] = all_patterns
    PATTERN_CACHE["last_updated"] = now
    
    print(f"✅ Cache Updated: Found {len(all_patterns)} clean patterns.")
    return jsonify(all_patterns)
    
@app.route('/routes')
def get_routes_list():
    # 1. Check Cache
    cached_data, is_expired = get_cached_data('route_meta', 3600)
    if cached_data and not is_expired: 
        return jsonify(cached_data)
    
    # 2. Fetch Fresh Data
    meta = fetch_xml_routes()
    if meta:
        set_cached_data('route_meta', meta)
        return jsonify(meta)
    
    # 3. Fallback to stale cache if API is down
    return jsonify(cached_data if cached_data else {})

@app.route('/alerts')
def get_alerts():
    # Cache for 5 minutes.
    data, expired = get_cached_data('alerts', 300)
    if data and not expired: return jsonify(data)
    
    new_data = fetch_json("GetActiveAlerts")
    if new_data and 'data' in new_data:
        return jsonify(set_cached_data('alerts', new_data['data']))
    
    return jsonify(data if data else [])

@app.route('/nearest')
def get_nearest():
    # Use Stable JSON
    lat = request.args.get('lat')
    lon = request.args.get('lon')
    data = fetch_json("GetNearestStops", {'latitude': lat, 'longitude': lon})
    return jsonify(data['data'] if data and 'data' in data else [])

@app.route('/stops')
def get_stops():
    route = request.args.get('route')
    cache_key = f"stops_{route}"
    
    # Cache stop lists for 1 hour
    cached = get_cached_data(cache_key, 7000)
    if cached: return jsonify(cached)
    
    today = datetime.now().strftime("%Y-%m-%d")
    data = fetch_json("GetScheduledStopInfo", {'routeShortName': route, 'serviceDate': today})
    
    stops = []
    if data and 'data' in data:
        for item in data['data']:
            stops.append({
                "name": item.get('stopName'),
                "code": item.get('stopCode'),
                "lat": float(item.get('latitude')),
                "lon": float(item.get('longitude'))
            })
        return jsonify(set_cached_data(cache_key, stops))
    return jsonify([])

@app.route('/route_shape')
def get_shape():
    pattern = request.args.get('pattern')
    cache_key = f"shape_{pattern}"
    
    data, expired = get_cached_data(cache_key, 86400)
    if data and not expired: return jsonify(data)
    
    new_data = fetch_json("getPatternPoints", {'patternName': pattern})
    if new_data and 'data' in new_data:
        points = [[float(i['latitude']), float(i['longitude'])] for i in new_data['data']]
        # Use the Legacy JSON's isTimePoint check which is much more accurate for shapes
        stops = [{
            "name": i['patternPointName'], 
            "code": i['stopCode'],
            "lat": float(i['latitude']), 
            "lon": float(i['longitude']),
            "isTimePoint": i['isTimePoint'] # Legacy "Y" or "N"
        } for i in new_data['data'] if i['isBusStop'] == 'Y']
        
        return jsonify(set_cached_data(cache_key, {"shape": points, "stops": stops}))
    
    return jsonify({"shape": [], "stops": []})

@app.route('/departures')
def get_departures():
    code = request.args.get('code')
    cache_key = f"dep_{code}"
    
    # High frequency data. Cache for 20s.
    data, expired = get_cached_data(cache_key, 5)
    if data and not expired: return jsonify(data)
    
    new_data = fetch_xml_departures(code)
    if new_data: # If fetch_xml returns a list (even empty), it's a success
        return jsonify(set_cached_data(cache_key, new_data))
    
    # FALLBACK: Serve stale arrival times if XML API is down
    return jsonify(data if data else [])

if __name__ == '__main__':
    print("🚀 Proxy V19 (Hybrid Engine) Running...")
    app.run(port=5000)
