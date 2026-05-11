from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import requests
import xml.etree.ElementTree as ET
import urllib3
import time
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import sqlite3
from datetime import datetime
import threading

# Disable SSL warnings for the JSON API
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)
CORS(app)

# --- GLOBAL MEMORY STORES (The "Decouplers") ---
# These store the latest data so user requests never hit the BT servers directly
LATEST_TELEMETRY = {}
LATEST_LEGACY_BUSES = []
TELEMETRY_LOCK = threading.Lock()
LEGACY_LOCK = threading.Lock()

KNOWN_ACTIVE_GUIDS = set()
SWEEP_COUNTER = 0

# Simple Cache Storage for other endpoints
cache = {}

def get_cached_data(key, max_age=15):
    if key in cache:
        timestamp, data = cache[key]
        if (time.time() - timestamp) <= max_age:
            return data, False
    return None, True

def set_cached_data(key, data):
    if data is not None:
        cache[key] = (time.time(), data)
    return data

# --- NETWORK CONFIGURATION ---
session = requests.Session()
# Aggressive retry logic for stability under load
retries = Retry(total=2, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=retries)
session.mount('https://', adapter)
session.mount('http://', adapter)

# --- API CONSTANTS ---
JSON_BASE = "https://ridebt.org/index.php?option=com_ajax&module=bt_map&format=json"
XML_BASE = "http://216.252.195.248/webservices/bt4u_webservice.asmx"
STREETS_BASE = "http://216.252.195.248/StreetsWeb/MyRide/"
STREETS_TOKEN = "kDgRUwUoLE7dG9end2_3pvoa1ZIfFVH02w6YPbwg6djUqJKj068AHh4SHgBDSmZ_2a2R9YaZyLhItGZJu3UofTt0-MDmNZi0ejTMYrf7zII1:D9u0NsH3u8I9JHiqYQO4bNr6iU7E0RgMB-spM5-UQ30zzMR-ZHzlHKi6ZTcUU-GAJn0XUvjDMp7vcDQHmIIcVBlpSkPLWWflpL92eYLDtN01"

# Exhaustive GUID Map
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
GUID_TO_ROUTE = {guid: r_id for r_id, guids in STREETS_GUID_MAP.items() for guid in guids}
ALL_GUIDS = [guid for sublist in STREETS_GUID_MAP.values() for guid in sublist]

# --- BACKGROUND WORKER 1: LEGACY API POLLED ONCE PER SECOND ---
def legacy_worker():
    """Polls the Legacy JSON API and stores it in memory. Prevents 100 users from causing 100 API hits."""
    print("🧵 Legacy Worker Active...")
    global LATEST_LEGACY_BUSES
    
    headers = {
        'User-Agent': 'BTMap-App-Developer-Ref-Blacksburg-Transit',
        'Referer': 'https://ridebt.org/live-map',
        'X-Requested-With': 'XMLHttpRequest'
    }
    
    while True:
        try:
            resp = session.get(f"{JSON_BASE}&method=getBuses", headers=headers, verify=False, timeout=3)
            if resp.status_code == 200:
                data = resp.json()
                if data and 'data' in data:
                    with LEGACY_LOCK:
                        LATEST_LEGACY_BUSES = data['data']
        except Exception as e:
            pass # Fail silently and keep last known good data
        
        time.sleep(1.0) # Strictly 1 request per second

# --- BACKGROUND WORKER 2: ADAPTIVE TELEMETRY POLLED ONCE PER SECOND ---
def telemetry_worker():
    """Polls the MyRide API using the Adaptive Active-Set method."""
    print("🧵 Adaptive Telemetry Worker Active...")
    global LATEST_TELEMETRY, KNOWN_ACTIVE_GUIDS, SWEEP_COUNTER
    
    while True:
        try:
            target_guids = []
            is_wide_sweep = False

            # Every 30 loops, Wide Sweep
            if SWEEP_COUNTER % 30 == 0 or not KNOWN_ACTIVE_GUIDS:
                target_guids = ALL_GUIDS
                is_wide_sweep = True
            else:
                target_guids = list(KNOWN_ACTIVE_GUIDS)

            url = f"{STREETS_BASE}RouteMap/GetVehicles"
            headers = {
                "X-Requested-With": "XMLHttpRequest", 
                "RequestVerificationToken": STREETS_TOKEN, 
                "Content-Type": "application/json"
            }
            
            timeout_val = 10 if is_wide_sweep else 4
            resp = session.post(url, headers=headers, json={"routeKeys": target_guids}, timeout=timeout_val)
            
            if resp.status_code == 200:
                raw_data = resp.json()
                new_data = {}
                found_guids = set()

                for route_obj in raw_data:
                    for dir_obj in route_obj.get('vehiclesByDirections', []):
                        for bus in dir_obj.get('vehicles', []):
                            loc = bus.get('location', {})
                            last_gps = loc.get('lastGpsDate', "")
                            r_key = bus.get('routeKey')
                            
                            if r_key: found_guids.add(r_key)
                            
                            ts = 0
                            if last_gps:
                                try:
                                    dt_obj = datetime.fromisoformat(last_gps.replace('Z', '+00:00'))
                                    ts = int(dt_obj.timestamp() * 1000)
                                except: pass

                            new_data[bus['name']] = {
                                "lat": loc.get('latitude'), "lon": loc.get('longitude'),
                                "heading": loc.get('heading'), "speed": loc.get('speed', 0),
                                "pax": bus.get('passengersOnboard', 0), "cap": bus.get('passengerCapacity', 70),
                                "ts": ts, "routeKey": r_key,
                                "isExtra": bus.get('isExtraTrip', False),
                                "amenities": bus.get('amenities', [])
                            }
                
                with TELEMETRY_LOCK:
                    LATEST_TELEMETRY.update(new_data)
                    # Cleanup ghosts older than 3 minutes
                    now_ms = int(time.time() * 1000)
                    LATEST_TELEMETRY = {k: v for k, v in LATEST_TELEMETRY.items() if (now_ms - v['ts']) < 180000}

                if is_wide_sweep and found_guids:
                    KNOWN_ACTIVE_GUIDS = found_guids

            SWEEP_COUNTER += 1
            time.sleep(1.0)
            
        except Exception as e:
            time.sleep(2.0)

# Start Both Background Workers
threading.Thread(target=legacy_worker, daemon=True).start()
threading.Thread(target=telemetry_worker, daemon=True).start()

def init_tracker_db():
    conn = sqlite3.connect('usage.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS sessions 
                 (session_id TEXT PRIMARY KEY, user_id TEXT, 
                  start_time DATETIME, last_seen DATETIME)''')
    conn.commit()
    conn.close()

init_tracker_db()

@app.route('/')
def index():
    return send_from_directory('.', 'bustracker.html')

@app.route('/bustracker.html')
def serve_file():
    return send_from_directory('.', 'bustracker.html')

@app.route('/log_session', methods=['POST'])
def log_session():
    data = request.json
    u_id = data.get('userId')
    s_id = data.get('sessionId')
    now = datetime.now()
    
    conn = sqlite3.connect('usage.db')
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO sessions VALUES (?, ?, ?, ?)", (s_id, u_id, now, now))
    conn.commit()
    conn.close()
    return jsonify({"status": "ok"})

@app.route('/heartbeat', methods=['POST'])
def heartbeat():
    s_id = request.json.get('sessionId')
    conn = sqlite3.connect('usage.db')
    c = conn.cursor()
    c.execute("UPDATE sessions SET last_seen = ? WHERE session_id = ?", (datetime.now(), s_id))
    conn.commit()
    conn.close()
    return jsonify({"status": "pulsed"})

# --- THE HIGH-CONCURRENCY ENDPOINT ---
@app.route('/buses')
def get_buses():
    """
    This endpoint no longer makes any external HTTP requests. 
    It serves data directly from RAM. It can handle thousands of requests per second.
    """
    # 1. Grab data from RAM Locks safely
    with LEGACY_LOCK:
        standard_buses = list(LATEST_LEGACY_BUSES)
    
    with TELEMETRY_LOCK:
        telemetry = dict(LATEST_TELEMETRY)
        
    is_legacy_failing = len(standard_buses) < 2 and len(telemetry) > 2
    final_fleet = []

    if not is_legacy_failing:
        for sb in standard_buses:
            bus_id = sb.get('id')
            sb['pattern'] = sb.get('patternName', 'Loop')
            
            if bus_id in telemetry:
                tel = telemetry[bus_id]
                legacy_state = sb['states'][0]
                
                legacy_ts = int(legacy_state.get('version', 0))
                myride_ts = int(tel['ts'])
                
                sb.update({
                    'pax': tel['pax'], 
                    'capacity': tel['cap'],
                    'isExtraTrip': tel['isExtra'],
                    'amenities': tel['amenities']
                })
                
                if myride_ts >= legacy_ts:
                    legacy_state.update({
                        'latitude': tel['lat'], 'longitude': tel['lon'],
                        'direction': int(tel['heading']), 'speed': tel['speed'],
                        'version': myride_ts
                    })

            final_fleet.append(sb)
    else:
        for bus_id, tel in telemetry.items():
            route_id = GUID_TO_ROUTE.get(tel['routeKey'], "UNK")
            final_fleet.append({
                "id": bus_id, "routeId": route_id, "pattern": "Loop", 
                "pax": tel['pax'], "capacity": tel['cap'],
                "isExtraTrip": tel['isExtra'], "amenities": tel['amenities'],
                "states": [{
                    "latitude": tel['lat'], "longitude": tel['lon'],
                    "direction": int(tel['heading']), "speed": tel['speed'],
                    "version": tel['ts'], 
                    "isBusAtStop": "Y" if tel['speed'] < 0.1 else "N", 
                    "isTimePoint": "N"
                }]
            })

    return jsonify({"data": final_fleet})

# --- HELPER FUNCTIONS FOR OTHER ENDPOINTS ---
def fetch_json(method_name, extra_params=None):
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
    except:
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
                adj_time = get_val(node, "AdjustedDepartureTime")
                sched_time = get_val(node, "ScheduledDepartureTime")
                
                if route and adj_time:
                    departures.append({
                        "route": route, "dest": dest,
                        "time": adj_time, "scheduled": sched_time
                    })
        return departures
    except:
        return []

def fetch_xml_routes():
    try:
        url = f"{XML_BASE}/GetScheduledRoutes"
        params = {'stopCode': '', 'serviceDate': datetime.now().strftime("%Y-%m-%d")}
        
        resp = session.get(url, params=params, timeout=10)
        if resp.status_code != 200: return {}

        root = ET.fromstring(resp.content)
        routes_meta = {}

        def find_val(parent, tag_name):
            for child in parent:
                if child.tag.split('}')[-1] == tag_name: return child.text
            return ""

        for node in root.iter():
            tag_local = node.tag.split('}')[-1]
            if tag_local == 'ScheduledRoutes':
                s_name  = find_val(node, 'RouteShortName')
                f_name  = find_val(node, 'RouteName')
                color   = find_val(node, 'RouteColor')
                service = find_val(node, 'ServiceLevel')
                pdf     = find_val(node, 'RouteURL')

                if s_name:
                    routes_meta[s_name] = {
                        "name": f_name or s_name,
                        "color": f"#{color}" if color else "#630031",
                        "serviceLevel": (service or "FULL SERVICE").upper(),
                        "pdf": pdf or "#"
                    }
        return routes_meta
    except:
        return {}

# --- STATIC/CACHED ENDPOINTS ---
@app.route('/summary')
def get_fleet_summary():
    data, expired = get_cached_data('summary', 10)
    if data and not expired: return jsonify(data)
    
    new_data = fetch_json("getSummary")
    if new_data and 'data' in new_data:
        return jsonify(set_cached_data('summary', new_data['data']))
    return jsonify(data if data else [])

@app.route('/routes')
def get_routes_list():
    cached_data, is_expired = get_cached_data('route_meta', 3600)
    if cached_data and not is_expired: return jsonify(cached_data)
    
    meta = fetch_xml_routes()
    if meta: return jsonify(set_cached_data('route_meta', meta))
    return jsonify(cached_data if cached_data else {})

@app.route('/alerts')
def get_alerts():
    data, expired = get_cached_data('alerts', 300)
    if data and not expired: return jsonify(data)
    
    new_data = fetch_json("GetActiveAlerts")
    if new_data and 'data' in new_data:
        return jsonify(set_cached_data('alerts', new_data['data']))
    return jsonify(data if data else [])

@app.route('/nearest')
def get_nearest():
    lat, lon = request.args.get('lat'), request.args.get('lon')
    data = fetch_json("GetNearestStops", {'latitude': lat, 'longitude': lon})
    return jsonify(data['data'] if data and 'data' in data else [])

@app.route('/stops')
def get_stops():
    route = request.args.get('route')
    cache_key = f"stops_{route}"
    
    cached, expired = get_cached_data(cache_key, 7000)
    if cached and not expired: return jsonify(cached)
    
    today = datetime.now().strftime("%Y-%m-%d")
    data = fetch_json("GetScheduledStopInfo", {'routeShortName': route, 'serviceDate': today})
    
    stops = []
    if data and 'data' in data:
        for item in data['data']:
            stops.append({
                "name": item.get('stopName'), "code": item.get('stopCode'),
                "lat": float(item.get('latitude')), "lon": float(item.get('longitude'))
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
        stops = [{
            "name": i['patternPointName'], "code": i['stopCode'],
            "lat": float(i['latitude']), "lon": float(i['longitude']),
            "isTimePoint": i['isTimePoint']
        } for i in new_data['data'] if i['isBusStop'] == 'Y']
        
        return jsonify(set_cached_data(cache_key, {"shape": points, "stops": stops}))
    return jsonify({"shape": [], "stops": []})

@app.route('/departures')
def get_departures():
    code = request.args.get('code')
    cache_key = f"dep_{code}"
    
    data, expired = get_cached_data(cache_key, 5)
    if data and not expired: return jsonify(data)
    
    new_data = fetch_xml_departures(code)
    if new_data is not None:
        return jsonify(set_cached_data(cache_key, new_data))
    return jsonify(data if data else [])

if __name__ == '__main__':
    print("🚀 Proxy V23 (High-Concurrency Edition) Running...")
    # Threaded mode allows Flask to process multiple concurrent user requests instantly
    app.run(port=5000, threaded=True)
