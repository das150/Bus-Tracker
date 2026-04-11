from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
import xml.etree.ElementTree as ET
import urllib3
from functools import lru_cache
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import sqlite3
from datetime import datetime, timedelta

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
        
        resp = requests.get(url, params=params, timeout=5)
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
    data, expired = get_cached_data('buses', 2)
    if data and not expired: return jsonify(data)
    
    new_data = fetch_json("getBuses")
    if new_data:
        return jsonify(set_cached_data('buses', new_data))
    
    # FALLBACK: If BT is down, serve stale cache
    if data:
        print("⚠️ Serving Stale Bus Positions (API Down)")
        return jsonify(data)
    return jsonify({"data": []})

@app.route('/routes')
def get_routes_list():
    # Cache the list of routes for 24 hours (86400 seconds)
    cached = get_cached_data('route_list', 86400)
    if cached: return jsonify(cached)
    
    data = fetch_json("getRoutes")
    routes = {}
    if data and 'data' in data:
        for r in data['data']:
            routes[r['routeShortName']] = r['routeLongName']
        return jsonify(set_cached_data('route_list', routes))
    return jsonify({})

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
    
    # Shapes never really change. Cache for 24 hours.
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
        
        result = {"shape": points, "stops": stops}
        return jsonify(set_cached_data(cache_key, result))
    
    return jsonify(data if data else {"shape": [], "stops": []})

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
