from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
import xml.etree.ElementTree as ET
import urllib3
from datetime import datetime

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
            'User-Agent': 'Mozilla/5.0',
            'Referer': 'https://ridebt.org/live-map',
            'X-Requested-With': 'XMLHttpRequest'
        }
        params = {'method': method_name, 'Itemid': '189'}
        if extra_params: params.update(extra_params)
        
        resp = requests.get(JSON_BASE, params=params, headers=headers, verify=False, timeout=5)
        return resp.json() if resp.status_code == 200 else None
    except Exception as e:
        print(f"❌ JSON Error {method_name}: {e}")
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

@app.route('/buses')
def get_buses():
    # Use Stable JSON
    data = fetch_json("getBuses")
    return jsonify(data if data else {"data": []})

@app.route('/routes')
def get_routes_list():
    # Use Stable JSON
    data = fetch_json("getRoutes")
    routes = {}
    if data and 'data' in data:
        for r in data['data']:
            routes[r['routeShortName']] = r['routeLongName']
    return jsonify(routes)

@app.route('/alerts')
def get_alerts():
    # Use Stable JSON (Fixes the 500 error you saw!)
    data = fetch_json("GetActiveAlerts")
    if data and 'data' in data:
        return jsonify(data['data'])
    return jsonify([])

@app.route('/nearest')
def get_nearest():
    # Use Stable JSON
    lat = request.args.get('lat')
    lon = request.args.get('lon')
    data = fetch_json("GetNearestStops", {'latitude': lat, 'longitude': lon})
    return jsonify(data['data'] if data and 'data' in data else [])

@app.route('/stops')
def get_stops():
    # Use Stable JSON
    route = request.args.get('route')
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
    return jsonify(stops)

@app.route('/route_shape')
def get_shape():
    # Use Stable JSON (getPatternPoints works great)
    pattern = request.args.get('pattern')
    data = fetch_json("getPatternPoints", {'patternName': pattern})
    
    points = []
    stops = []
    
    if data and 'data' in data:
        for item in data['data']:
            lat = float(item.get('latitude'))
            lon = float(item.get('longitude'))
            points.append([lat, lon])
            if item.get('isBusStop') == 'Y':
                stops.append({
                    "name": item.get('patternPointName'),
                    "code": item.get('stopCode'),
                    "lat": lat, "lon": lon,
                    "isTimePoint": item.get('isTimePoint')
                })
    return jsonify({"shape": points, "stops": stops})

@app.route('/departures')
def get_departures():
    # HYBRID SWITCH: Use Raw XML for this ONE feature
    code = request.args.get('code')
    return jsonify(fetch_xml_departures(code))

if __name__ == '__main__':
    print("🚀 Proxy V19 (Hybrid Engine) Running...")
    app.run(port=5000)