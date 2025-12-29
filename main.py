import time
import asyncio
import httpx
import csv
import math
import pickle
import os
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from google.transit import gtfs_realtime_pb2

GTFS_RT_VEHICLE_POSITIONS = "https://gtfs.sofiatraffic.bg/api/v1/vehicle-positions"
GTFS_RT_TRIP_UPDATES = "https://gtfs.sofiatraffic.bg/api/v1/trip-updates"
GTFS_STATIC_PATH = "./gtfs"
GTFS_CACHE_FILE = "gtfs_cache.pkl"

OCCUPANCY_MAP = {
    0: "EMPTY",
    1: "MANY_SEATS_AVAILABLE",
    2: "FEW_SEATS_AVAILABLE",
    3: "STANDING_ROOM_ONLY",
    4: "CRUSHED_STANDING_ROOM_ONLY",
    5: "FULL",
    6: "NOT_ACCEPTING_PASSENGERS",
}

# ---------------- GTFS STATIC LOAD ----------------

stops_map = {}
trip_stops = {}
trip_last_stop = {}
stop_times_map = {}


def time_to_seconds(time_str):
    try:
        h, m, s = map(int, time_str.split(':'))
        return h * 3600 + m * 60 + s
    except:
        return 0


def load_gtfs_static():
    global stops_map, trip_stops, trip_last_stop, stop_times_map

    if os.path.exists(GTFS_CACHE_FILE):
        print("Loading GTFS data from cache...")
        try:
            with open(GTFS_CACHE_FILE, "rb") as f:
                data = pickle.load(f)
                stops_map = data["stops_map"]
                trip_stops = data["trip_stops"]
                trip_last_stop = data["trip_last_stop"]
                stop_times_map = data["stop_times_map"]
            return
        except Exception as e:
            print(f"Failed to load cache: {e}. Parsing CSVs...")

    print("Parsing GTFS CSVs...")

    with open(f"{GTFS_STATIC_PATH}/stops.txt", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            stops_map[r["stop_id"]] = int(
                "".join(filter(str.isdigit, r["stop_id"])) or 0
            )

    with open(f"{GTFS_STATIC_PATH}/stop_times.txt", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            tid = r["trip_id"]
            trip_stops.setdefault(tid, []).append(r)

            key = (tid, r["stop_id"])
            stop_times_map[key] = time_to_seconds(r["arrival_time"])

    for tid, stops in trip_stops.items():
        stops.sort(key=lambda x: int(x["stop_sequence"]))
        trip_last_stop[tid] = stops[-1]["stop_id"]

    print("Saving GTFS data to cache...")
    with open(GTFS_CACHE_FILE, "wb") as f:
        pickle.dump({
            "stops_map": stops_map,
            "trip_stops": trip_stops,
            "trip_last_stop": trip_last_stop,
            "stop_times_map": stop_times_map
        }, f)


load_gtfs_static()

# ---------------- FASTAPI ----------------

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------- DELAY CALCULATION ----------------

async def fetch_trip_delays():
    delays = {}

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(GTFS_RT_TRIP_UPDATES)

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(r.content)

        for entity in feed.entity:
            if not entity.HasField('trip_update'):
                continue

            tu = entity.trip_update
            trip_id = tu.trip.trip_id
            current_delay = 0

            if tu.stop_time_update:
                first_update = tu.stop_time_update[0]

                if first_update.HasField('arrival') and first_update.arrival.HasField('delay'):
                    current_delay = first_update.arrival.delay
                elif first_update.HasField('departure') and first_update.departure.HasField('delay'):
                    current_delay = first_update.departure.delay

                elif first_update.HasField('arrival') and first_update.arrival.HasField('time'):
                    estimated_time = first_update.arrival.time
                    stop_id = first_update.stop_id

                    key = (trip_id, stop_id)

                    if key not in stop_times_map:
                        stop_id_digits = "".join(filter(str.isdigit, stop_id))
                        key = (trip_id, stop_id_digits)

                    if key in stop_times_map:
                        scheduled_seconds = stop_times_map[key]

                        est_struct = time.localtime(estimated_time)
                        midnight_unix = time.mktime((
                            est_struct.tm_year, est_struct.tm_mon, est_struct.tm_mday,
                            0, 0, 0, 0, 0, -1
                        ))

                        scheduled_unix = midnight_unix + scheduled_seconds
                        current_delay = int(estimated_time - scheduled_unix)


                        if abs(current_delay) > 43200:
                            current_delay = 0

            delays[trip_id] = current_delay

    except Exception as e:
        print(f"Error fetching trip delays: {e}")

    return delays


# ---------------- FETCH VEHICLES ----------------

async def fetch_vehicles():
    # Fetch both feeds in parallel
    async with httpx.AsyncClient(timeout=10) as client:
        vehicle_response, delay_task = await asyncio.gather(
            client.get(GTFS_RT_VEHICLE_POSITIONS),
            fetch_trip_delays(),
            return_exceptions=True
        )

    delays = delay_task if isinstance(delay_task, dict) else {}

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(vehicle_response.content)

    vehicles = []

    for e in feed.entity:
        if not e.HasField("vehicle"):
            continue

        v = e.vehicle
        if not v.position or not v.trip:
            continue

        vehicle_id = v.vehicle.id if v.vehicle and v.vehicle.id else ""
        inv_number = int("".join(filter(str.isdigit, vehicle_id))) if vehicle_id else None

        stop_id = v.stop_id if v.stop_id else None
        trip_id = v.trip.trip_id


        car_sequence = v.current_stop_sequence if v.HasField("current_stop_sequence") else None


        delay_seconds = delays.get(trip_id, 0)

        if delay_seconds > 0:
            delay_minutes = math.ceil(delay_seconds / 60) - 2
        elif delay_seconds < 0:
            delay_minutes = math.floor(delay_seconds / 60) - 2
        else:
            delay_minutes = -2

        vehicles.append({
            "trip": trip_id,
            "coords": [v.position.latitude, v.position.longitude],
            "speed": int(v.position.speed or 0),
            "scheduled_time": car_sequence,
            "next_stop": stops_map.get(stop_id),
            "destination_stop": stops_map.get(trip_last_stop.get(trip_id)),
            "occupancy": OCCUPANCY_MAP.get(v.occupancy_status) if v.HasField("occupancy_status") else None,
            "cgm_id": vehicle_id,
            "inv_number": inv_number,
            "cgm_route_id": v.trip.route_id,
            "car": car_sequence,
            "timestamp": v.timestamp if v.timestamp else int(time.time()),
            "delay": delay_minutes
        })

    return vehicles


# ---------------- WEBSOCKET ----------------

@app.websocket("/v2/livemap/")
async def websocket_livemap(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            data = await fetch_vehicles()
            print(f"Sending {len(data)} vehicles (with delays)")
            await ws.send_json(data)
            await asyncio.sleep(5)
    except Exception as e:
        print("WS closed:", e)
