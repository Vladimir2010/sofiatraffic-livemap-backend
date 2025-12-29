# import time
# import asyncio
# import httpx
# import pandas as pd
# from fastapi import FastAPI, WebSocket
# from fastapi.middleware.cors import CORSMiddleware
# from google.transit import gtfs_realtime_pb2
#
# GTFS_RT_URL = "https://gtfs.sofiatraffic.bg/api/v1/vehicle-positions"
# GTFS_TU_URL = "https://gtfs.sofiatraffic.bg/api/v1/trip-updates"
#
# # =======================
# # LOAD STATIC GTFS
# # =======================
# stops = pd.read_csv("gtfs/stops.txt")
# stop_times = pd.read_csv("gtfs/stop_times.txt")
# trips = pd.read_csv("gtfs/trips.txt")
#
# stop_id_to_code = dict(zip(stops.stop_id, stops.stop_code))
#
# trip_to_last_stop = (
#     stop_times.sort_values("stop_sequence")
#     .groupby("trip_id")
#     .last()["stop_id"]
#     .to_dict()
# )
#
# trip_to_first_stop = (
#     stop_times.sort_values("stop_sequence")
#     .groupby("trip_id")
#     .first()["stop_id"]
#     .to_dict()
# )
#
# OCCUPANCY_MAP = {
#     0: "EMPTY",
#     1: "MANY_SEATS_AVAILABLE",
#     2: "FEW_SEATS_AVAILABLE",
#     3: "STANDING_ROOM_ONLY",
#     4: "CRUSHED_STANDING_ROOM_ONLY",
#     5: "FULL",
#     6: "NOT_ACCEPTING_PASSENGERS",
# }
#
# app = FastAPI()
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )
#
# # =======================
# # FETCH DELAYS
# # =======================
# async def fetch_trip_delays():
#     feed = gtfs_realtime_pb2.FeedMessage()
#     async with httpx.AsyncClient(timeout=10) as client:
#         r = await client.get(GTFS_TU_URL)
#         feed.ParseFromString(r.content)
#
#     delays = {}
#
#     for e in feed.entity:
#         if not e.HasField("trip_update"):
#             continue
#         tu = e.trip_update
#         if tu.stop_time_update:
#             delays[tu.trip.trip_id] = tu.stop_time_update[0].arrival.delay or 0
#
#     return delays
#
# # =======================
# # FETCH VEHICLES
# # =======================
# async def fetch_vehicles():
#     async with httpx.AsyncClient(timeout=10) as client:
#         r = await client.get(GTFS_RT_URL)
#         feed = gtfs_realtime_pb2.FeedMessage()
#         feed.ParseFromString(r.content)
#
#     delays = await fetch_trip_delays()
#     vehicles = []
#
#     for entity in feed.entity:
#         if not entity.HasField("vehicle"):
#             continue
#
#         v = entity.vehicle
#         if not v.position or not v.trip:
#             continue
#
#         vehicle_id = v.vehicle.id
#         inv_number = int("".join(filter(str.isdigit, vehicle_id)))
#
#         trip_id = v.trip.trip_id
#         route_id = v.trip.route_id
#
#         next_stop_code = stop_id_to_code.get(v.stop_id)
#         destination_stop_code = stop_id_to_code.get(
#             trip_to_last_stop.get(trip_id)
#         )
#
#         vehicles.append({
#             "trip": trip_id,
#             "coords": [
#                 v.position.latitude,
#                 v.position.longitude
#             ],
#             "speed": int(v.position.speed or 0),  # m/s -> km/h
#             "scheduled_time": v.current_stop_sequence,
#             "next_stop": next_stop_code,
#             "destination_stop": destination_stop_code,
#             "occupancy": OCCUPANCY_MAP.get(v.occupancy_status),
#             "cgm_id": vehicle_id,
#             "inv_number": inv_number,
#             "cgm_route_id": route_id,
#             "car": int(v.vehicle.label) if v.vehicle.label.isdigit() else None,
#             "timestamp": v.timestamp or int(time.time()),
#             "delay": delays.get(trip_id, 0)
#         })
#
#     return vehicles
#
# # =======================
# # WEBSOCKET
# # =======================
# @app.websocket("/v2/livemap/")
# async def websocket_livemap(ws: WebSocket):
#     await ws.accept()
#     try:
#         while True:
#             data = await fetch_vehicles()
#             await ws.send_json(data)
#             await asyncio.sleep(3)
#     except Exception as e:
#         print("WS closed:", e)
#
# import time
# import asyncio
# import csv
# import httpx
# from fastapi import FastAPI, WebSocket
# from fastapi.middleware.cors import CORSMiddleware
# from google.transit import gtfs_realtime_pb2
#
# GTFS_RT_URL = "https://gtfs.sofiatraffic.bg/api/v1/vehicle-positions"
# GTFS_STATIC_PATH = "./gtfs"
#
# REFRESH_SECONDS = 5  # <-- 5–10 сек, както искаш
#
# OCCUPANCY_MAP = {
#     0: "EMPTY",
#     1: "MANY_SEATS_AVAILABLE",
#     2: "FEW_SEATS_AVAILABLE",
#     3: "STANDING_ROOM_ONLY",
#     4: "CRUSHED_STANDING_ROOM_ONLY",
#     5: "FULL",
#     6: "NOT_ACCEPTING_PASSENGERS",
# }
#
# app = FastAPI()
#
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )
#
# # =========================
# # GTFS STATIC LOAD (ONCE)
# # =========================
#
# trips = {}
# stop_times = {}
#
# def load_gtfs_static():
#     global trips, stop_times
#
#     # trips.txt
#     with open(f"{GTFS_STATIC_PATH}/trips.txt", encoding="utf-8") as f:
#         for row in csv.DictReader(f):
#             trips[row["trip_id"]] = row
#
#     # stop_times.txt
#     with open(f"{GTFS_STATIC_PATH}/stop_times.txt", encoding="utf-8") as f:
#         for row in csv.DictReader(f):
#             stop_times.setdefault(row["trip_id"], []).append(row)
#
#     # sort stops by sequence
#     for t in stop_times:
#         stop_times[t].sort(key=lambda x: int(x["stop_sequence"]))
#
#     print(f"GTFS loaded: {len(trips)} trips")
#
# load_gtfs_static()
#
# # =========================
# # REALTIME FETCH
# # =========================
#
# async def fetch_vehicles():
#     async with httpx.AsyncClient(timeout=10) as client:
#         r = await client.get(GTFS_RT_URL)
#         feed = gtfs_realtime_pb2.FeedMessage()
#         feed.ParseFromString(r.content)
#
#     vehicles = []
#
#     for entity in feed.entity:
#         if not entity.HasField("vehicle"):
#             continue
#
#         v = entity.vehicle
#         trip_id = v.trip.trip_id if v.trip else None
#
#         if not trip_id or trip_id not in trips:
#             continue
#
#         static_trip = trips[trip_id]
#         stops = stop_times.get(trip_id, [])
#
#         # current stop index
#         idx = v.current_stop_sequence - 1 if v.HasField("current_stop_sequence") else None
#
#         next_stop = int(stops[idx]["stop_id"]) if idx is not None and idx < len(stops) else None
#         destination_stop = int(stops[-1]["stop_id"]) if stops else None
#
#         vehicle_id = v.vehicle.id if v.vehicle else ""
#
#         vehicles.append({
#             "trip": trip_id,
#             "coords": [
#                 v.position.latitude,
#                 v.position.longitude
#             ],
#             "speed": int((v.position.speed or 0) * 3.6),  # m/s → km/h
#             "scheduled_time": int(stops[idx]["arrival_time"].split(":")[1]) if idx is not None and idx < len(stops) else None,
#             "next_stop": next_stop,
#             "destination_stop": destination_stop,
#             "occupancy": OCCUPANCY_MAP.get(v.occupancy_status),
#             "cgm_id": vehicle_id,
#             "inv_number": int("".join(filter(str.isdigit, vehicle_id))) if vehicle_id else None,
#             "cgm_route_id": static_trip["route_id"],
#             "car": int(static_trip["block_id"]) if static_trip.get("block_id", "").isdigit() else None,
#             "timestamp": v.timestamp or int(time.time())
#         })
#
#     return vehicles
#
# # =========================
# # WEBSOCKET
# # =========================
#
# @app.websocket("/v2/livemap/")
# async def websocket_livemap(ws: WebSocket):
#     await ws.accept()
#     try:
#         while True:
#             data = await fetch_vehicles()
#             await ws.send_json(data)
#             await asyncio.sleep(REFRESH_SECONDS)
#     except Exception as e:
#         print("WS closed:", e)


import time
import asyncio
import httpx
import csv
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from google.transit import gtfs_realtime_pb2

GTFS_RT_URL = "https://gtfs.sofiatraffic.bg/api/v1/vehicle-positions"
GTFS_STATIC_PATH = "./gtfs"

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

def load_gtfs_static():
    global stops_map, trip_stops, trip_last_stop

    # stops.txt
    with open(f"{GTFS_STATIC_PATH}/stops.txt", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            stops_map[r["stop_id"]] = int(
                "".join(filter(str.isdigit, r["stop_id"])) or 0
            )

    # stop_times.txt
    with open(f"{GTFS_STATIC_PATH}/stop_times.txt", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            tid = r["trip_id"]
            trip_stops.setdefault(tid, []).append(r)

    for tid, stops in trip_stops.items():
        stops.sort(key=lambda x: int(x["stop_sequence"]))
        trip_last_stop[tid] = stops[-1]["stop_id"]

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

# ---------------- FETCH ----------------

async def fetch_vehicles():
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(GTFS_RT_URL)

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(r.content)

    vehicles = []

    for e in feed.entity:
        if not e.HasField("vehicle"):
            continue

        v = e.vehicle
        if not v.position or not v.trip:
            continue

        vehicle_id = v.vehicle.id or ""
        inv_number = int("".join(filter(str.isdigit, vehicle_id)) or 0)

        stop_id = v.stop_id if v.stop_id else None

        vehicles.append({
            "trip": v.trip.trip_id,
            "coords": [v.position.latitude, v.position.longitude],
            "speed": int(v.position.speed or 0),  # m/s -> km/h
            "scheduled_time": v.current_stop_sequence or None,
            "next_stop": stops_map.get(stop_id),
            "destination_stop": stops_map.get(trip_last_stop.get(v.trip.trip_id)),
            "occupancy": OCCUPANCY_MAP.get(v.occupancy_status),
            "cgm_id": vehicle_id,
            "inv_number": inv_number,
            "cgm_route_id": v.trip.route_id,
            "car": int(v.vehicle.label) if v.vehicle.label.isdigit() else None,
            "timestamp": v.timestamp or int(time.time())
        })

    return vehicles

# ---------------- WEBSOCKET ----------------

@app.websocket("/v2/livemap/")
async def websocket_livemap(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            data = await fetch_vehicles()
            await ws.send_json(data)
            await asyncio.sleep(5)  # ⏱️ 5 секунди
    except Exception as e:
        print("WS closed:", e)
