"""
Standalone public relay/matchmaking server for Ironstrike2D co-op - v2.

Same core forwarding logic as before (stamp each message with the sender's
id, forward it to one player or to everyone else) plus:

  - Three ways a connecting player can be placed:
      {"t": "quickmatch"}                    -> auto-matched into any open room
      {"t": "host_room", "name": "..."}      -> creates a NEW, LISTED room
      {"t": "join_room", "room": <id>}       -> joins a specific room by id
    The client sends ONE of these as its very first message right after
    connecting. If nothing arrives within a few seconds (e.g. some other
    client/tool connects), it falls back to quickmatch so the server never
    hangs waiting.

  - A plain HTTP GET /rooms returns the currently joinable LISTED rooms as
    JSON, so the game can show a "Browse Public Servers" screen without
    needing its own websocket connection just to look.

  - The server peeks at "settings" messages as they pass through (the host
    already broadcasts these whenever lobby settings change) purely to
    cache the gamemode/difficulty for display in the room list. It does not
    otherwise interpret game messages.

Deploy this file + requirements.txt as a Render "Web Service" (Python
runtime). Render sets $PORT for you - do not hardcode a port.
"""

import asyncio
import itertools
import json
import os

import websockets

ROOM_MAX_PLAYERS = int(os.environ.get("ROOM_MAX_PLAYERS", "4"))
PORT = int(os.environ.get("PORT", "10000"))
FIRST_MESSAGE_TIMEOUT = 5.0          # how long we wait for the initial quickmatch/host_room/join_room message

rooms = {}                           # room_id -> room dict, see _new_room()
_room_ids = itertools.count(1)
_pid_counter = itertools.count(1)


def _new_room(listed=False, name=""):
    return {
        "clients": {},      # pid -> websocket
        "host": None,       # pid of the current room host
        "locked": False,    # True once a "start" message has passed through
        "listed": listed,   # True = show up in GET /rooms
        "name": name[:40] if name else "Game",
        "mode": "",         # cached from the last "settings" message seen (display only)
        "diff": "",
    }


def _room_roster_msg(room):
    return json.dumps({"t": "roster", "ids": sorted(room["clients"])})


def _get_open_room():
    """First unlisted-or-listed room that isn't full/locked, else a fresh unlisted one (quickmatch)."""
    for rid, room in rooms.items():
        if not room["locked"] and len(room["clients"]) < ROOM_MAX_PLAYERS:
            return rid
    rid = next(_room_ids)
    rooms[rid] = _new_room()
    return rid


def _pick_room(first_msg):
    """Decide which room a newly connecting player goes into, based on their first message."""
    kind = first_msg.get("t")

    if kind == "host_room":
        rid = next(_room_ids)
        rooms[rid] = _new_room(listed=True, name=first_msg.get("name", "Game"))
        return rid

    if kind == "join_room":
        rid = first_msg.get("room")
        room = rooms.get(rid)
        if room is not None and not room["locked"] and len(room["clients"]) < ROOM_MAX_PLAYERS:
            return rid
        return _get_open_room()          # requested room gone/full/started -> fall back gracefully

    return _get_open_room()              # "quickmatch" or anything unrecognized


async def handler(ws):
    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=FIRST_MESSAGE_TIMEOUT)
        first_msg = json.loads(raw)
        if not isinstance(first_msg, dict):
            first_msg = {}
    except Exception:
        first_msg = {}                    # timed out or garbage -> just quickmatch them

    rid = _pick_room(first_msg)
    room = rooms[rid]

    if len(room["clients"]) >= ROOM_MAX_PLAYERS:
        await ws.send(json.dumps({"t": "full"}))
        return

    pid = next(_pid_counter)
    if room["host"] is None:
        room["host"] = pid
    room["clients"][pid] = ws

    try:
        await ws.send(json.dumps({"t": "welcome", "id": pid, "host": room["host"]}))
        websockets.broadcast(list(room["clients"].values()), _room_roster_msg(room))

        async for raw in ws:
            msg = json.loads(raw)
            msg["id"] = pid

            t = msg.get("t")
            if t == "start":
                room["locked"] = True
            elif t == "settings":
                room["mode"] = msg.get("gamemode", room["mode"])
                room["diff"] = msg.get("difficulty", room["diff"])

            data = json.dumps(msg)
            to = msg.get("to")
            if to is None:
                websockets.broadcast([w for p, w in room["clients"].items() if p != pid], data)
            elif to in room["clients"]:
                websockets.broadcast([room["clients"][to]], data)
    finally:
        room["clients"].pop(pid, None)
        if not room["clients"]:
            rooms.pop(rid, None)
        else:
            if room["host"] == pid:
                room["host"] = min(room["clients"])
            websockets.broadcast(list(room["clients"].values()), json.dumps({"t": "leave", "id": pid}))
            websockets.broadcast(list(room["clients"].values()), _room_roster_msg(room))


def _rooms_json():
    listed = [
        {
            "room": rid,
            "name": room["name"],
            "players": len(room["clients"]),
            "max": ROOM_MAX_PLAYERS,
            "mode": room["mode"],
            "difficulty": room["diff"],
        }
        for rid, room in rooms.items()
        if room["listed"] and not room["locked"] and len(room["clients"]) < ROOM_MAX_PLAYERS
    ]
    return json.dumps(listed)


async def _http_hook(connection, request):
    """Intercept plain HTTP requests before the websocket handshake: serve /rooms as JSON,
    answer anything else with a simple 200 so Render's health check (and a stray browser
    visit) sees the service as up. Returning None lets a real websocket upgrade proceed."""
    if request.headers.get("Upgrade", "").lower() == "websocket":
        return None
    if request.path == "/rooms":
        return connection.respond(200, _rooms_json())
    return connection.respond(200, "Ironstrike2D relay is running\n")


async def main():
    async with websockets.serve(handler, "0.0.0.0", PORT, process_request=_http_hook):
        print(f"Relay listening on 0.0.0.0:{PORT}")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
