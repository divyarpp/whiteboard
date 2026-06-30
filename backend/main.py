"""main.py — FastAPI backend for the whiteboard.

Phase 4: save & load over HTTP.
Phase 5: real-time collaboration over WebSockets (a "room" per session code),
         with owner-controlled drawing permission and autosave-on-last-leave.

Run from the backend/ folder:
    python -m uvicorn main:app --reload --port 8000
"""
import asyncio
import random
import secrets
import string

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from psycopg.rows import dict_row
from psycopg.types.json import Json
import psycopg.errors

from db import pool

app = FastAPI(title="Whiteboard API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

ALPHABET = string.ascii_uppercase + string.digits


# ===================================================================
#  HTTP: create session, load board, save board
# ===================================================================
class SessionIn(BaseModel):
    title: str = "Untitled whiteboard"


class BoardIn(BaseModel):
    title: str | None = None
    pages: list[dict]


def _replace_board(cur, sid, pages, title):
    """Wipe a session's pages/objects and re-insert from a payload list of
    {page_number, objects:[{id,type,...}]}. Shared by HTTP save and autosave."""
    cur.execute("delete from whiteboard_objects where session_id = %s", (sid,))
    cur.execute("delete from pages where session_id = %s", (sid,))
    for i, p in enumerate(pages):
        cur.execute(
            "insert into pages (session_id, page_number) values (%s, %s) returning page_id",
            (sid, p.get("page_number", i + 1)),
        )
        page_id = cur.fetchone()["page_id"]
        for o in p.get("objects", []):
            data = {k: v for k, v in o.items() if k not in ("id", "type")}
            cur.execute(
                "insert into whiteboard_objects (object_id, session_id, page_id, object_type, data_json) "
                "values (%s, %s, %s, %s, %s)",
                (o.get("id"), sid, page_id, o.get("type"), Json(data)),
            )
    if title is not None:
        cur.execute("update sessions set title = %s, updated_at = now() where session_id = %s", (title, sid))
    else:
        cur.execute("update sessions set updated_at = now() where session_id = %s", (sid,))


@app.get("/api/health")
def health():
    return {"ok": True}


@app.post("/api/sessions")
def create_session(body: SessionIn):
    with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        for _ in range(6):
            code = "".join(random.choices(ALPHABET, k=6))
            try:
                cur.execute(
                    "insert into sessions (session_code, title) values (%s, %s) returning session_id, session_code",
                    (code, body.title),
                )
                row = cur.fetchone()
                return {"session_id": str(row["session_id"]), "session_code": row["session_code"]}
            except psycopg.errors.UniqueViolation:
                conn.rollback()
        raise HTTPException(500, "could not generate a unique session code")


@app.get("/api/sessions/{code}/board")
def load_board(code: str):
    with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("select session_id, title from sessions where session_code = %s", (code,))
        s = cur.fetchone()
        if not s:
            raise HTTPException(404, "session not found")
        sid = s["session_id"]
        cur.execute("select page_id, page_number from pages where session_id = %s order by page_number", (sid,))
        pages = cur.fetchall()
        cur.execute(
            "select object_id, page_id, object_type, data_json from whiteboard_objects "
            "where session_id = %s and is_deleted = false",
            (sid,),
        )
        rows = cur.fetchall()
    by_page: dict[str, list] = {}
    for o in rows:
        by_page.setdefault(str(o["page_id"]), []).append(
            {"object_id": o["object_id"], "object_type": o["object_type"], "data_json": o["data_json"]}
        )
    return {"title": s["title"],
            "pages": [{"page_number": p["page_number"], "objects": by_page.get(str(p["page_id"]), [])} for p in pages]}


@app.put("/api/sessions/{code}/board")
def save_board(code: str, body: BoardIn):
    with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("select session_id from sessions where session_code = %s", (code,))
        s = cur.fetchone()
        if not s:
            raise HTTPException(404, "session not found")
        _replace_board(cur, s["session_id"], body.pages, body.title)
    return {"ok": True}


# ===================================================================
#  WebSockets: one room per session code
# ===================================================================
# Each room holds:
#   conns      : { websocket: {"id": str, "is_owner": bool} }
#   owner      : client id of the first joiner
#   allow_draw : may non-owners draw right now?
#   pages      : authoritative live state, a list (one per page) of
#                { object_id: object_dict }.  None until seeded.
ROOMS: dict[str, dict] = {}
DRAW_OPS = {"object_create", "object_update", "object_delete", "page_clear", "page_create", "page_delete"}


def _serialize(room) -> list:
    return [{"objects": list(pg.values())} for pg in (room["pages"] or [])]


async def _broadcast(room, msg, exclude=None):
    for w in list(room["conns"].keys()):
        if w is exclude:
            continue
        try:
            await w.send_json(msg)
        except Exception:
            pass


def _apply(room, msg):
    pages = room["pages"]
    if pages is None:
        return
    t = msg["type"]
    if t in ("object_create", "object_update"):
        i = msg.get("page", -1)
        if 0 <= i < len(pages):
            pages[i][msg["object"]["id"]] = msg["object"]
    elif t == "object_delete":
        i = msg.get("page", -1)
        if 0 <= i < len(pages):
            pages[i].pop(msg["id"], None)
    elif t == "page_clear":
        i = msg.get("page", -1)
        if 0 <= i < len(pages):
            pages[i] = {}
    elif t == "page_create":
        idx = msg.get("index", len(pages))
        idx = max(0, min(idx, len(pages)))
        pages.insert(idx, {o["id"]: o for o in (msg.get("objects") or [])})
    elif t == "page_delete":
        i = msg.get("index", -1)
        if len(pages) > 1 and 0 <= i < len(pages):
            pages.pop(i)


def _persist_room(code):
    """Write a room's live state to the database (used on last-leave autosave)."""
    room = ROOMS.get(code)
    if not room or room["pages"] is None:
        return
    pages = [{"page_number": i + 1, "objects": list(pg.values())} for i, pg in enumerate(room["pages"])]
    with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("select session_id from sessions where session_code = %s", (code,))
        s = cur.fetchone()
        if s:
            _replace_board(cur, s["session_id"], pages, None)


@app.websocket("/ws/{code}")
async def ws_endpoint(ws: WebSocket, code: str):
    await ws.accept()
    room = ROOMS.get(code)
    if room is None:
        room = {"conns": {}, "owner": None, "allow_draw": False, "pages": None}
        ROOMS[code] = room

    client_id = secrets.token_hex(4)
    is_owner = room["owner"] is None
    if is_owner:
        room["owner"] = client_id
    room["conns"][ws] = {"id": client_id, "is_owner": is_owner}

    need_seed = room["pages"] is None and is_owner
    await ws.send_json({"type": "init", "clientId": client_id, "isOwner": is_owner,
                        "allowDraw": room["allow_draw"], "needSeed": need_seed})
    if room["pages"] is not None:
        await ws.send_json({"type": "snapshot", "pages": _serialize(room)})
    await _broadcast(room, {"type": "presence", "count": len(room["conns"])})

    try:
        while True:
            msg = await ws.receive_json()
            t = msg.get("type")
            client = room["conns"].get(ws)
            if client is None:
                break

            if t == "seed":
                if room["pages"] is None:
                    room["pages"] = [{o["id"]: o for o in (p.get("objects") or [])} for p in msg.get("pages", [])]
            elif t == "set_permission":
                if client["is_owner"]:
                    room["allow_draw"] = bool(msg.get("allowDraw"))
                    await _broadcast(room, {"type": "permission_update", "allowDraw": room["allow_draw"]})
            elif t in DRAW_OPS:
                if not (client["is_owner"] or room["allow_draw"]):
                    await ws.send_json({"type": "denied"})
                    continue
                _apply(room, msg)
                await _broadcast(room, msg, exclude=ws)
    except WebSocketDisconnect:
        pass
    finally:
        room["conns"].pop(ws, None)
        if room["conns"]:
            await _broadcast(room, {"type": "presence", "count": len(room["conns"])})
        else:
            try:
                await asyncio.to_thread(_persist_room, code)  # autosave the last live state
            except Exception:
                pass
            ROOMS.pop(code, None)