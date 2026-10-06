"""Maritime search-and-rescue coordination service."""
from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent
DEFAULT_DB = ROOT / "maritime_sar.db"
ACTIVE_INCIDENT = {"reported", "coordinating", "recovering"}
CLOSED_INCIDENT = {"closed", "cancelled", "duplicate"}


class DomainError(Exception):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_field_time(raw: Any) -> datetime:
    """Parse an on-scene timestamp into an aware datetime (naive values are UTC)."""
    if not isinstance(raw, str) or not raw.strip():
        raise DomainError("事件缺少现场时间 occurred_at")
    text = raw.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise DomainError("现场时间 occurred_at 不是有效的 ISO 8601 时间") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * radius * math.asin(math.sqrt(a))


def require_role(role: str, allowed: set[str], action: str) -> None:
    if role not in allowed:
        raise DomainError("角色无权执行：%s" % action, 403)


def clean_actor(actor: str) -> str:
    actor = (actor or "").strip()
    if not actor:
        raise DomainError("缺少操作人")
    return actor


def validate_position(lat: Any, lon: Any) -> tuple[float, float]:
    try:
        lat, lon = float(lat), float(lon)
    except (TypeError, ValueError) as exc:
        raise DomainError("经纬度必须是数值") from exc
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        raise DomainError("经纬度超出有效范围")
    return lat, lon


def json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


class MaritimeSARService:
    def __init__(self, db_path: str | os.PathLike[str] = DEFAULT_DB):
        self.db_path = str(db_path)
        self._init_schema()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

    def _init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS incidents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL UNIQUE,
                    vessel_name TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    latitude REAL NOT NULL,
                    longitude REAL NOT NULL,
                    uncertainty_km REAL NOT NULL,
                    drift_direction REAL NOT NULL DEFAULT 0,
                    drift_speed_kn REAL NOT NULL DEFAULT 0,
                    sea_state INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'reported',
                    lead_org TEXT NOT NULL,
                    duplicate_of INTEGER REFERENCES incidents(id),
                    version INTEGER NOT NULL DEFAULT 1,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS assets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    kind TEXT NOT NULL,
                    capabilities TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'available',
                    latitude REAL NOT NULL,
                    longitude REAL NOT NULL,
                    speed_kn REAL NOT NULL,
                    range_km REAL NOT NULL,
                    max_sea_state INTEGER NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS search_areas (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_id INTEGER NOT NULL REFERENCES incidents(id),
                    code TEXT NOT NULL UNIQUE,
                    kind TEXT NOT NULL,
                    center_lat REAL NOT NULL,
                    center_lon REAL NOT NULL,
                    radius_km REAL NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 3,
                    status TEXT NOT NULL DEFAULT 'planned',
                    assigned_asset_id INTEGER REFERENCES assets(id),
                    note TEXT NOT NULL DEFAULT '',
                    version INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS clues (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_id INTEGER NOT NULL REFERENCES incidents(id),
                    area_id INTEGER REFERENCES search_areas(id),
                    client_event_id TEXT NOT NULL UNIQUE,
                    latitude REAL NOT NULL,
                    longitude REAL NOT NULL,
                    confidence REAL NOT NULL,
                    source TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'unverified',
                    distance_from_incident_km REAL NOT NULL,
                    reporter TEXT NOT NULL,
                    details TEXT NOT NULL DEFAULT '',
                    recorded_at TEXT NOT NULL,
                    merged_at TEXT
                );
                CREATE TABLE IF NOT EXISTS offline_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_batch_id TEXT NOT NULL UNIQUE,
                    actor TEXT NOT NULL,
                    status TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    merged_at TEXT,
                    summary TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS offline_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id INTEGER NOT NULL REFERENCES offline_batches(id),
                    client_batch_id TEXT NOT NULL,
                    client_event_id TEXT NOT NULL UNIQUE,
                    event_type TEXT NOT NULL,
                    occurred_at TEXT,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL,
                    conflict_reason TEXT NOT NULL DEFAULT '',
                    reference_id INTEGER,
                    idempotent INTEGER NOT NULL DEFAULT 0,
                    resolved_by TEXT NOT NULL DEFAULT '',
                    resolved_note TEXT NOT NULL DEFAULT '',
                    processed_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS timeline (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_id INTEGER REFERENCES incidents(id),
                    actor TEXT NOT NULL,
                    action TEXT NOT NULL,
                    details TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    occurred_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_clues_incident ON clues(incident_id, recorded_at);
                CREATE INDEX IF NOT EXISTS idx_timeline_incident ON timeline(incident_id, id);
                CREATE INDEX IF NOT EXISTS idx_offline_events_batch ON offline_events(batch_id, occurred_at);
                """
            )
            timeline_cols = {row["name"] for row in conn.execute("PRAGMA table_info(timeline)")}
            if "occurred_at" not in timeline_cols:
                conn.execute("ALTER TABLE timeline ADD COLUMN occurred_at TEXT")

    def _audit(self, conn: sqlite3.Connection, incident_id: int | None, actor: str, action: str,
               details: dict[str, Any], occurred_at: str | None = None) -> None:
        conn.execute(
            "INSERT INTO timeline(incident_id,actor,action,details,created_at,occurred_at) VALUES(?,?,?,?,?,?)",
            (incident_id, actor, action, json_dump(details), utcnow(), occurred_at),
        )

    def create_incident(self, actor: str, role: str, code: str, vessel_name: str,
                        latitude: float, longitude: float, uncertainty_km: float,
                        sea_state: int, lead_org: str, drift_direction: float = 0,
                        drift_speed_kn: float = 0, description: str = "") -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator", "operator"}, "创建遇险事件")
        code, vessel_name, lead_org = code.strip(), vessel_name.strip(), lead_org.strip()
        if not code or not vessel_name or not lead_org:
            raise DomainError("事件编号、船名和负责机构不能为空")
        lat, lon = validate_position(latitude, longitude)
        try:
            uncertainty_km = float(uncertainty_km)
            sea_state = int(sea_state)
            drift_direction = float(drift_direction)
            drift_speed_kn = float(drift_speed_kn)
        except (TypeError, ValueError) as exc:
            raise DomainError("不确定半径、海况和漂移参数必须是数值") from exc
        if uncertainty_km <= 0 or uncertainty_km > 1000:
            raise DomainError("不确定半径应在 0 到 1000 公里之间")
        if not 0 <= sea_state <= 9 or drift_speed_kn < 0:
            raise DomainError("海况或漂移速度无效")
        now = utcnow()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            duplicate = conn.execute(
                "SELECT * FROM incidents WHERE vessel_name=? AND status IN ('reported','coordinating','recovering') ORDER BY id DESC",
                (vessel_name,),
            ).fetchall()
            duplicate_of = None
            for row in duplicate:
                if haversine_km(lat, lon, row["latitude"], row["longitude"]) <= max(20.0, uncertainty_km + row["uncertainty_km"]):
                    duplicate_of = row["id"]
                    break
            status = "duplicate" if duplicate_of else "reported"
            try:
                cur = conn.execute(
                    """INSERT INTO incidents(code,vessel_name,description,latitude,longitude,uncertainty_km,
                       drift_direction,drift_speed_kn,sea_state,status,lead_org,duplicate_of,created_by,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (code, vessel_name, description.strip(), lat, lon, uncertainty_km, drift_direction, drift_speed_kn,
                     sea_state, status, lead_org, duplicate_of, actor, now, now),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("事件编号已存在", 409) from exc
            incident_id = int(cur.lastrowid)
            self._audit(conn, incident_id, actor, "incident.reported", {"duplicate_of": duplicate_of})
            if duplicate_of:
                self._audit(conn, duplicate_of, actor, "incident.duplicate_detected", {"duplicate_incident": code})
            return dict(conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone())

    def list_assets(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM assets ORDER BY id").fetchall()
        return [dict(row) for row in rows]

    def add_asset(self, actor: str, role: str, name: str, kind: str,
                  capabilities: list[str], latitude: float, longitude: float,
                  speed_kn: float, range_km: float, max_sea_state: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "登记搜救资源")
        lat, lon = validate_position(latitude, longitude)
        name, kind = name.strip(), kind.strip()
        caps = sorted({str(item).strip() for item in capabilities if str(item).strip()})
        if not name or not kind or not caps:
            raise DomainError("资源名称、类型和能力不能为空")
        try:
            speed_kn, range_km, max_sea_state = float(speed_kn), float(range_km), int(max_sea_state)
        except (TypeError, ValueError) as exc:
            raise DomainError("速度和航程参数必须是数值") from exc
        if speed_kn <= 0 or range_km <= 0 or not 0 <= max_sea_state <= 9:
            raise DomainError("速度、航程或适用海况无效")
        with self.connect() as conn:
            try:
                cur = conn.execute(
                    """INSERT INTO assets(name,kind,capabilities,latitude,longitude,speed_kn,range_km,max_sea_state,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    (name, kind, json_dump(caps), lat, lon, speed_kn, range_km, max_sea_state, utcnow()),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("资源名称已存在", 409) from exc
            self._audit(conn, None, actor, "asset.registered", {"asset_id": cur.lastrowid, "name": name})
            return dict(conn.execute("SELECT * FROM assets WHERE id=?", (cur.lastrowid,)).fetchone())

    def create_search_area(self, actor: str, role: str, incident_id: int, code: str,
                           kind: str, center_lat: float, center_lon: float,
                           radius_km: float, priority: int = 3, note: str = "") -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "创建搜索区域")
        lat, lon = validate_position(center_lat, center_lon)
        kind, code = kind.strip(), code.strip()
        if not kind or not code:
            raise DomainError("区域类型和编号不能为空")
        try:
            radius_km, priority = float(radius_km), int(priority)
        except (TypeError, ValueError) as exc:
            raise DomainError("半径和优先级必须是数值") from exc
        if radius_km <= 0 or not 1 <= priority <= 5:
            raise DomainError("搜索半径或优先级无效")
        now = utcnow()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if not incident:
                raise DomainError("事件不存在", 404)
            if incident["status"] not in ACTIVE_INCIDENT:
                raise DomainError("当前事件不能创建搜索区域", 409)
            try:
                cur = conn.execute(
                    """INSERT INTO search_areas(incident_id,code,kind,center_lat,center_lon,radius_km,priority,note,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?)""",
                    (incident_id, code, kind, lat, lon, radius_km, priority, note.strip(), now, now),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("搜索区域编号已存在", 409) from exc
            self._audit(conn, incident_id, actor, "area.created", {"area_id": cur.lastrowid, "code": code})
            return dict(conn.execute("SELECT * FROM search_areas WHERE id=?", (cur.lastrowid,)).fetchone())

    def assign_area(self, actor: str, role: str, area_id: int, asset_id: int,
                    expected_asset_version: int | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "分配搜索任务")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            area = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
            asset = conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()
            if not area or not asset:
                raise DomainError("搜索区域或资源不存在", 404)
            if area["assigned_asset_id"] is not None:
                raise DomainError("搜索区域已经分配", 409)
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (area["incident_id"],)).fetchone()
            if not incident or incident["status"] not in ACTIVE_INCIDENT:
                raise DomainError("事件当前不可分配", 409)
            if expected_asset_version is not None and asset["version"] != int(expected_asset_version):
                raise DomainError("资源状态已变化，请刷新后重试", 409)
            if asset["status"] != "available":
                raise DomainError("资源当前不可用", 409)
            if incident["sea_state"] > asset["max_sea_state"]:
                raise DomainError("海况超出资源能力", 409)
            capabilities = json.loads(asset["capabilities"])
            if area["kind"] not in capabilities:
                raise DomainError("资源不具备该搜索区域能力", 409)
            distance = haversine_km(asset["latitude"], asset["longitude"], area["center_lat"], area["center_lon"])
            if distance > asset["range_km"]:
                raise DomainError("搜索区域超出资源航程", 409)
            now = utcnow()
            changed = conn.execute(
                "UPDATE assets SET status='assigned',version=version+1,updated_at=? WHERE id=? AND status='available' AND version=?",
                (now, asset_id, asset["version"]),
            )
            if changed.rowcount != 1:
                raise DomainError("资源已被其他任务占用", 409)
            conn.execute(
                "UPDATE search_areas SET assigned_asset_id=?,status='assigned',version=version+1,updated_at=? WHERE id=?",
                (asset_id, now, area_id),
            )
            self._audit(conn, area["incident_id"], actor, "area.assigned", {"area_id": area_id, "asset_id": asset_id, "distance_km": round(distance, 2)})
            return dict(conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone())

    def record_clue(self, actor: str, role: str, incident_id: int, client_event_id: str,
                    latitude: float, longitude: float, confidence: float, source: str,
                    area_id: int | None = None, details: str = "") -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator", "operator", "field"}, "记录搜索线索")
        lat, lon = validate_position(latitude, longitude)
        event_id, source = client_event_id.strip(), source.strip()
        if not event_id or not source:
            raise DomainError("事件幂等编号和线索来源不能为空")
        try:
            confidence = float(confidence)
        except (TypeError, ValueError) as exc:
            raise DomainError("线索置信度必须是数值") from exc
        if not 0 <= confidence <= 1:
            raise DomainError("置信度应在 0 到 1 之间")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute("SELECT * FROM clues WHERE client_event_id=?", (event_id,)).fetchone()
            if existing:
                return dict(existing)
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if not incident:
                raise DomainError("事件不存在", 404)
            if incident["status"] in CLOSED_INCIDENT:
                raise DomainError("已结束事件不能新增线索", 409)
            if area_id is not None:
                area = conn.execute("SELECT * FROM search_areas WHERE id=? AND incident_id=?", (area_id, incident_id)).fetchone()
                if not area:
                    raise DomainError("搜索区域不属于该事件", 409)
            distance = haversine_km(incident["latitude"], incident["longitude"], lat, lon)
            status = "unverified" if distance <= incident["uncertainty_km"] * 3 else "invalid"
            cur = conn.execute(
                """INSERT INTO clues(incident_id,area_id,client_event_id,latitude,longitude,confidence,source,status,
                   distance_from_incident_km,reporter,details,recorded_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (incident_id, area_id, event_id, lat, lon, confidence, source, status, distance, actor, details.strip(), utcnow()),
            )
            self._audit(conn, incident_id, actor, "clue.recorded", {"clue_id": cur.lastrowid, "status": status, "event_id": event_id})
            return dict(conn.execute("SELECT * FROM clues WHERE id=?", (cur.lastrowid,)).fetchone())

    def verify_clue(self, actor: str, role: str, clue_id: int, status: str,
                    expected_version: int | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator", "analyst"}, "核验线索")
        if status not in {"verified", "rejected", "unverified"}:
            raise DomainError("线索状态无效")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            clue = conn.execute("SELECT * FROM clues WHERE id=?", (clue_id,)).fetchone()
            if not clue:
                raise DomainError("线索不存在", 404)
            if status == "verified" and clue["status"] == "invalid" and role != "coordinator":
                raise DomainError("异常位置线索只能由协调员确认", 403)
            conn.execute("UPDATE clues SET status=?,merged_at=? WHERE id=?", (status, utcnow(), clue_id))
            self._audit(conn, clue["incident_id"], actor, "clue.reviewed", {"clue_id": clue_id, "status": status})
            return dict(conn.execute("SELECT * FROM clues WHERE id=?", (clue_id,)).fetchone())

    def withdraw_asset(self, actor: str, role: str, asset_id: int, reason: str,
                       expected_asset_version: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "撤回资源")
        if not reason.strip():
            raise DomainError("撤回原因不能为空")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            asset = conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()
            if not asset:
                raise DomainError("资源不存在", 404)
            if asset["version"] != int(expected_asset_version):
                raise DomainError("资源状态已变化，请刷新后重试", 409)
            if asset["status"] == "available":
                raise DomainError("资源当前未分配", 409)
            now = utcnow()
            areas = conn.execute("SELECT id,incident_id FROM search_areas WHERE assigned_asset_id=? AND status IN ('assigned','active')", (asset_id,)).fetchall()
            for area in areas:
                conn.execute("UPDATE search_areas SET assigned_asset_id=NULL,status='planned',version=version+1,updated_at=? WHERE id=?", (now, area["id"]))
                self._audit(conn, area["incident_id"], actor, "area.unassigned", {"area_id": area["id"], "reason": reason.strip()})
            conn.execute("UPDATE assets SET status='available',version=version+1,updated_at=? WHERE id=?", (now, asset_id))
            self._audit(conn, None, actor, "asset.withdrawn", {"asset_id": asset_id, "reason": reason.strip()})
            return dict(conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone())

    def transfer_incident(self, actor: str, role: str, incident_id: int, new_org: str,
                          expected_version: int, note: str = "") -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "移交事件")
        new_org = new_org.strip()
        if not new_org:
            raise DomainError("接收机构不能为空")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if not incident:
                raise DomainError("事件不存在", 404)
            if incident["status"] in CLOSED_INCIDENT:
                raise DomainError("已结束事件不能移交", 409)
            if incident["version"] != int(expected_version):
                raise DomainError("事件已变化，请刷新后重试", 409)
            conn.execute(
                "UPDATE incidents SET lead_org=?,version=version+1,updated_at=? WHERE id=? AND version=?",
                (new_org, utcnow(), incident_id, expected_version),
            )
            self._audit(conn, incident_id, actor, "incident.transferred", {"from": incident["lead_org"], "to": new_org, "note": note.strip()})
            return dict(conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone())

    def complete_area(self, actor: str, role: str, area_id: int, outcome: str,
                      expected_version: int | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "结束搜索区域")
        if outcome not in {"completed", "abandoned"}:
            raise DomainError("区域结束结论无效")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            area = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
            if not area:
                raise DomainError("搜索区域不存在", 404)
            if area["status"] in {"completed", "abandoned"}:
                raise DomainError("搜索区域已经结束", 409)
            if expected_version is not None and area["version"] != int(expected_version):
                raise DomainError("搜索区域已变化，请刷新后重试", 409)
            if area["assigned_asset_id"] is not None:
                conn.execute("UPDATE assets SET status='available',version=version+1,updated_at=? WHERE id=?", (utcnow(), area["assigned_asset_id"]))
            conn.execute("UPDATE search_areas SET status=?,assigned_asset_id=NULL,version=version+1,updated_at=? WHERE id=?", (outcome, utcnow(), area_id))
            self._audit(conn, area["incident_id"], actor, "area." + outcome, {"area_id": area_id})
            return dict(conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone())

    def close_incident(self, actor: str, role: str, incident_id: int, outcome: str,
                       expected_version: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "结束事件")
        if outcome not in {"resolved", "cancelled", "false_alarm"}:
            raise DomainError("结束结论无效")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if not incident:
                raise DomainError("事件不存在", 404)
            if incident["version"] != int(expected_version):
                raise DomainError("事件已变化，请刷新后重试", 409)
            active_area = conn.execute(
                "SELECT COUNT(*) AS c FROM search_areas WHERE incident_id=? AND status IN ('planned','assigned','active')",
                (incident_id,),
            ).fetchone()["c"]
            if active_area and outcome != "false_alarm":
                raise DomainError("仍有未结束搜索区域，不能关闭事件", 409)
            status = "closed" if outcome == "resolved" else "cancelled"
            conn.execute("UPDATE incidents SET status=?,version=version+1,updated_at=? WHERE id=?", (status, utcnow(), incident_id))
            self._audit(conn, incident_id, actor, "incident.closed", {"outcome": outcome})
            return dict(conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone())

    # -- offline replay -----------------------------------------------------

    OFFLINE_TYPES = {"clue", "timeline", "assignment", "withdraw", "transfer"}

    def _replay_assignment(self, conn: sqlite3.Connection, event: dict[str, Any],
                           occurred: str, force: bool = False) -> tuple[int, dict[str, Any]]:
        area_id = int(event["area_id"])
        asset_id = int(event["asset_id"])
        area = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
        asset = conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()
        if not area or not asset:
            raise DomainError("搜索区域或资源不存在", 404)
        incident = conn.execute("SELECT * FROM incidents WHERE id=?", (area["incident_id"],)).fetchone()
        if not incident:
            raise DomainError("事件不存在", 404)
        # Idempotent: this exact allocation already holds.
        if area["assigned_asset_id"] == asset_id and area["status"] == "assigned":
            return area_id, {"idempotent": True}
        # A stale asset version means the shore side touched the same resource;
        # that is the conflict the coordinator must adjudicate.
        expected = event.get("expected_asset_version")
        if expected is not None and int(expected) != asset["version"] and not force:
            raise DomainError("岸端已修改该资源（版本 %s，现场基于版本 %s）" % (asset["version"], int(expected)), 409)
        reason: str | None = None
        if area["assigned_asset_id"] is not None:
            reason = "搜索区域已经分配给其他资源"
        elif incident["status"] in CLOSED_INCIDENT:
            reason = "事件已结束，不能占用资源"
        elif asset["status"] != "available":
            reason = "资源已被占用或撤回"
        if reason is None and incident["sea_state"] > asset["max_sea_state"]:
            reason = "海况超出资源能力"
        if reason is None:
            capabilities = json.loads(asset["capabilities"])
            if area["kind"] not in capabilities:
                reason = "资源不具备该搜索区域能力"
        if reason is None:
            distance = haversine_km(asset["latitude"], asset["longitude"], area["center_lat"], area["center_lon"])
            if distance > asset["range_km"]:
                reason = "搜索区域超出资源航程"
        if reason and not force:
            raise DomainError(reason, 409)
        now = utcnow()
        if force:
            # Adjudication in favour of the field operation: release any prior
            # occupancy that would otherwise leave two areas on one asset.
            prior_areas = conn.execute(
                "SELECT id FROM search_areas WHERE assigned_asset_id=? AND id<>? AND status IN ('assigned','active')",
                (asset_id, area_id),
            ).fetchall()
            for prior in prior_areas:
                conn.execute(
                    "UPDATE search_areas SET assigned_asset_id=NULL,status='planned',version=version+1,updated_at=? WHERE id=?",
                    (now, prior["id"]),
                )
            other_asset = area["assigned_asset_id"]
            if other_asset is not None and other_asset != asset_id:
                conn.execute(
                    "UPDATE assets SET status='available',version=version+1,updated_at=? WHERE id=?",
                    (now, other_asset),
                )
        conn.execute(
            "UPDATE assets SET status='assigned',version=version+1,updated_at=? WHERE id=? AND status='available'",
            (now, asset_id),
        )
        conn.execute(
            "UPDATE search_areas SET assigned_asset_id=?,status='assigned',version=version+1,updated_at=? WHERE id=?",
            (asset_id, now, area_id),
        )
        self._audit(conn, area["incident_id"], event.get("actor") or "offline", "area.assigned",
                    {"area_id": area_id, "asset_id": asset_id, "source": "offline", "forced": force}, occurred)
        return area_id, {"idempotent": False, "forced": force}

    def _replay_withdraw(self, conn: sqlite3.Connection, event: dict[str, Any],
                         occurred: str, force: bool = False) -> tuple[int, dict[str, Any]]:
        asset_id = int(event["asset_id"])
        reason_text = str(event.get("reason", "离线撤回")).strip() or "离线撤回"
        asset = conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()
        if not asset:
            raise DomainError("资源不存在", 404)
        if asset["status"] == "available":
            return asset_id, {"idempotent": True}
        incident_id: int | None = None
        areas = conn.execute(
            "SELECT id,incident_id FROM search_areas WHERE assigned_asset_id=? AND status IN ('assigned','active')",
            (asset_id,),
        ).fetchall()
        conflict = None
        expected = event.get("expected_asset_version")
        if expected is not None and int(expected) != asset["version"] and not force:
            conflict = "岸端已修改该资源（版本 %s，现场基于版本 %s）" % (asset["version"], int(expected))
        if conflict:
            raise DomainError(conflict, 409)
        now = utcnow()
        for area in areas:
            incident_id = area["incident_id"]
            conn.execute(
                "UPDATE search_areas SET assigned_asset_id=NULL,status='planned',version=version+1,updated_at=? WHERE id=?",
                (now, area["id"]),
            )
            self._audit(conn, area["incident_id"], event.get("actor") or "offline", "area.unassigned",
                        {"area_id": area["id"], "reason": reason_text, "source": "offline", "forced": force}, occurred)
        conn.execute("UPDATE assets SET status='available',version=version+1,updated_at=? WHERE id=?", (now, asset_id))
        self._audit(conn, incident_id, event.get("actor") or "offline", "asset.withdrawn",
                    {"asset_id": asset_id, "reason": reason_text, "source": "offline", "forced": force}, occurred)
        return asset_id, {"idempotent": False, "forced": force}

    def _replay_transfer(self, conn: sqlite3.Connection, event: dict[str, Any],
                         occurred: str, force: bool = False) -> tuple[int, dict[str, Any]]:
        incident_id = int(event["incident_id"])
        new_org = str(event.get("new_org", "")).strip()
        if not new_org:
            raise DomainError("接收机构不能为空")
        incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
        if not incident:
            raise DomainError("事件不存在", 404)
        if incident["status"] in CLOSED_INCIDENT:
            if not force:
                raise DomainError("事件已结束，不能移交", 409)
        if incident["lead_org"] == new_org:
            return incident_id, {"idempotent": True}
        expected = event.get("expected_version")
        if expected is not None and int(expected) != incident["version"] and not force:
            raise DomainError("岸端已修改事件（版本 %s，现场基于版本 %s）" % (incident["version"], int(expected)), 409)
        from_org = incident["lead_org"]
        now = utcnow()
        conn.execute(
            "UPDATE incidents SET lead_org=?,version=version+1,updated_at=? WHERE id=?",
            (new_org, now, incident_id),
        )
        self._audit(conn, incident_id, event.get("actor") or "offline", "incident.transferred",
                    {"from": from_org, "to": new_org, "source": "offline", "forced": force,
                     "note": str(event.get("note", "")).strip()}, occurred)
        return incident_id, {"idempotent": False, "forced": force}

    def _replay_clue(self, conn: sqlite3.Connection, event: dict[str, Any],
                     occurred: str, force: bool = False) -> tuple[int | None, dict[str, Any]]:
        event_id = str(event["client_event_id"]).strip()
        existing_clue = conn.execute("SELECT id FROM clues WHERE client_event_id=?", (event_id,)).fetchone()
        if existing_clue:
            return existing_clue["id"], {"idempotent": True}
        incident_id = int(event["incident_id"])
        lat, lon = validate_position(event["latitude"], event["longitude"])
        confidence = float(event["confidence"])
        if not 0 <= confidence <= 1:
            raise DomainError("置信度应在 0 到 1 之间")
        incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
        if not incident:
            raise DomainError("事件不存在", 404)
        if incident["status"] in CLOSED_INCIDENT and not force:
            raise DomainError("已结束事件不能新增线索", 409)
        area_id = event.get("area_id")
        if area_id is not None and not conn.execute(
            "SELECT 1 FROM search_areas WHERE id=? AND incident_id=?", (area_id, incident_id)
        ).fetchone():
            raise DomainError("搜索区域不属于该事件", 409)
        distance = haversine_km(incident["latitude"], incident["longitude"], lat, lon)
        status = "unverified" if distance <= incident["uncertainty_km"] * 3 else "invalid"
        cur = conn.execute(
            """INSERT INTO clues(incident_id,area_id,client_event_id,latitude,longitude,confidence,source,status,
               distance_from_incident_km,reporter,details,recorded_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (incident_id, area_id, event_id, lat, lon, confidence,
             str(event.get("source", "offline")).strip(), status, distance,
             str(event.get("actor", "offline")).strip(), str(event.get("details", "")).strip(), occurred),
        )
        self._audit(conn, incident_id, str(event.get("actor", "offline")).strip(), "clue.recorded",
                    {"clue_id": cur.lastrowid, "status": status, "event_id": event_id,
                     "source": "offline", "forced": force}, occurred)
        return cur.lastrowid, {"idempotent": False, "forced": force}

    def _replay_timeline(self, conn: sqlite3.Connection, event: dict[str, Any], occurred: str) -> tuple[None, dict[str, Any]]:
        incident_id = int(event["incident_id"])
        if not conn.execute("SELECT 1 FROM incidents WHERE id=?", (incident_id,)).fetchone():
            raise DomainError("事件不存在", 404)
        details = event.get("details", {})
        if not isinstance(details, dict):
            raise DomainError("时间线详情必须是对象")
        self._audit(conn, incident_id, str(event.get("actor", "offline")).strip(),
                    str(event.get("action", "offline.note")).strip() or "offline.note",
                    {**details, "source": "offline"}, occurred)
        return None, {"idempotent": False}

    def _replay_one(self, conn: sqlite3.Connection, event: dict[str, Any], occurred: str,
                    force: bool = False) -> tuple[int | None, dict[str, Any]]:
        event_type = event.get("type")
        if event_type == "assignment":
            return self._replay_assignment(conn, event, occurred, force)
        if event_type == "withdraw":
            return self._replay_withdraw(conn, event, occurred, force)
        if event_type == "transfer":
            return self._replay_transfer(conn, event, occurred, force)
        if event_type == "clue":
            return self._replay_clue(conn, event, occurred, force)
        if event_type == "timeline":
            return self._replay_timeline(conn, event, occurred)
        raise DomainError("不支持的离线事件类型：%r" % (event_type,))

    def merge_offline_batch(self, actor: str, role: str, client_batch_id: str,
                            events: list[dict[str, Any]]) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"operator", "field", "coordinator"}, "合并离线记录")
        batch_id_ref = client_batch_id.strip()
        if not batch_id_ref or not isinstance(events, list):
            raise DomainError("批次编号和事件列表不能为空")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute("SELECT * FROM offline_batches WHERE client_batch_id=?", (batch_id_ref,)).fetchone()
            if existing:
                # Retry of a fully or partially uploaded batch is idempotent:
                # already-stored events are never replayed twice.
                stored = conn.execute(
                    "SELECT * FROM offline_events WHERE batch_id=? ORDER BY occurred_at IS NULL, occurred_at, client_event_id",
                    (existing["id"],),
                ).fetchall()
                if not stored:
                    summary = json.loads(existing["summary"])
                else:
                    summary = self._batch_summary(stored)
                    conn.execute("UPDATE offline_batches SET summary=?, status=? WHERE id=?",
                                 (json.dumps(summary, ensure_ascii=False),
                                  "conflict" if summary["conflicts"] else "merged", existing["id"]))
                return {"batch_id": batch_id_ref, "idempotent": True,
                        "status": "conflict" if summary["conflicts"] else "merged", "summary": summary}

            # Parse + structurally validate every event, and order by on-scene time.
            ordered: list[tuple[datetime, str, dict[str, Any]]] = []
            structural: list[tuple[str, str, dict[str, Any]]] = []
            for raw in events:
                if not isinstance(raw, dict):
                    structural.append(("", "离线事件必须是对象", {}))
                    continue
                event_id = str(raw.get("client_event_id", "")).strip()
                if not event_id:
                    structural.append(("", "离线事件缺少 client_event_id", raw))
                    continue
                if raw.get("type") not in self.OFFLINE_TYPES:
                    structural.append((event_id, "不支持的离线事件类型：%r" % raw.get("type"), raw))
                    continue
                try:
                    when = parse_field_time(raw.get("occurred_at"))
                except DomainError as exc:
                    structural.append((event_id, str(exc), raw))
                    continue
                ordered.append((when, event_id, raw))
            ordered.sort(key=lambda item: (item[0], item[1]))

            now = utcnow()
            batch_cur = conn.execute(
                "INSERT INTO offline_batches(client_batch_id,actor,status,received_at,merged_at,summary) VALUES(?,?,?,?,?,?)",
                (batch_id_ref, actor, "merging", now, None, "{}"),
            )
            batch_pk = int(batch_cur.lastrowid)
            results: list[dict[str, Any]] = []

            def store(event_id: str, event_type: str, when: str | None, payload: dict[str, Any],
                      status: str, conflict_reason: str, reference_id: int | None,
                      idempotent: bool) -> dict[str, Any]:
                conn.execute(
                    """INSERT INTO offline_events(batch_id,client_batch_id,client_event_id,event_type,occurred_at,
                       payload,status,conflict_reason,reference_id,idempotent,processed_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                    (batch_pk, batch_id_ref, event_id, event_type, when, json.dumps(payload, ensure_ascii=False),
                     status, conflict_reason, reference_id, 1 if idempotent else 0, utcnow()),
                )
                item = {"client_event_id": event_id, "type": event_type, "occurred_at": when,
                        "status": status, "idempotent": bool(idempotent)}
                if conflict_reason:
                    item["error"] = conflict_reason
                if reference_id is not None:
                    item["record_id"] = reference_id
                results.append(item)
                return item

            # Permanently malformed events never become conflicts.
            for seq, (event_id, error, payload) in enumerate(structural):
                store(event_id or "__invalid__%d-%d" % (batch_pk, seq + 1),
                      str(payload.get("type", "unknown")) if payload else "unknown",
                      None, payload if isinstance(payload, dict) else {}, "rejected", error, None, False)

            # Replay in field-time order; a conflicting event is parked untouched.
            for when, event_id, raw in ordered:
                occurred = iso(when)
                dup = conn.execute("SELECT * FROM offline_events WHERE client_event_id=?", (event_id,)).fetchone()
                if dup:
                    # Same event already delivered in another batch: never replay twice.
                    item = {"client_event_id": event_id, "type": raw["type"], "occurred_at": occurred,
                            "status": dup["status"], "idempotent": True,
                            "record_id": dup["reference_id"]}
                    if dup["conflict_reason"]:
                        item["error"] = dup["conflict_reason"]
                    results.append(item)
                    continue
                try:
                    reference_id, meta = self._replay_one(conn, raw, occurred)
                    store(event_id, raw["type"], occurred, raw, "merged", "",
                          reference_id, bool(meta.get("idempotent")))
                except DomainError as exc:
                    # Conflict (409/404-style domain collision) waits for adjudication;
                    # it is not replayed against later state automatically.
                    store(event_id, raw["type"], occurred, raw, "conflict", str(exc), None, False)

            summary = self._batch_summary(results)
            status = "conflict" if summary["conflicts"] else "merged"
            conn.execute(
                "UPDATE offline_batches SET status=?,merged_at=?,summary=? WHERE id=?",
                (status, utcnow(), json.dumps(summary, ensure_ascii=False), batch_pk),
            )
            self._audit(conn, None, actor, "offline.batch_merged",
                        {"batch_id": batch_id_ref, "accepted": summary["accepted"],
                         "conflicts": summary["conflicts"], "rejected": summary["rejected"]})
            return {"batch_id": batch_id_ref, "idempotent": False, "status": status, "summary": summary}

    EVENT_ORDER = "occurred_at IS NULL, occurred_at, client_event_id"

    @staticmethod
    def _batch_summary(events: list[Any]) -> dict[str, Any]:
        items: list[dict[str, Any]] = []
        for row in events:
            if isinstance(row, dict):
                items.append(row)
            else:
                items.append({
                    "client_event_id": row["client_event_id"],
                    "type": row["event_type"],
                    "occurred_at": row["occurred_at"],
                    "status": row["status"],
                    "error": row["conflict_reason"],
                    "record_id": row["reference_id"],
                    "idempotent": bool(row["idempotent"]),
                })
        items.sort(key=lambda item: (item.get("occurred_at") is None,
                                     item.get("occurred_at") or "", item.get("client_event_id") or ""))
        return {
            "accepted": sum(1 for item in items if item["status"] == "merged"),
            "conflicts": sum(1 for item in items if item["status"] == "conflict"),
            "rejected": sum(1 for item in items if item["status"] == "rejected"),
            "events": items,
        }

    def list_offline_batches(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            batches = [dict(r) for r in conn.execute(
                "SELECT * FROM offline_batches ORDER BY id DESC").fetchall()]
            events = [dict(r) for r in conn.execute(
                "SELECT * FROM offline_events ORDER BY occurred_at IS NULL, occurred_at, client_event_id").fetchall()]
        grouped: dict[int, list[dict[str, Any]]] = {}
        for event in events:
            event["payload"] = json.loads(event["payload"])
            event["idempotent"] = bool(event["idempotent"])
            grouped.setdefault(event["batch_id"], []).append(event)
        for batch in batches:
            batch["summary"] = json.loads(batch["summary"]) if batch["summary"] else {}
            batch["events"] = grouped.get(batch["id"], [])
        return batches

    def resolve_offline_event(self, actor: str, role: str, client_event_id: str,
                              decision: str, note: str = "") -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "裁决离线冲突")
        if decision not in {"apply", "discard"}:
            raise DomainError("裁决结论必须是 apply 或 discard")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM offline_events WHERE client_event_id=?", (client_event_id,)).fetchone()
            if not row:
                raise DomainError("离线事件不存在", 404)
            if row["status"] != "conflict":
                raise DomainError("该事件不是待裁决冲突项", 409)
            payload = json.loads(row["payload"])
            payload["actor"] = payload.get("actor") or row["client_batch_id"]
            occurred = row["occurred_at"] or utcnow()
            if decision == "discard":
                conn.execute(
                    "UPDATE offline_events SET status='discarded',resolved_by=?,resolved_note=?,processed_at=? WHERE id=?",
                    (actor, note.strip(), utcnow(), row["id"]),
                )
                self._audit(conn, payload.get("incident_id"), actor, "offline.conflict_discarded",
                            {"event_id": client_event_id, "type": row["event_type"], "note": note.strip()}, occurred)
                result = {"client_event_id": client_event_id, "status": "discarded"}
            else:
                reference_id, meta = self._replay_one(conn, payload, occurred, force=True)
                conn.execute(
                    "UPDATE offline_events SET status='merged',conflict_reason='',reference_id=?,"
                    "resolved_by=?,resolved_note=?,processed_at=? WHERE id=?",
                    (reference_id, actor, note.strip(), utcnow(), row["id"]),
                )
                self._audit(conn, payload.get("incident_id"), actor, "offline.conflict_applied",
                            {"event_id": client_event_id, "type": row["event_type"], "note": note.strip()}, occurred)
                result = {"client_event_id": client_event_id, "status": "merged",
                          "record_id": reference_id, "forced": bool(meta.get("forced"))}
            remaining = conn.execute(
                "SELECT * FROM offline_events WHERE batch_id=? ORDER BY occurred_at IS NULL, occurred_at, client_event_id",
                (row["batch_id"],),
            ).fetchall()
            summary = self._batch_summary(remaining)
            conn.execute(
                "UPDATE offline_batches SET status=?,summary=?,merged_at=? WHERE id=?",
                ("conflict" if summary["conflicts"] else "merged",
                 json.dumps(summary, ensure_ascii=False), utcnow(), row["batch_id"]),
            )
            return result

    def state(self, actor: str = "", role: str = "viewer") -> dict[str, Any]:
        with self.connect() as conn:
            incidents = [dict(r) for r in conn.execute("SELECT * FROM incidents ORDER BY id DESC").fetchall()]
            areas = [dict(r) for r in conn.execute("SELECT * FROM search_areas ORDER BY priority,id").fetchall()]
            clues = [dict(r) for r in conn.execute("SELECT * FROM clues ORDER BY id DESC LIMIT 200").fetchall()]
            assets = [dict(r) for r in conn.execute("SELECT * FROM assets ORDER BY id").fetchall()]
            timeline = [dict(r) for r in conn.execute(
                "SELECT * FROM (SELECT * FROM timeline ORDER BY id DESC LIMIT 300) "
                "ORDER BY COALESCE(occurred_at, created_at), id"
            ).fetchall()]
        # Latest batches first for the console; replay order lives inside each summary.
        batches = self.list_offline_batches()
        return {"incidents": incidents, "assets": assets, "search_areas": areas, "clues": clues,
                "timeline": timeline, "offline_batches": batches}

    def incident_timeline(self, incident_id: int) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM timeline WHERE incident_id=? ORDER BY COALESCE(occurred_at, created_at), id",
                (incident_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def seed_demo(self) -> dict[str, Any]:
        with self.connect() as conn:
            if conn.execute("SELECT COUNT(*) AS c FROM incidents").fetchone()["c"]:
                return {"seeded": False, "reason": "已有数据"}
        incident = self.create_incident("coord-demo", "coordinator", "SAR-2026-001", "远星号", 31.2, 122.5, 15.0, 3, "东海搜救中心", description="演示遇险事件")
        self.add_asset("coord-demo", "coordinator", "海巡01", "vessel", ["surface", "night"], 31.0, 122.0, 22.0, 180.0, 6)
        self.add_asset("coord-demo", "coordinator", "救助直升机", "aircraft", ["air", "night"], 30.8, 122.1, 180.0, 260.0, 5)
        self.create_search_area("coord-demo", "coordinator", incident["id"], "AREA-A", "surface", 31.2, 122.5, 20.0, 1, "首要搜索区")
        return {"seeded": True, "incident_id": incident["id"]}


class ApiHandler(BaseHTTPRequestHandler):
    service: MaritimeSARService

    def _send(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _actor(self) -> tuple[str, str]:
        return self.headers.get("X-User", ""), self.headers.get("X-Role", "viewer")

    def _json(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise DomainError("Content-Length 无效") from exc
        if length > 2_000_000:
            raise DomainError("请求体过大", 413)
        if not length:
            return {}
        try:
            data = json.loads(self.rfile.read(length).decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise DomainError("请求体不是有效 JSON") from exc
        if not isinstance(data, dict):
            raise DomainError("JSON 请求体必须是对象")
        return data

    def do_GET(self) -> None:
        try:
            path = urlparse(self.path).path
            if path in {"/", "/index.html"}:
                body = (ROOT / "static" / "index.html").read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if path == "/health":
                self._send(200, {"status": "ok", "service": "maritime-sar"})
                return
            if path == "/api/state":
                self._send(200, self.service.state(*self._actor()))
                return
            if path == "/api/offline/batches":
                self._send(200, {"batches": self.service.list_offline_batches()})
                return
            if path.startswith("/api/incidents/") and path.endswith("/timeline"):
                incident_id = int(path.split("/")[3])
                self._send(200, {"timeline": self.service.incident_timeline(incident_id)})
                return
            self._send(404, {"error": "接口不存在"})
        except (DomainError, ValueError) as exc:
            self._send(getattr(exc, "status", 400), {"error": str(exc)})

    def do_POST(self) -> None:
        try:
            path = urlparse(self.path).path
            data, (actor, role) = self._json(), self._actor()
            if path == "/api/incidents":
                result = self.service.create_incident(actor, role, **data)
            elif path == "/api/assets":
                result = self.service.add_asset(actor, role, **data)
            elif path == "/api/areas":
                result = self.service.create_search_area(actor, role, **data)
            elif path == "/api/assignments":
                result = self.service.assign_area(actor, role, **data)
            elif path == "/api/clues":
                result = self.service.record_clue(actor, role, **data)
            elif path == "/api/clues/verify":
                result = self.service.verify_clue(actor, role, **data)
            elif path == "/api/assets/withdraw":
                result = self.service.withdraw_asset(actor, role, **data)
            elif path == "/api/areas/complete":
                result = self.service.complete_area(actor, role, **data)
            elif path == "/api/incidents/transfer":
                result = self.service.transfer_incident(actor, role, **data)
            elif path == "/api/incidents/close":
                result = self.service.close_incident(actor, role, **data)
            elif path == "/api/offline/batch":
                result = self.service.merge_offline_batch(actor, role, **data)
            elif path == "/api/offline/resolve":
                result = self.service.resolve_offline_event(actor, role, **data)
            else:
                raise DomainError("接口不存在", 404)
            self._send(201, result)
        except DomainError as exc:
            self._send(exc.status, {"error": str(exc)})
        except (KeyError, TypeError, ValueError) as exc:
            self._send(400, {"error": "请求参数错误: %s" % exc})
        except Exception as exc:
            self._send(500, {"error": "服务器内部错误", "detail": str(exc)})

    def log_message(self, fmt: str, *args: Any) -> None:
        return


def serve(service: MaritimeSARService, host: str, port: int) -> None:
    ApiHandler.service = service
    server = ThreadingHTTPServer((host, port), ApiHandler)
    print("Maritime SAR service listening on http://%s:%s" % (host, port))
    server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description="海上搜救协调服务")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8206)
    parser.add_argument("--init", action="store_true")
    parser.add_argument("--seed", action="store_true")
    args = parser.parse_args()
    service = MaritimeSARService(args.db)
    if args.init:
        print(json.dumps(service.seed_demo() if args.seed else {"initialized": True, "db": args.db}, ensure_ascii=False))
        return
    serve(service, args.host, args.port)


if __name__ == "__main__":
    main()
