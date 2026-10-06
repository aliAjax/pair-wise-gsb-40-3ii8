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
OFFLINE_TYPE_ALIASES = {
    "clue": "clue",
    "timeline": "timeline",
    "assign": "asset.assign",
    "asset.assign": "asset.assign",
    "area.withdraw": "area.withdraw",
    "withdraw_area": "area.withdraw",
    "asset.withdraw": "asset.withdraw",
    "withdraw": "asset.withdraw",
    "incident.transfer": "incident.transfer",
    "transfer": "incident.transfer",
}
# 需要现场时间才能按处置顺序重放的操作
OFFLINE_TIMED_TYPES = {"asset.assign", "area.withdraw", "asset.withdraw", "incident.transfer"}


class DomainError(Exception):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_occurred_at(value: Any) -> str | None:
    """解析现场时间（ISO 8601，兼容末尾 Z），返回归一化 ISO 字符串。"""
    if value is None or str(value).strip() == "":
        return None
    text = str(value).strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DomainError("现场时间不是有效的 ISO 8601 时间：%s" % text) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat(timespec="seconds")


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
                    client_event_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    incident_id INTEGER,
                    occurred_at TEXT,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error TEXT NOT NULL DEFAULT '',
                    conflict_code TEXT NOT NULL DEFAULT '',
                    record_type TEXT NOT NULL DEFAULT '',
                    record_id INTEGER,
                    idempotent INTEGER NOT NULL DEFAULT 0,
                    resolved_by TEXT NOT NULL DEFAULT '',
                    resolved_at TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS timeline (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_id INTEGER REFERENCES incidents(id),
                    actor TEXT NOT NULL,
                    action TEXT NOT NULL,
                    details TEXT NOT NULL,
                    occurred_at TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_clues_incident ON clues(incident_id, recorded_at);
                CREATE INDEX IF NOT EXISTS idx_timeline_incident ON timeline(incident_id, occurred_at, id);
                CREATE INDEX IF NOT EXISTS idx_offline_events_batch ON offline_events(batch_id, id);
                CREATE INDEX IF NOT EXISTS idx_offline_events_event ON offline_events(client_event_id);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_offline_events_batch_event
                    ON offline_events(batch_id, client_event_id);
                CREATE INDEX IF NOT EXISTS idx_offline_events_status ON offline_events(status);
                """
            )
            self._migrate_schema(conn)

    def _migrate_schema(self, conn: sqlite3.Connection) -> None:
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(timeline)").fetchall()}
        if "occurred_at" not in columns:
            conn.execute("ALTER TABLE timeline ADD COLUMN occurred_at TEXT")

    def _audit(self, conn: sqlite3.Connection, incident_id: int | None, actor: str, action: str,
               details: dict[str, Any], occurred_at: str | None = None) -> None:
        conn.execute(
            "INSERT INTO timeline(incident_id,actor,action,details,occurred_at,created_at) VALUES(?,?,?,?,?,?)",
            (incident_id, actor, action, json_dump(details), occurred_at, utcnow()),
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

    def merge_offline_batch(self, actor: str, role: str, client_batch_id: str,
                            events: list[dict[str, Any]]) -> dict[str, Any]:
        """合并一批离线记录：按现场时间逐条重放，成功即生效，冲突挂起待裁决。"""
        actor = clean_actor(actor)
        require_role(role, {"operator", "field", "coordinator"}, "合并离线记录")
        batch_key = client_batch_id.strip()
        if not batch_key or not isinstance(events, list):
            raise DomainError("批次编号和事件列表不能为空")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT * FROM offline_batches WHERE client_batch_id=?", (batch_key,)
            ).fetchone()
            if existing:
                return self._batch_response(conn, existing)

            ordered = []
            for upload_index, event in enumerate(events):
                event_id = str(event.get("client_event_id", "")).strip()
                occurred = None
                try:
                    occurred = parse_occurred_at(event.get("occurred_at"))
                except DomainError:
                    pass
                ordered.append((upload_index, event_id, occurred, event))
            # 上传顺序不代替处置顺序：按现场时间排序；无现场时间的（线索/备注）按原上传次序排在其后
            ordered.sort(key=lambda item: (
                item[2] is None,
                item[2] or "",
                item[0],
            ))

            now = utcnow()
            batch_cur = conn.execute(
                "INSERT INTO offline_batches(client_batch_id,actor,status,received_at,summary) VALUES(?,?,?,?,?)",
                (batch_key, actor, "merged", now, json_dump({"accepted": 0, "conflicted": 0, "rejected": 0, "events": []})),
            )
            batch_pk = int(batch_cur.lastrowid)

            for upload_index, event_id, occurred, event in ordered:
                self._replay_offline_event(conn, actor, batch_pk, upload_index, event_id, occurred, event)

            ledger_rows = conn.execute(
                "SELECT * FROM offline_events WHERE batch_id=? ORDER BY id", (batch_pk,)
            ).fetchall()
            ledger = [self._offline_event_result(row) for row in ledger_rows]
            idempotent_events = [r["client_event_id"] for r in ledger if r["idempotent"]]
            accepted = sum(1 for r in ledger if r["status"] == "merged")
            conflicted = sum(1 for r in ledger if r["status"] == "conflict")
            rejected = sum(1 for r in ledger if r["status"] == "rejected")
            batch_status = "pending_review" if conflicted else "merged"
            summary = {
                "accepted": accepted,
                "conflicted": conflicted,
                "rejected": rejected,
                "idempotent_events": idempotent_events,
                "events": ledger,
            }
            conn.execute(
                "UPDATE offline_batches SET status=?,merged_at=?,summary=? WHERE id=?",
                (batch_status, now, json_dump(summary), batch_pk),
            )
            self._audit(conn, None, actor, "offline.batch_merged",
                        {"batch_id": batch_key, "accepted": accepted, "conflicted": conflicted, "rejected": rejected})
            return {"batch_id": batch_key, "idempotent": False, "status": batch_status, "summary": summary}

    def _batch_response(self, conn: sqlite3.Connection, batch_row: sqlite3.Row) -> dict[str, Any]:
        rows = conn.execute(
            "SELECT * FROM offline_events WHERE batch_id=? ORDER BY id", (batch_row["id"],)
        ).fetchall()
        ledger = [self._offline_event_result(row) for row in rows]
        status = self._recompute_batch_status(rows) if rows else batch_row["status"]
        # 裁决可能改变批次内事件状态，计数以 ledger 实时重算
        return {
            "batch_id": batch_row["client_batch_id"],
            "idempotent": True,
            "status": status,
            "summary": {
                "accepted": sum(1 for e in ledger if e["status"] == "merged"),
                "conflicted": sum(1 for e in ledger if e["status"] == "conflict"),
                "rejected": sum(1 for e in ledger if e["status"] == "rejected"),
                "events": ledger,
            },
        }

    def _offline_event_result(self, row: sqlite3.Row) -> dict[str, Any]:
        result = {
            "client_event_id": row["client_event_id"],
            "type": row["event_type"],
            "incident_id": row["incident_id"],
            "occurred_at": row["occurred_at"],
            "status": row["status"],
            "idempotent": bool(row["idempotent"]),
            "record_type": row["record_type"] or None,
            "record_id": row["record_id"],
        }
        if row["error"]:
            result["error"] = row["error"]
        if row["conflict_code"]:
            result["conflict_code"] = row["conflict_code"]
        if row["status"] == "conflict":
            result["resolution"] = "pending"
        elif row["status"] in {"merged", "rejected"} and row["resolved_at"]:
            result["resolution"] = "applied" if row["status"] == "merged" else "dismissed"
        return {k: v for k, v in result.items() if v is not None or k in ("record_id",)}

    def _replay_offline_event(self, conn: sqlite3.Connection, actor: str, batch_pk: int,
                              upload_index: int, event_id: str, occurred_at: str | None,
                              event: dict[str, Any]) -> dict[str, Any]:
        raw_type = str(event.get("type", "")).strip()
        event_type = OFFLINE_TYPE_ALIASES.get(raw_type)
        if not event_id:
            return self._record_offline_event(
                conn, batch_pk, "", raw_type or "unknown", None, None, event,
                "rejected", "离线事件缺少 client_event_id", "", upload_index)
        prior = conn.execute(
            "SELECT * FROM offline_events WHERE client_event_id=? AND idempotent=0 ORDER BY id LIMIT 1",
            (event_id,),
        ).fetchone()
        # 已有原操作（可能仍是待裁决），不能再接受同一事件的第二份"原操作"
        if prior:
            in_batch = conn.execute(
                "SELECT 1 FROM offline_events WHERE batch_id=? AND client_event_id=? AND idempotent=0",
                (batch_pk, event_id),
            ).fetchone()
            if in_batch:
                result = self._offline_event_result(prior)
                result["idempotent"] = True
                return result
            # 跨批次/重试幂等：同一事件绝不第二次生效，不重复占用资源；在当前批次留一条幂等命中记录
            return self._record_offline_event(
                conn, batch_pk, event_id, prior["event_type"], prior["incident_id"],
                prior["occurred_at"], event, prior["status"], prior["error"],
                prior["conflict_code"], upload_index, record_type=prior["record_type"],
                record_id=prior["record_id"], idempotent=1)
        if event_type is None:
            return self._record_offline_event(
                conn, batch_pk, event_id, raw_type, None, occurred_at, event,
                "rejected", "不支持的离线事件类型：%s" % raw_type, "", upload_index)
        if event_type in OFFLINE_TIMED_TYPES and not occurred_at:
            return self._record_offline_event(
                conn, batch_pk, event_id, event_type, event.get("incident_id"),
                None, event, "rejected", "该操作缺少 occurred_at 现场时间，无法按处置顺序重放",
                "missing_occurred_at", upload_index)
        raw_time = event.get("occurred_at")
        if occurred_at is None and raw_time is not None and str(raw_time).strip():
            return self._record_offline_event(
                conn, batch_pk, event_id, event_type, event.get("incident_id"),
                None, event, "rejected",
                "现场时间不是有效的 ISO 8601 时间：%s" % str(raw_time).strip(),
                "bad_occurred_at", upload_index)
        try:
            if event_type == "clue":
                return self._replay_clue(conn, actor, batch_pk, event_id, occurred_at, event, upload_index)
            if event_type == "timeline":
                return self._replay_timeline(conn, actor, batch_pk, event_id, occurred_at, event, upload_index)
            if event_type == "asset.assign":
                return self._replay_assign(conn, actor, batch_pk, event_id, occurred_at, event, upload_index)
            if event_type == "area.withdraw":
                return self._replay_area_withdraw(conn, actor, batch_pk, event_id, occurred_at, event, upload_index)
            if event_type == "asset.withdraw":
                return self._replay_asset_withdraw(conn, actor, batch_pk, event_id, occurred_at, event, upload_index)
            if event_type == "incident.transfer":
                return self._replay_transfer(conn, actor, batch_pk, event_id, occurred_at, event, upload_index)
        except DomainError as exc:
            # 并发/状态类冲突（岸端已改资源、已结束事件）挂起等待裁决；数据类硬错误直接拒绝
            if exc.status == 409:
                return self._record_offline_event(
                    conn, batch_pk, event_id, event_type, event.get("incident_id"),
                    occurred_at, event, "conflict", str(exc), self._conflict_code(exc), upload_index)
            return self._record_offline_event(
                conn, batch_pk, event_id, event_type, event.get("incident_id"),
                occurred_at, event, "rejected", str(exc), "", upload_index)
        except (KeyError, TypeError, ValueError) as exc:
            return self._record_offline_event(
                conn, batch_pk, event_id, event_type, event.get("incident_id"),
                occurred_at, event, "rejected", "事件参数错误：%s" % exc, "", upload_index)
        return self._record_offline_event(
            conn, batch_pk, event_id, event_type, event.get("incident_id"),
            occurred_at, event, "rejected", "不支持的离线事件类型", "", upload_index)

    def _conflict_code(self, exc: DomainError) -> str:
        message = str(exc)
        if "事件" in message and ("结束" in message or "关闭" in message):
            return "incident_closed"
        if "版本" in message or "已变化" in message:
            return "version_mismatch"
        if "占用" in message or "不可用" in message:
            return "asset_busy"
        if "不可分配" in message:
            return "incident_not_active"
        return "state_conflict"

    def _record_offline_event(self, conn: sqlite3.Connection, batch_pk: int, event_id: str,
                              event_type: str, incident_id: Any, occurred_at: str | None,
                              event: dict[str, Any], status: str, error: str,
                              conflict_code: str, upload_index: int,
                              record_type: str = "", record_id: int | None = None,
                              idempotent: int = 0) -> dict[str, Any]:
        safe_incident = None
        try:
            if incident_id is not None:
                safe_incident = int(incident_id)
        except (TypeError, ValueError):
            safe_incident = None
        row_id = event_id or "batch-%d-event-%d" % (batch_pk, upload_index)
        try:
            cur = conn.execute(
                """INSERT INTO offline_events(batch_id,client_event_id,event_type,incident_id,occurred_at,payload,
                   status,error,conflict_code,record_type,record_id,idempotent,created_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (batch_pk, row_id, event_type, safe_incident, occurred_at,
                 json.dumps(event, ensure_ascii=False, sort_keys=True), status, error, conflict_code,
                 record_type, record_id, idempotent, utcnow()),
            )
        except sqlite3.IntegrityError:
            # 同批次内重复的幂等编号：引用本批次首行，不再写入第二份
            existing = conn.execute(
                "SELECT * FROM offline_events WHERE batch_id=? AND client_event_id=? ORDER BY id LIMIT 1",
                (batch_pk, row_id),
            ).fetchone()
            result = self._offline_event_result(existing)
            result["idempotent"] = True
            return result
        result = {
            "client_event_id": row_id,
            "type": event_type,
            "incident_id": safe_incident,
            "occurred_at": occurred_at,
            "status": status,
            "idempotent": bool(idempotent),
            "record_type": record_type or None,
            "record_id": record_id,
        }
        if error:
            result["error"] = error
        if conflict_code:
            result["conflict_code"] = conflict_code
        return {k: v for k, v in result.items() if v is not None or k == "record_id"}

    def _replay_clue(self, conn: sqlite3.Connection, actor: str, batch_pk: int, event_id: str,
                     occurred_at: str | None, event: dict[str, Any], upload_index: int) -> dict[str, Any]:
        incident_id = int(event["incident_id"])
        lat, lon = validate_position(event["latitude"], event["longitude"])
        confidence = float(event["confidence"])
        if not 0 <= confidence <= 1:
            raise DomainError("置信度应在 0 到 1 之间")
        incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
        if not incident:
            raise DomainError("事件不存在", 404)
        if incident["status"] in CLOSED_INCIDENT:
            raise DomainError("岸端已结束事件，线索待裁决", 409)
        area_id = event.get("area_id")
        if area_id is not None and not conn.execute(
            "SELECT 1 FROM search_areas WHERE id=? AND incident_id=?", (area_id, incident_id)
        ).fetchone():
            raise DomainError("搜索区域不属于该事件", 404)
        distance = haversine_km(incident["latitude"], incident["longitude"], lat, lon)
        status = "unverified" if distance <= incident["uncertainty_km"] * 3 else "invalid"
        cur = conn.execute(
            """INSERT INTO clues(incident_id,area_id,client_event_id,latitude,longitude,confidence,source,status,
               distance_from_incident_km,reporter,details,recorded_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (incident_id, area_id, event_id, lat, lon, confidence,
             str(event.get("source", "offline")).strip(), status, distance, actor,
             str(event.get("details", "")).strip(), occurred_at or utcnow()),
        )
        self._audit(conn, incident_id, actor, "clue.recorded",
                    {"clue_id": cur.lastrowid, "status": status, "event_id": event_id, "offline": True},
                    occurred_at)
        return self._record_offline_event(
            conn, batch_pk, event_id, "clue", incident_id, occurred_at, event,
            "merged", "", "", upload_index, record_type="clue", record_id=cur.lastrowid)

    def _replay_timeline(self, conn: sqlite3.Connection, actor: str, batch_pk: int, event_id: str,
                         occurred_at: str | None, event: dict[str, Any], upload_index: int) -> dict[str, Any]:
        incident_id = int(event["incident_id"])
        if not conn.execute("SELECT 1 FROM incidents WHERE id=?", (incident_id,)).fetchone():
            raise DomainError("事件不存在", 404)
        details = event.get("details", {})
        if not isinstance(details, dict):
            details = {"note": details}
        self._audit(conn, incident_id, actor, str(event.get("action", "offline.note")).strip() or "offline.note",
                    details, occurred_at)
        return self._record_offline_event(
            conn, batch_pk, event_id, "timeline", incident_id, occurred_at, event,
            "merged", "", "", upload_index, record_type="timeline")

    def _replay_assign(self, conn: sqlite3.Connection, actor: str, batch_pk: int, event_id: str,
                       occurred_at: str, event: dict[str, Any], upload_index: int) -> dict[str, Any]:
        area_id, asset_id = int(event["area_id"]), int(event["asset_id"])
        expected = event.get("expected_asset_version")
        expected = int(expected) if expected is not None else None
        area = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
        asset = conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()
        if not area or not asset:
            raise DomainError("搜索区域或资源不存在", 404)
        if area["assigned_asset_id"] is not None:
            raise DomainError("岸端已为该区域安排资源，现场分配待裁决", 409)
        incident = conn.execute("SELECT * FROM incidents WHERE id=?", (area["incident_id"],)).fetchone()
        if not incident or incident["status"] not in ACTIVE_INCIDENT:
            raise DomainError("岸端已结束事件，现场分配待裁决", 409)
        if expected is not None and asset["version"] != expected:
            raise DomainError("岸端已改动该资源（现场基线版本 %s，当前版本 %s），占用待裁决" % (expected, asset["version"]), 409)
        if asset["status"] != "available":
            raise DomainError("资源已被其他任务占用，现场分配待裁决", 409)
        # 以下是能力/海况/航程等硬性条件，岸端变化也无法通过裁决消除
        if incident["sea_state"] > asset["max_sea_state"]:
            raise DomainError("海况超出资源能力")
        capabilities = json.loads(asset["capabilities"])
        if area["kind"] not in capabilities:
            raise DomainError("资源不具备该搜索区域能力")
        distance = haversine_km(asset["latitude"], asset["longitude"], area["center_lat"], area["center_lon"])
        if distance > asset["range_km"]:
            raise DomainError("搜索区域超出资源航程")
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
        self._audit(conn, area["incident_id"], actor, "area.assigned",
                    {"area_id": area_id, "asset_id": asset_id, "distance_km": round(distance, 2),
                     "event_id": event_id, "offline": True, "occurred_at": occurred_at},
                    occurred_at)
        return self._record_offline_event(
            conn, batch_pk, event_id, "asset.assign", area["incident_id"], occurred_at, event,
            "merged", "", "", upload_index, record_type="search_area", record_id=area_id)

    def _replay_area_withdraw(self, conn: sqlite3.Connection, actor: str, batch_pk: int, event_id: str,
                              occurred_at: str, event: dict[str, Any], upload_index: int) -> dict[str, Any]:
        """现场撤回区域：释放该区域占用的资源（区域撤回）。"""
        area_id = int(event["area_id"])
        reason = str(event.get("reason", "现场撤回")).strip() or "现场撤回"
        area = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
        if not area:
            raise DomainError("搜索区域不存在", 404)
        if area["status"] in {"completed", "abandoned"}:
            raise DomainError("岸端已结束该搜索区域，撤回待裁决", 409)
        expected = event.get("expected_area_version")
        if expected is not None and area["version"] != int(expected):
            raise DomainError("岸端已改动该搜索区域，撤回待裁决", 409)
        now = utcnow()
        asset_id = area["assigned_asset_id"]
        if asset_id is not None:
            conn.execute(
                "UPDATE assets SET status='available',version=version+1,updated_at=? WHERE id=?",
                (now, asset_id),
            )
        conn.execute(
            "UPDATE search_areas SET assigned_asset_id=NULL,status='planned',version=version+1,updated_at=? WHERE id=?",
            (now, area_id),
        )
        self._audit(conn, area["incident_id"], actor, "area.unassigned",
                    {"area_id": area_id, "asset_id": asset_id, "reason": reason,
                     "event_id": event_id, "offline": True, "occurred_at": occurred_at},
                    occurred_at)
        return self._record_offline_event(
            conn, batch_pk, event_id, "area.withdraw", area["incident_id"], occurred_at, event,
            "merged", "", "", upload_index, record_type="search_area", record_id=area_id)

    def _replay_asset_withdraw(self, conn: sqlite3.Connection, actor: str, batch_pk: int, event_id: str,
                               occurred_at: str, event: dict[str, Any], upload_index: int) -> dict[str, Any]:
        """现场撤回资源：释放该资源承担的全部在执行区域。"""
        asset_id = int(event["asset_id"])
        reason = str(event.get("reason", "现场撤回资源")).strip() or "现场撤回资源"
        asset = conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()
        if not asset:
            raise DomainError("资源不存在", 404)
        expected = event.get("expected_asset_version")
        if expected is not None and asset["version"] != int(expected):
            raise DomainError("岸端已改动该资源，撤回待裁决", 409)
        if asset["status"] == "available":
            raise DomainError("资源当前未分配，撤回待裁决", 409)
        now = utcnow()
        areas = conn.execute(
            "SELECT id,incident_id FROM search_areas WHERE assigned_asset_id=? AND status IN ('assigned','active')",
            (asset_id,),
        ).fetchall()
        incident_id = None
        for area_row in areas:
            conn.execute(
                "UPDATE search_areas SET assigned_asset_id=NULL,status='planned',version=version+1,updated_at=? WHERE id=?",
                (now, area_row["id"]),
            )
            self._audit(conn, area_row["incident_id"], actor, "area.unassigned",
                        {"area_id": area_row["id"], "reason": reason, "event_id": event_id,
                         "offline": True, "occurred_at": occurred_at}, occurred_at)
            incident_id = area_row["incident_id"]
        conn.execute("UPDATE assets SET status='available',version=version+1,updated_at=? WHERE id=?", (now, asset_id))
        self._audit(conn, None, actor, "asset.withdrawn",
                    {"asset_id": asset_id, "reason": reason, "event_id": event_id,
                     "offline": True, "occurred_at": occurred_at}, occurred_at)
        return self._record_offline_event(
            conn, batch_pk, event_id, "asset.withdraw", incident_id, occurred_at, event,
            "merged", "", "", upload_index, record_type="asset", record_id=asset_id)

    def _replay_transfer(self, conn: sqlite3.Connection, actor: str, batch_pk: int, event_id: str,
                         occurred_at: str, event: dict[str, Any], upload_index: int) -> dict[str, Any]:
        incident_id = int(event["incident_id"])
        new_org = str(event.get("new_org", "")).strip()
        if not new_org:
            raise DomainError("接收机构不能为空")
        note = str(event.get("note", "")).strip()
        incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
        if not incident:
            raise DomainError("事件不存在", 404)
        if incident["status"] in CLOSED_INCIDENT:
            raise DomainError("岸端已结束事件，现场移交待裁决", 409)
        expected = event.get("expected_version")
        if expected is not None and incident["version"] != int(expected):
            raise DomainError("岸端已改动事件（现场基线版本 %s，当前版本 %s），移交待裁决" % (expected, incident["version"]), 409)
        conn.execute(
            "UPDATE incidents SET lead_org=?,version=version+1,updated_at=? WHERE id=?",
            (new_org, utcnow(), incident_id),
        )
        self._audit(conn, incident_id, actor, "incident.transferred",
                    {"from": incident["lead_org"], "to": new_org, "note": note,
                     "event_id": event_id, "offline": True, "occurred_at": occurred_at},
                    occurred_at)
        return self._record_offline_event(
            conn, batch_pk, event_id, "incident.transfer", incident_id, occurred_at, event,
            "merged", "", "", upload_index, record_type="incident", record_id=incident_id)

    def _recompute_batch_status(self, rows: list[sqlite3.Row]) -> str:
        return "pending_review" if any(r["status"] == "conflict" for r in rows) else "merged"

    def resolve_offline_conflict(self, actor: str, role: str, client_event_id: str,
                                 resolution: str, force: bool = False, note: str = "") -> dict[str, Any]:
        """对挂起的离线冲突进行裁决：force=True 按现场意图强制执行，否则驳回保留岸端现状。"""
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "裁决离线冲突")
        event_key = client_event_id.strip()
        if resolution not in {"apply", "dismiss"}:
            raise DomainError("裁决结论必须是 apply 或 dismiss")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM offline_events WHERE client_event_id=? AND idempotent=0 ORDER BY id LIMIT 1",
                (event_key,),
            ).fetchone()
            if not row:
                raise DomainError("离线事件不存在", 404)
            if row["status"] != "conflict":
                # 同步本事件在其他批次里的幂等引用状态
                conn.execute(
                    "UPDATE offline_events SET status=?,resolved_by=?,resolved_at=? WHERE client_event_id=? AND idempotent=1",
                    (row["status"], actor, utcnow(), event_key),
                )
                return {"client_event_id": event_key, "idempotent": True, "status": row["status"]}
            payload = json.loads(row["payload"])
            occurred_at = row["occurred_at"]
            if resolution == "apply":
                if row["event_type"] == "clue":
                    self._adjudicate_clue(conn, actor, row, payload, force)
                elif row["event_type"] == "asset.assign":
                    self._adjudicate_assign(conn, actor, row, payload, force)
                elif row["event_type"] == "area.withdraw":
                    self._adjudicate_area_withdraw(conn, actor, row, payload, force)
                elif row["event_type"] == "asset.withdraw":
                    self._adjudicate_asset_withdraw(conn, actor, row, payload, force)
                elif row["event_type"] == "incident.transfer":
                    self._adjudicate_transfer(conn, actor, row, payload, force)
                else:
                    raise DomainError("该冲突类型无法强制执行")
                new_status = "merged"
            else:
                new_status = "rejected"
            now = utcnow()
            conn.execute(
                "UPDATE offline_events SET status=?,resolved_by=?,resolved_at=? WHERE id=?",
                (new_status, actor, now, row["id"]),
            )
            # 其他批次里对同一事件的幂等引用同步裁决结论，不重复执行业务
            conn.execute(
                "UPDATE offline_events SET status=?,resolved_by=?,resolved_at=? WHERE client_event_id=? AND idempotent=1",
                (new_status, actor, now, row["client_event_id"]),
            )
            affected_batches = [row["batch_id"]] + [
                r["batch_id"] for r in conn.execute(
                    "SELECT DISTINCT batch_id FROM offline_events WHERE client_event_id=? AND idempotent=1",
                    (row["client_event_id"],),
                ).fetchall()
            ]
            for batch_pk in set(affected_batches):
                batch_rows = conn.execute(
                    "SELECT * FROM offline_events WHERE batch_id=?", (batch_pk,)
                ).fetchall()
                conn.execute(
                    "UPDATE offline_batches SET status=? WHERE id=?",
                    (self._recompute_batch_status(batch_rows), batch_pk),
                )
            batch = conn.execute("SELECT * FROM offline_batches WHERE id=?", (row["batch_id"],)).fetchone()
            batch_rows = conn.execute("SELECT * FROM offline_events WHERE batch_id=?", (row["batch_id"],)).fetchall()
            batch_status = self._recompute_batch_status(batch_rows)
            self._audit(conn, row["incident_id"], actor, "offline.conflict_resolved",
                        {"client_event_id": event_key, "type": row["event_type"], "resolution": resolution,
                         "forced": bool(force and resolution == "apply"), "note": note.strip(),
                         "original_conflict": row["conflict_code"]}, occurred_at)
            updated = conn.execute("SELECT * FROM offline_events WHERE id=?", (row["id"],)).fetchone()
            return {
                "client_event_id": event_key,
                "idempotent": False,
                "status": new_status,
                "batch_id": batch["client_batch_id"],
                "batch_status": batch_status,
                "event": self._offline_event_result(updated),
            }

    def _adjudicate_clue(self, conn: sqlite3.Connection, actor: str, row: sqlite3.Row,
                         payload: dict[str, Any], force: bool) -> None:
        incident_id = int(payload["incident_id"])
        if not force:
            raise DomainError("事件已结束，需显式 force 才能补录线索", 409)
        lat, lon = validate_position(payload["latitude"], payload["longitude"])
        confidence = float(payload["confidence"])
        incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
        if not incident:
            raise DomainError("事件不存在", 404)
        distance = haversine_km(incident["latitude"], incident["longitude"], lat, lon)
        status = "unverified" if distance <= incident["uncertainty_km"] * 3 else "invalid"
        cur = conn.execute(
            """INSERT INTO clues(incident_id,area_id,client_event_id,latitude,longitude,confidence,source,status,
               distance_from_incident_km,reporter,details,recorded_at,merged_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (incident_id, payload.get("area_id"), row["client_event_id"], lat, lon, confidence,
             str(payload.get("source", "offline")).strip(), status, distance, actor,
             str(payload.get("details", "")).strip(), row["occurred_at"] or utcnow(), utcnow()),
        )
        self._audit(conn, incident_id, actor, "clue.recorded",
                    {"clue_id": cur.lastrowid, "status": status, "event_id": row["client_event_id"],
                     "offline": True, "adjudicated": True}, row["occurred_at"])
        conn.execute(
            "UPDATE offline_events SET record_type='clue',record_id=? WHERE id=?", (cur.lastrowid, row["id"])
        )

    def _adjudicate_assign(self, conn: sqlite3.Connection, actor: str, row: sqlite3.Row,
                           payload: dict[str, Any], force: bool) -> None:
        area = conn.execute("SELECT * FROM search_areas WHERE id=?", (int(payload["area_id"]),)).fetchone()
        asset = conn.execute("SELECT * FROM assets WHERE id=?", (int(payload["asset_id"]),)).fetchone()
        if not area or not asset:
            raise DomainError("搜索区域或资源不存在", 404)
        incident = conn.execute("SELECT * FROM incidents WHERE id=?", (area["incident_id"],)).fetchone()
        if not incident:
            raise DomainError("事件不存在", 404)
        if incident["status"] in CLOSED_INCIDENT or area["status"] in {"completed", "abandoned"}:
            raise DomainError("事件/区域已结束，无法再占用资源；如确需处置请先重开事件", 409)
        if area["assigned_asset_id"] is not None:
            other = area["assigned_asset_id"]
            if force and other != asset["id"]:
                # 以现场裁决为准：顶掉岸端安排，原资源释放
                conn.execute(
                    "UPDATE assets SET status='available',version=version+1,updated_at=? WHERE id=?",
                    (utcnow(), other),
                )
                self._audit(conn, area["incident_id"], actor, "area.unassigned",
                            {"area_id": area["id"], "asset_id": other, "reason": "离线冲突裁决改派",
                             "event_id": row["client_event_id"]}, row["occurred_at"])
            elif other == asset["id"]:
                pass
            else:
                raise DomainError("岸端已为该区域安排其他资源，需 force 才能改派", 409)
        if asset["status"] != "available" and not (area["assigned_asset_id"] == asset["id"]):
            if not force:
                raise DomainError("资源已被其他任务占用，需 force 才能抢占", 409)
            for other_area in conn.execute(
                "SELECT id,incident_id FROM search_areas WHERE assigned_asset_id=? AND id<>? AND status IN ('assigned','active')",
                (asset["id"], area["id"]),
            ).fetchall():
                conn.execute(
                    "UPDATE search_areas SET assigned_asset_id=NULL,status='planned',version=version+1,updated_at=? WHERE id=?",
                    (utcnow(), other_area["id"]),
                )
        # 硬性能力条件在裁决时仍不放松
        if incident["sea_state"] > asset["max_sea_state"]:
            raise DomainError("海况超出资源能力")
        capabilities = json.loads(asset["capabilities"])
        if area["kind"] not in capabilities:
            raise DomainError("资源不具备该搜索区域能力")
        distance = haversine_km(asset["latitude"], asset["longitude"], area["center_lat"], area["center_lon"])
        if distance > asset["range_km"]:
            raise DomainError("搜索区域超出资源航程")
        now = utcnow()
        conn.execute(
            "UPDATE assets SET status='assigned',version=version+1,updated_at=? WHERE id=?",
            (now, asset["id"]),
        )
        conn.execute(
            "UPDATE search_areas SET assigned_asset_id=?,status='assigned',version=version+1,updated_at=? WHERE id=?",
            (asset["id"], now, area["id"]),
        )
        self._audit(conn, area["incident_id"], actor, "area.assigned",
                    {"area_id": area["id"], "asset_id": asset["id"], "distance_km": round(distance, 2),
                     "event_id": row["client_event_id"], "offline": True, "adjudicated": True, "forced": force},
                    row["occurred_at"])
        conn.execute(
            "UPDATE offline_events SET record_type='search_area',record_id=? WHERE id=?",
            (area["id"], row["id"]),
        )

    def _adjudicate_area_withdraw(self, conn: sqlite3.Connection, actor: str, row: sqlite3.Row,
                                  payload: dict[str, Any], force: bool) -> None:
        area = conn.execute("SELECT * FROM search_areas WHERE id=?", (int(payload["area_id"]),)).fetchone()
        if not area:
            raise DomainError("搜索区域不存在", 404)
        if area["status"] in {"completed", "abandoned"} and not force:
            raise DomainError("区域已结束，需 force 才能按现场撤回重开为待派", 409)
        now = utcnow()
        if area["assigned_asset_id"] is not None:
            conn.execute(
                "UPDATE assets SET status='available',version=version+1,updated_at=? WHERE id=?",
                (now, area["assigned_asset_id"]),
            )
        conn.execute(
            "UPDATE search_areas SET assigned_asset_id=NULL,status='planned',version=version+1,updated_at=? WHERE id=?",
            (now, area["id"]),
        )
        self._audit(conn, area["incident_id"], actor, "area.unassigned",
                    {"area_id": area["id"], "asset_id": area["assigned_asset_id"],
                     "reason": str(payload.get("reason", "现场撤回")), "event_id": row["client_event_id"],
                     "offline": True, "adjudicated": True, "forced": force}, row["occurred_at"])
        conn.execute(
            "UPDATE offline_events SET record_type='search_area',record_id=? WHERE id=?",
            (area["id"], row["id"]),
        )

    def _adjudicate_asset_withdraw(self, conn: sqlite3.Connection, actor: str, row: sqlite3.Row,
                                   payload: dict[str, Any], force: bool) -> None:
        asset = conn.execute("SELECT * FROM assets WHERE id=?", (int(payload["asset_id"]),)).fetchone()
        if not asset:
            raise DomainError("资源不存在", 404)
        if asset["status"] == "available" and not force:
            raise DomainError("资源已被岸端释放，需 force 确认现场撤回结论", 409)
        now = utcnow()
        areas = conn.execute(
            "SELECT id,incident_id FROM search_areas WHERE assigned_asset_id=? AND status IN ('assigned','active')",
            (asset["id"],),
        ).fetchall()
        incident_id = row["incident_id"]
        for area_row in areas:
            conn.execute(
                "UPDATE search_areas SET assigned_asset_id=NULL,status='planned',version=version+1,updated_at=? WHERE id=?",
                (now, area_row["id"]),
            )
            self._audit(conn, area_row["incident_id"], actor, "area.unassigned",
                        {"area_id": area_row["id"], "reason": "离线冲突裁决撤回资源",
                         "event_id": row["client_event_id"]}, row["occurred_at"])
            incident_id = area_row["incident_id"]
        conn.execute(
            "UPDATE assets SET status='available',version=version+1,updated_at=? WHERE id=?",
            (now, asset["id"]),
        )
        self._audit(conn, None, actor, "asset.withdrawn",
                    {"asset_id": asset["id"], "reason": str(payload.get("reason", "现场撤回资源")),
                     "event_id": row["client_event_id"], "offline": True, "adjudicated": True, "forced": force},
                    row["occurred_at"])
        conn.execute(
            "UPDATE offline_events SET incident_id=?,record_type='asset',record_id=? WHERE id=?",
            (incident_id, asset["id"], row["id"]),
        )

    def _adjudicate_transfer(self, conn: sqlite3.Connection, actor: str, row: sqlite3.Row,
                             payload: dict[str, Any], force: bool) -> None:
        incident = conn.execute("SELECT * FROM incidents WHERE id=?", (int(payload["incident_id"]),)).fetchone()
        if not incident:
            raise DomainError("事件不存在", 404)
        if incident["status"] in CLOSED_INCIDENT:
            raise DomainError("事件已结束，移交不再生效；如仍需变更负责机构请先重开事件", 409)
        new_org = str(payload.get("new_org", "")).strip()
        if not new_org:
            raise DomainError("接收机构不能为空")
        if incident["lead_org"] == new_org:
            return
        if not force:
            raise DomainError("岸端已有更新的负责机构，需 force 才能以现场移交为准", 409)
        conn.execute(
            "UPDATE incidents SET lead_org=?,version=version+1,updated_at=? WHERE id=?",
            (new_org, utcnow(), incident["id"]),
        )
        self._audit(conn, incident["id"], actor, "incident.transferred",
                    {"from": incident["lead_org"], "to": new_org, "note": str(payload.get("note", "")),
                     "event_id": row["client_event_id"], "offline": True, "adjudicated": True, "forced": True},
                    row["occurred_at"])
        conn.execute(
            "UPDATE offline_events SET record_type='incident',record_id=? WHERE id=?",
            (incident["id"], row["id"]),
        )

    def list_offline_batches(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            batches = conn.execute("SELECT * FROM offline_batches ORDER BY id DESC").fetchall()
            result = []
            for batch in batches:
                rows = conn.execute(
                    "SELECT * FROM offline_events WHERE batch_id=? ORDER BY id", (batch["id"],)
                ).fetchall()
                events = [self._offline_event_result(row) for row in rows]
                summary = json.loads(batch["summary"])
                result.append({
                    "id": batch["id"],
                    "batch_id": batch["client_batch_id"],
                    "actor": batch["actor"],
                    "status": self._recompute_batch_status(rows) if rows else batch["status"],
                    "received_at": batch["received_at"],
                    "merged_at": batch["merged_at"],
                    "counts": {
                        "accepted": sum(1 for e in events if e["status"] == "merged"),
                        "conflicted": sum(1 for e in events if e["status"] == "conflict"),
                        "rejected": sum(1 for e in events if e["status"] == "rejected"),
                    },
                    "stored_counts": {k: summary.get(k, 0) for k in ("accepted", "conflicted", "rejected")},
                    "events": events,
                })
        return result

    def state(self, actor: str = "", role: str = "viewer") -> dict[str, Any]:
        with self.connect() as conn:
            incidents = [dict(r) for r in conn.execute("SELECT * FROM incidents ORDER BY id DESC").fetchall()]
            areas = [dict(r) for r in conn.execute("SELECT * FROM search_areas ORDER BY priority,id").fetchall()]
            clues = [dict(r) for r in conn.execute("SELECT * FROM clues ORDER BY id DESC LIMIT 200").fetchall()]
            assets = [dict(r) for r in conn.execute("SELECT * FROM assets ORDER BY id").fetchall()]
            # 现场时间优先：离线补传的处置动作按 occurred_at 归位，而不是按服务器接收时间
            timeline = [dict(r) for r in conn.execute(
                "SELECT * FROM timeline ORDER BY COALESCE(occurred_at, created_at) DESC, id DESC LIMIT 300"
            ).fetchall()]
        return {"incidents": incidents, "assets": assets, "search_areas": areas, "clues": clues,
                "timeline": timeline, "offline_batches": self.list_offline_batches()}

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
            elif path == "/api/offline/conflicts/resolve":
                result = self.service.resolve_offline_conflict(actor, role, **data)
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
