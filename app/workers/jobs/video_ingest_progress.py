import json
from typing import Any, Dict

from redis.exceptions import RedisError

from app.config.redis import get_redis

DOWNLOAD_PROGRESS_PARTS = 2
DOWNLOAD_PROGRESS_START = 5
DOWNLOAD_PROGRESS_MAX = 70
UPLOAD_PROGRESS_START = 71
UPLOAD_PROGRESS_DONE = 92
FINALIZE_PROGRESS_START = 93
FINALIZE_PROGRESS_DONE = 100


def _progress_channel(project_id: str) -> str:
    return f"uploads:{project_id}"


redis_conn = get_redis()


def clamp(value: int, *, lower: int = 0, upper: int = 100) -> int:
    return max(lower, min(upper, value))


def emit_progress(project_id: str, payload: Dict[str, Any]) -> None:
    if not project_id or not redis_conn:
        return
    progress = payload.get("progress")
    if progress is not None:
        payload["progress"] = clamp(int(progress))
    try:
        redis_conn.publish(_progress_channel(project_id), json.dumps(payload))
    except RedisError:
        pass


def update_job_stage(
    job, stage: str, *, progress: int | None = None, **meta: Any
) -> None:
    if not job:
        return
    payload = {"stage": stage, **meta}
    if progress is not None:
        payload["progress"] = clamp(int(progress))
    job.meta.update(payload)
    job.save_meta()


def _download_part_span() -> float:
    parts = max(DOWNLOAD_PROGRESS_PARTS, 1)
    return (DOWNLOAD_PROGRESS_MAX - DOWNLOAD_PROGRESS_START) / parts


def map_download_progress(
    raw_pct: int | None, *, completed_parts: int = 0
) -> int | None:
    if raw_pct is None:
        return None
    span = _download_part_span()
    progress = DOWNLOAD_PROGRESS_START + completed_parts * span + (raw_pct / 100) * span
    return clamp(int(progress))


def download_progress_for_completed_parts(completed_parts: int) -> int:
    span = _download_part_span()
    progress = (
        DOWNLOAD_PROGRESS_START + min(completed_parts, DOWNLOAD_PROGRESS_PARTS) * span
    )
    return clamp(int(progress))


def make_progress_payload(status: Dict[str, Any]) -> Dict[str, Any] | None:
    status_name = status.get("status")
    if status_name not in {"downloading", "finished"}:
        return None
    total = status.get("total_bytes") or status.get("total_bytes_estimate")
    downloaded = status.get("downloaded_bytes")
    payload: Dict[str, Any] = {"status": status_name}
    if total and downloaded is not None:
        pct = int(downloaded / total * 100)
        payload["progress"] = clamp(pct, upper=DOWNLOAD_PROGRESS_MAX)
    if status_name == "finished":
        payload["progress"] = 100
    if eta := status.get("eta"):
        payload["eta"] = eta
    return payload
