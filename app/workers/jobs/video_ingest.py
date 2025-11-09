import os
import asyncio
import tempfile
from pathlib import Path
from typing import Any, Callable, Mapping

from botocore.exceptions import BotoCoreError, ClientError
from fastapi import HTTPException, status
from rq import get_current_job
from yt_dlp import YoutubeDL

from app.config.env import settings
from app.config.s3 import s3
from app.utils.s3 import build_object_key

from app.workers.jobs.video_ingest_finalizer import finalize_ingest
from app.workers.jobs.video_ingest_progress import (
    DOWNLOAD_PROGRESS_PARTS,
    FINALIZE_PROGRESS_DONE,
    FINALIZE_PROGRESS_START,
    UPLOAD_PROGRESS_DONE,
    UPLOAD_PROGRESS_START,
    emit_progress,
    make_progress_payload,
    map_download_progress,
    download_progress_for_completed_parts,
    update_job_stage,
)


async def _download_youtube_video(
    url: str,
    temp_dir: str,
    *,
    progress_hook: Callable[[dict[str, Any]], None] | None = None,
) -> Path:
    def _download() -> Path:
        ydl_opts = {
            "outtmpl": os.path.join(temp_dir, "%(id)s.%(ext)s"),
            "format": "bestvideo[ext=mp4]+bestaudio/best",
            "merge_output_format": "mp4",
        }
        if progress_hook:
            ydl_opts["progress_hooks"] = [progress_hook]
        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            return Path(ydl.prepare_filename(info))

    return await asyncio.to_thread(_download)


async def _run_ingest_async(payload: Mapping[str, Any]) -> str:
    source_url = payload.get("source_url")
    project_id = payload.get("project_id")

    if not source_url or not project_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid ingest payload",
        )

    bucket = settings.S3_BUCKET
    if not bucket:
        raise HTTPException(status_code=500, detail="AWS_S3_BUCKET env not set")

    job = get_current_job()
    job_id = job.id if job else None
    update_job_stage(job, "downloading", progress=0)
    emit_progress(
        project_id,
        {
            "job_id": job_id,
            "stage": "downloading",
            "status": "다운로드 시작",
            "progress": 5,  # 초기 진행률 설정
        },
    )

    last_download_progress = -1
    completed_parts = 0

    def _progress_hook(status: dict[str, Any]) -> None:
        nonlocal last_download_progress, completed_parts
        progress_payload = make_progress_payload(status)
        if progress_payload is None:
            return
        status_name = status.get("status")
        if status_name == "finished":
            completed_parts = min(completed_parts + 1, DOWNLOAD_PROGRESS_PARTS)
            mapped_progress = download_progress_for_completed_parts(completed_parts)
        else:
            raw_progress = progress_payload.get("progress")
            mapped_progress = map_download_progress(
                raw_progress, completed_parts=completed_parts
            )
            if mapped_progress is None:
                return
        if mapped_progress == last_download_progress:
            return
        last_download_progress = mapped_progress
        progress_payload["progress"] = mapped_progress
        progress_payload.update({"job_id": job_id, "stage": "downloading"})
        emit_progress(project_id, progress_payload)

    # 1) yt 다운로드 + s3 업로드
    ingest_root = Path(settings.INGEST_WORKDIR)
    ingest_root.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(dir=str(ingest_root)) as temp_dir:
        try:
            local_file = await _download_youtube_video(
                source_url, temp_dir, progress_hook=_progress_hook
            )
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="유튜브 영상을 다운로드할 수 없습니다.",
            ) from exc

        object_key = build_object_key(project_id, local_file)

        update_job_stage(job, "uploading", progress=UPLOAD_PROGRESS_START)
        emit_progress(
            project_id,
            {
                "job_id": job_id,
                "stage": "uploading",
                "status": "업로드 시작",
                "progress": UPLOAD_PROGRESS_START,
            },
        )

        try:
            s3.upload_file(str(local_file), bucket, object_key)
        except (BotoCoreError, ClientError) as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="S3 업로드 중 오류가 발생했습니다.",
            ) from exc

    emit_progress(
        project_id,
        {
            "job_id": job_id,
            "stage": "uploading",
            "status": "업로드 완료",
            "progress": UPLOAD_PROGRESS_DONE,
        },
    )

    update_job_stage(job, "finalizing", progress=FINALIZE_PROGRESS_START)
    emit_progress(
        project_id,
        {
            "job_id": job_id,
            "stage": "finalizing",
            "status": "최종 처리 시작",
            "progress": FINALIZE_PROGRESS_START,
        },
    )

    await finalize_ingest(project_id, object_key)

    update_job_stage(job, "done", s3_key=object_key, progress=FINALIZE_PROGRESS_DONE)
    emit_progress(
        project_id,
        {
            "job_id": job_id,
            "stage": "done",
            "status": "최종 처리 완료",
            "s3_key": object_key,
            "progress": FINALIZE_PROGRESS_DONE,
        },
    )
    return object_key


def run_ingest(payload: Mapping[str, Any]) -> str:  # ← RQ가 호출하는 동기 함수
    return asyncio.run(_run_ingest_async(payload))
