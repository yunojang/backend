import os
import asyncio
import tempfile
from pathlib import Path
from typing import Any, Mapping

from fastapi import HTTPException, status
from rq import get_current_job
from yt_dlp import YoutubeDL
from botocore.exceptions import BotoCoreError, ClientError

from app.api.jobs.service import start_job
from app.api.pipeline.models import PipelineStatus, PipelineUpdate
from app.api.pipeline.service import update_pipeline_stage
from app.api.project.models import ProjectUpdate
from app.api.project.service import ProjectService
from app.config.db import database
from app.config.env import settings
from app.config.s3 import s3
from app.utils.s3 import build_object_key


async def _download_youtube_video(url: str, temp_dir: str) -> Path:
    def _download() -> Path:
        ydl_opts = {
            "outtmpl": os.path.join(temp_dir, "%(id)s.%(ext)s"),
            "format": "bestvideo[ext=mp4]+bestaudio/best",
            "merge_output_format": "mp4",
        }
        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            return Path(ydl.prepare_filename(info))

    return await asyncio.to_thread(_download)


project_service = ProjectService(database)


async def _finalize_ingest(project_id: str, object_key: str) -> None:
    update_payload = ProjectUpdate(
        project_id=project_id,
        status="upload_done",
        video_source=object_key,
    )
    project = await project_service.update_project(payload=update_payload)

    await start_job(project, database)
    await update_pipeline_stage(
        database,
        PipelineUpdate(
            project_id=project_id,
            stage_id="upload",
            status=PipelineStatus.COMPLETED,
            progress=100,
        ),
    )


async def _run_ingest_async(payload: Mapping[str, Any]) -> str:
    source_url = payload.get("source_url")
    project_id = payload.get("project_id")

    print("@run", source_url, project_id)

    if not source_url or not project_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid ingest payload",
        )

    bucket = settings.S3_BUCKET
    if not bucket:
        raise HTTPException(status_code=500, detail="AWS_S3_BUCKET env not set")

    job = get_current_job()
    if job:
        job.meta.update(stage="downloading")
        job.save_meta()
        print(job)

    # 1) yt 다운로드 + s3 업로드
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            local_file = await _download_youtube_video(source_url, temp_dir)
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="유튜브 영상을 다운로드할 수 없습니다.",
            ) from exc

        object_key = build_object_key(project_id, local_file)

        if job:
            job.meta.update(stage="uploading")
            job.save_meta()

        try:
            s3.upload_file(str(local_file), bucket, object_key)
        except (BotoCoreError, ClientError) as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="S3 업로드 중 오류가 발생했습니다.",
            ) from exc

    if job:
        job.meta.update(stage="finalizing")
        job.save_meta()
        print(job)

    await _finalize_ingest(project_id, object_key)

    if job:
        job.meta.update(stage="done", s3_key=object_key)
        job.save_meta()
    return object_key


def run_ingest(payload: Mapping[str, Any]) -> str:  # ← RQ가 호출하는 동기 함수
    return asyncio.run(_run_ingest_async(payload))
