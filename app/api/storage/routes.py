# app/api/routes/upload.py
import os
from uuid import uuid4
from hashlib import sha256

from fastapi import APIRouter, HTTPException, Request, status, Depends
from fastapi.responses import RedirectResponse
from redis.exceptions import RedisError
from rq import Queue
from pymongo.errors import PyMongoError

from app.api.jobs.service import start_job
from app.api.project.service import ProjectService
from app.config.s3 import s3
from ..deps import DbDep
from bson.errors import InvalidId
from ..project.models import ProjectUpdate
from ..pipeline.service import update_pipeline_stage, get_pipeline_status
from ..pipeline.models import PipelineUpdate, PipelineStatus
from .models import PresignRequest, RegisterRequest, UploadFinalize
from app.config.redis import get_redis
from app.workers.jobs.video_ingest import run_ingest

upload_router = APIRouter(prefix="/storage", tags=["storage"])


def _make_idem_key(req: RegisterRequest, header_key: str | None) -> str:
    return (
        header_key
        or sha256(f"{req.project_id}|{str(req.youtube_url)}".encode()).hexdigest()
    )


r = get_redis()
UPLOAD_QUEUE = Queue("uploads", connection=r)
IDEMPOTENCY_HEADER_CANDIDATES = (
    "Idempotency-Key",
    "X-Idempotency-Key",
    "Dupilot-Idempotency-Key",
)


@upload_router.post(
    "/register-source",
    status_code=status.HTTP_202_ACCEPTED,
    summary="YouTube 소스 등록(큐잉)",
)
async def register_source(payload: RegisterRequest, request: Request, db: DbDep):
    # 1) 멱등키 확보
    header_key = None
    for header_name in IDEMPOTENCY_HEADER_CANDIDATES:
        value = request.headers.get(header_name)
        if value:
            header_key = value
            break
    job_id = _make_idem_key(payload, header_key)

    # 2) 기존 jobId가 있으면 그대로 반환
    try:
        existing_job = UPLOAD_QUEUE.fetch_job(job_id)
    except RedisError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="업로드 작업 상태를 확인할 수 없습니다.",
        ) from exc

    if existing_job:
        existing_job.refresh()
        return {
            "job_id": existing_job.id,
            "queue": existing_job.origin,
            "status": existing_job.get_status(),
            "stage": existing_job.meta.get("stage"),
        }

    # 3) 큐에 넣기
    job_payload = {
        "project_id": payload.project_id,
        "source_url": payload.youtube_url,
    }

    try:
        job = UPLOAD_QUEUE.enqueue(
            run_ingest,
            job_payload,
            job_id=job_id,
            description=f"YouTube ingest for project {payload.project_id}",
            meta={
                "stage": "queued",
                "project_id": payload.project_id,
                "source_url": payload.youtube_url,
            },
        )
    except RedisError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="업로드 작업을 예약하지 못했습니다.",
        ) from exc

    return {
        "job_id": job.id,
        "queue": job.origin,
        "status": job.get_status(),
        "stage": job.meta.get("stage"),
    }


@upload_router.post("/prepare-upload")
async def prepare_file_upload(payload: PresignRequest):
    bucket = os.getenv("AWS_S3_BUCKET")
    if not bucket:
        raise HTTPException(status_code=500, detail="AWS_S3_BUCKET env not set")

    # project = await create_project(db, payload)
    # project_id = project["project_id"]
    object_key = (
        f"projects/{payload.project_id}/inputs/videos/{uuid4()}_{payload.filename}"
    )
    try:
        presigned = s3.generate_presigned_post(
            Bucket=bucket,
            Key=object_key,
            Fields={"Content-Type": payload.content_type},
            Conditions=[
                ["starts-with", "$Content-Type", payload.content_type.split("/")[0]]
            ],
            ExpiresIn=300,  # 5분
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"presign 실패: {exc}")

    return {
        "project_id": payload.project_id,
        "upload_url": presigned["url"],
        "fields": presigned["fields"],
        "object_key": object_key,
    }


@upload_router.post("/finish-upload", status_code=status.HTTP_202_ACCEPTED)
async def finish_upload(
    db: DbDep,
    payload: UploadFinalize,
    project_service: ProjectService = Depends(ProjectService),
):
    update_payload = ProjectUpdate(
        project_id=payload.project_id,
        status="upload_done",
        video_source=payload.object_key,
    )
    try:
        get_pipeline_status(db, update_payload.project_id)
        result = await project_service.update_project(update_payload)
    except InvalidId as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid project_id",
        ) from exc
    except PyMongoError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update project",
        ) from exc

    await update_pipeline_stage(
        db,
        PipelineUpdate(
            project_id=payload.project_id,
            stage_id="upload",
            status=PipelineStatus.COMPLETED,
            progress=100,
        ),
    )

    await start_job(result, db)
    await update_pipeline_stage(
        db,
        PipelineUpdate(
            project_id=payload.project_id,
            stage_id="upload",
            status=PipelineStatus.COMPLETED,
            progress=100,
        ),
    )

    return result


@upload_router.get("/media/{key:path}")
def media_redirect(key: str):
    bucket = os.getenv("AWS_S3_BUCKET")
    if not bucket:
        raise HTTPException(status_code=500, detail="AWS_S3_BUCKET env not set")

    # 키 검증

    url = s3.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=3600
    )

    resp = RedirectResponse(url, status_code=302)
    resp.headers["Cache-Control"] = "private, max-age=300"
    return resp
