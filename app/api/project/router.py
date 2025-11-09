from bson import ObjectId
from bson.errors import InvalidId
from fastapi import APIRouter, HTTPException, status, Depends, Query
from typing import List, Any, Optional
from pymongo.errors import PyMongoError
from app.api.deps import DbDep
from .models import ProjectOut
from .service import ProjectService
from ..segment.segment_service import SegmentService
from .models import ProjectCreate, ProjectCreateResponse, ProjectOut

# from app.api.auth.service import get_current_user_from_cookie


def _serialize(value: Any) -> Any:
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, list):
        return [_serialize(item) for item in value]
    if isinstance(value, dict):
        return {key: _serialize(item) for key, item in value.items()}
    return value


project_router = APIRouter(prefix="/projects", tags=["Projects"])


@project_router.post(
    "/",
    response_model=ProjectCreateResponse,
    status_code=status.HTTP_201_CREATED,
    summary="프로젝트 생성",
)
async def create_project_endpoint(
    payload: ProjectCreate,
    project_service: ProjectService = Depends(ProjectService),
) -> ProjectCreateResponse:
    result = await project_service.create_project(payload)
    return ProjectCreateResponse.model_validate(result)


@project_router.get(
    "/me",
    response_model=List[ProjectOut],
    summary="현재 사용자 프로젝트 목록",
)
async def list_my_projects(
    # current_user: UserOut = Depends(get_current_user_from_cookie),
    sort: Optional[str] = Query(default="created_at", description="정렬 필드"),
    page: int = Query(1, ge=1),
    limit: int = Query(6, ge=1, le=100),
    project_service: ProjectService = Depends(ProjectService),
) -> List[ProjectOut]:
    try:
        return await project_service.get_project_paging(
            sort=sort, page=page, limit=limit, user_id="owner-1234"
        )
    except InvalidId as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid project_id",
        ) from exc
    except PyMongoError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve projects",
        ) from exc


@project_router.get("/", summary="프로젝트 전체 목록")
async def list_projects(db: DbDep):
    # await db["projects"].delete_many({})
    docs = await db["projects"].find().sort("created_at", -1).to_list(length=None)
    return {"items": [ProjectOut.model_validate(doc) for doc in docs]}
    # return {ProjectOut.model_validate(doc) for doc in docs]


# import time, logging

# logger = logging.getLogger(__name__)


# @project_router.get("/health", summary="프로젝트 헬스체크")
# async def health_check():
#     return {"status": "ok"}


@project_router.get("/{project_id}", summary="프로젝트 상세 조회")
async def get_project(project_id: str, db: DbDep):
    try:
        project_oid = ObjectId(project_id)
    except InvalidId as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid project_id",
        ) from exc

    project = await db["projects"].find_one({"_id": project_oid})
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Project not found",
        )

    segments = (
        await db["segments"]
        .find({"project_id": project_oid})
        .sort("segment_index", 1)
        .to_list(length=None)
    )
    segment_ids = [seg["_id"] for seg in segments]

    issues = (
        await db["issues"]
        .find({"segment_id": {"$in": segment_ids}})
        .to_list(length=None)
    )

    issues_by_segment: dict[ObjectId, list[dict[str, Any]]] = {}
    for issue in issues:
        issues_by_segment.setdefault(issue["segment_id"], []).append(issue)

    for segment in segments:
        seg_id = segment["_id"]
        segment["issues"] = issues_by_segment.get(seg_id, [])
    project["segments"] = segments
    serialized = _serialize(project)
    return serialized


@project_router.delete("/{project_id}", response_model=int, summary="프로젝트 삭제")
async def delete_project(
    project_id: str,
    project_service: ProjectService = Depends(ProjectService),
    segment_service: SegmentService = Depends(SegmentService),
) -> None:
    try:
        project_oid = ObjectId(project_id)
    except InvalidId as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid project_id",
        ) from exc

    result = await project_service.delete_project(project_oid)
    if result == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Project not found",
        )

    result = await segment_service.delete_segments_by_project(project_oid)
    return result
