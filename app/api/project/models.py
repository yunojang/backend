from datetime import datetime
from typing import Any, Dict, List, Optional, Annotated
from bson import ObjectId
from pydantic import BaseModel, BeforeValidator, Field

PyObjectId = Annotated[
    str, BeforeValidator(lambda v: str(v) if isinstance(v, ObjectId) else v)
]


class ProjectPublic(BaseModel):
    project_id: str
    title: str
    progress: int
    status: str
    video_source: str | None
    created_at: datetime
    updated_at: datetime
    segment_assets_prefix: Optional[str] = None
    segments: Optional[List[Dict[str, Any]]] = None
    owner_code: str


class ProjectCreate(BaseModel):
    title: str
    filename: str | None = None
    owner_code: str
    # sourceType: 'youtube' | 'file'
    # youtubeUrl: str
    # fileName: str | None
    # fileSize: int | None
    sourceLanguage: str
    targetLanguages: List[str]
    # detectAutomatically: bool
    speakerCount: int


class ProjectCreateResponse(BaseModel):
    project_id: str


class ProjectUpdate(BaseModel):
    project_id: str
    status: str
    video_source: str | None = None
    segment_assets_prefix: Optional[str] = None
    segments: Optional[List[Dict[str, Any]]] = None
    owner_code: str | None = None


class ProjectOut(BaseModel):
    id: PyObjectId = Field(validation_alias="_id")
    title: str
    progress: int
    status: str
    video_source: str | None
    created_at: datetime
    updated_at: datetime
    segment_assets_prefix: Optional[str] = None
    # segments: Optional[List[Dict[str, Any]]] = None
    # owner_code: str
    issue_count: int = 0  # 새로 집계한 값을 넣기 위한 필드
