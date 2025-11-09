from pydantic import BaseModel, HttpUrl


class PresignRequest(BaseModel):
    project_id: str
    filename: str
    content_type: str
    # owner_code: str


class RegisterRequest(BaseModel):
    project_id: str
    youtube_url: str


class UploadFinalize(BaseModel):
    project_id: str
    object_key: str


class UploadFail(BaseModel):
    project_id: str
