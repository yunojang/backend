from fastapi import HTTPException, status
from datetime import datetime
from typing import Optional, List
from bson import ObjectId
from ..deps import DbDep
from ..project.models import (
    ProjectCreate,
    ProjectUpdate,
    ProjectPublic,
    ProjectOut,
)
from ..pipeline.service import _create_default_pipeline


class ProjectService:
    def __init__(self, db: DbDep):
        self.db = db
        self.project_collection = db.get_collection("projects")
        self.segment_collection = db.get_collection("segments")

    async def get_project_by_id(self, project_id: str) -> ProjectPublic:
        doc = await self.project_collection.find_one({"_id": ObjectId(project_id)})
        doc["project_id"] = str(doc["_id"])
        return ProjectPublic.model_validate(doc)

    async def get_project_paging(
        self,
        user_id: Optional[str] = None,
        sort: str = "created_at",
        page: int = 1,
        limit: int = 6,
    ) -> List[ProjectOut]:
        skip = (page - 1) * limit
        docs = (
            await self.project_collection.find({"owner_code": user_id})
            .sort([(sort, -1)])
            .skip(skip)
            .limit(limit)
            .to_list(length=limit)
        )

        project_ids = [doc["_id"] for doc in docs]

        pipeline = [
            {"$match": {"project_id": {"$in": project_ids}}},
            {
                "$lookup": {
                    "from": "issues",
                    "let": {"segmentId": "$_id"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$segment_id", "$$segmentId"]}}},
                        {"$count": "count"},
                    ],
                    "as": "issue_docs",
                }
            },
            {
                "$addFields": {
                    "issue_count": {"$ifNull": [{"$first": "$issue_docs.count"}, 0]}
                }
            },
            {
                "$group": {
                    "_id": "$project_id",
                    "issue_count": {"$sum": "$issue_count"},
                }
            },
        ]
        issue_counts = await self.segment_collection.aggregate(pipeline).to_list(
            length=None
        )
        issue_map = {row["_id"]: row["issue_count"] for row in issue_counts}

        result = []
        for doc in docs:
            doc["issue_count"] = issue_map.get(doc["_id"], 0)
            result.append(ProjectOut.model_validate(doc))
        return result

    async def delete_project(self, project_id: str) -> int:
        result = await self.project_collection.delete_one({"_id": project_id})
        return result.deleted_count

    async def create_project(self, payload: ProjectCreate) -> str:
        now = datetime.now()
        payload_data = payload.model_dump(exclude_none=True)
        doc = {
            **payload_data,
            "progress": 0,
            "status": "upload_ready",
            "video_source": None,
            "created_at": now,
            "updated_at": now,
            "owner_code": payload.owner_code,
        }

        result = await self.project_collection.insert_one(doc)

        project_id = str(result.inserted_id)
        # 프로젝트 생성 시 파이프 라인도 생성
        await _create_default_pipeline(db=self.db, project_id=project_id)

        return {"project_id": project_id}

    async def update_project(self, payload: ProjectUpdate) -> ProjectPublic:
        project_id = payload.project_id
        update_data = payload.model_dump(exclude={"project_id"}, exclude_none=True)
        update_data["updated_at"] = datetime.now()

        result = await self.project_collection.update_one(
            {"_id": ObjectId(project_id)},
            {"$set": update_data},
        )

        doc = await self.project_collection.find_one({"_id": ObjectId(project_id)})
        if not doc:
            raise HTTPException(status_code=404, detail="Project not found")

        doc["project_id"] = str(doc["_id"])

        if result.matched_count == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Project not found",
            )

        return ProjectPublic.model_validate(doc)
