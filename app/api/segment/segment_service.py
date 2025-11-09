from datetime import datetime
from fastapi import HTTPException
from bson import ObjectId
from bson.errors import InvalidId
from typing import Any, Dict, List, Optional, Tuple

from ..deps import DbDep
from .model import ResponseSegment, RequestSegment


class SegmentService:
    def __init__(self, db: DbDep):
        self.db = db
        self.collection_name = "projects"
        self.collection = db.get_collection(self.collection_name)
        self.segment_collection = db.get_collection("segments")
        self.projection = {
            "segments": 1,
            "editor_id": 1,
            "segment_assets_prefix": 1,
            "target_lang": 1,
            "source_lang": 1,
            "video_source": 1,
        }

    async def test_save_segment(self, request: RequestSegment, db_name: str):
        project_oid = ObjectId(request.project_id)
        collection = self.db.get_collection(db_name)
        doc = request.model_dump(by_alias=True)
        doc["_id"] = project_oid
        result = await collection.insert_one(doc)
        return str(result.inserted_id)

    async def delete_segments_by_project(self, project_id: ObjectId) -> int:
        result = await self.segment_collection.delete_many({"project_id": project_id})
        return result.deleted_count

    async def insert_segments_from_metadata(
        self,
        project_id: str | ObjectId,
        segments_meta: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        project_oid = self._as_object_id(str(project_id))
        now = datetime.now()
        docs: list[dict[str, Any]] = []

        for index, raw in enumerate(segments_meta or []):
            normalized = self._normalize_segment_for_store(raw or {}, index=index)
            normalized.update(
                {
                    "project_id": project_oid,
                    "segment_index": index,
                    "created_at": now,
                    "updated_at": now,
                }
            )
            docs.append(normalized)

        if docs:
            await self.segment_collection.insert_many(docs)

        return docs

    async def find_all_segment(self, project_id: Optional[str] = None):
        query: Dict[str, Any] = {}
        if project_id:
            object_id = self._as_object_id(project_id)
            query["_id"] = object_id

        project_docs = await self.collection.find(query, self.projection).to_list(
            length=None
        )

        all_segments: List[ResponseSegment] = []

        for project_doc in project_docs:
            project_id = project_doc["_id"]
            editor_id = project_doc.get("editor_id")
            segments = project_doc.get("segments") or []
            for segment_data in segments:
                segment_data = dict(segment_data)
                segment_data["_id"] = project_id
                segment_data.setdefault("editor_id", editor_id)
                all_segments.append(ResponseSegment(**segment_data))

        return all_segments

    async def update_segment(self, request: RequestSegment):
        result = await self.collection.update_one(
            {"_id": request.project_id, "segment_id": request.segment_id},
            {"$set": {"translate_context": request.translate_context}},
        )
        return result

    def _as_object_id(self, project_id: str) -> ObjectId:
        try:
            return ObjectId(project_id)
        except InvalidId as exc:
            raise HTTPException(status_code=400, detail="invalid project_id") from exc

    async def _load_project(self, project_id: str) -> Tuple[Dict[str, Any], ObjectId]:
        object_id = self._as_object_id(project_id)
        project = await self.collection.find_one({"_id": object_id}, self.projection)
        if not project:
            raise HTTPException(status_code=404, detail="project not found")
        return project, object_id

    async def get_project_segment(
        self, project_id: str, segment_id: str
    ) -> Tuple[Dict[str, Any], Dict[str, Any], int, ObjectId]:
        project, object_id = await self._load_project(project_id)

        segment = await self.segment_collection.find_one(
            {"segment_id": ObjectId(segment_id), "project_id": object_id}
        )

        return project, dict(segment), segment["segment_index"], object_id
        raise HTTPException(status_code=404, detail="segment not found")

    async def set_segment_translation(
        self,
        project_object_id: ObjectId,
        segment_index: int,
        text: str,
        *,
        editor_id: Optional[str] = None,
    ) -> None:
        set_fields: Dict[str, Any] = {
            f"segments.{segment_index}.translate_context": text,
        }
        if editor_id:
            set_fields[f"segments.{segment_index}.editor_id"] = editor_id

        await self.collection.update_one(
            {"_id": project_object_id},
            {"$set": set_fields},
        )

    def _normalize_segment_for_store(
        self,
        segment: dict[str, Any],
        *,
        index: int,
    ) -> dict[str, Any]:
        def _float_or_none(value: Any) -> float | None:
            try:
                return float(value)
            except (TypeError, ValueError):
                return None

        segment_id = segment.get("seg_id")
        try:
            segment_oid = ObjectId(segment_id)
        except (InvalidId, TypeError):
            segment_oid = ObjectId()

        issues = segment.get("issues") or []
        if not isinstance(issues, list):
            issues = [issues]

        normalized: dict[str, Any] = {
            "segment_id": segment_oid,
            "segment_text": segment.get("seg_txt", ""),
            "translate_context": segment.get("trans_txt", ""),
            "score": segment.get("score"),
            "editor_id": segment.get("editor_id"),
            "start_point": _float_or_none(segment.get("start")) or 0.0,
            "end_point": _float_or_none(segment.get("end")) or 0.0,
            "issues": issues,
            "sub_langth": _float_or_none(segment.get("sub_langth")),
            # "order": segment.get("order", index),
        }

        assets = segment.get("assets")
        if isinstance(assets, dict):
            normalized["assets"] = assets

        for key in ("source_key", "bgm_key", "tts_key", "mix_key", "video_key"):
            value = segment.get(key)
            if value:
                normalized[key] = value

        return normalized
