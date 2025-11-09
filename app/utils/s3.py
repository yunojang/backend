from pathlib import Path
from uuid import uuid4


def build_object_key(project_id: str, file_path: Path) -> str:
    extension = file_path.suffix or ".mp4"
    filename = f"{uuid4()}{extension}"
    return f"projects/{project_id}/inputs/videos/{filename}"
