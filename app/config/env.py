import os
from dotenv import load_dotenv
from pydantic_settings import BaseSettings

load_dotenv()
_raw_origins = os.getenv("CORS_ORIGINS", "http://localhost:5173")
origins = [origin.strip() for origin in _raw_origins.split(",") if origin.strip()]
SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_DEFAULT_ROLE = os.getenv("GOOGLE_DEFAULT_ROLE", "user")


class Settings(BaseSettings):
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://redis:6379/0")
    S3_BUCKET: str = os.getenv("AWS_S3_BUCKET", "dupilot-dev-media")
    AWS_REGION: str = os.getenv("AWS_REGION", "ap-northeast-2")


settings = Settings()
