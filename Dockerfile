# backend/Dockerfile
FROM python:3.11-slim AS base

#  로그가 버퍼링 없이 바로 출력
ENV PYTHONUNBUFFERED=1

# 1) OS 패키지 설치 (필요한 경우만 추가)
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates curl tzdata git procps iproute2 net-tools lsof dnsutils vim ffmpeg \
 && update-ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# 2) 작업 디렉터리
WORKDIR /workspaces/backend

# 3) 의존성(캐시 최적화) : requirements 먼저 복사
COPY requirements.txt .

# 4) 가상환경 없이 전역 설치, 캐시 제거
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# 5) 애플리케이션 소스 복사
COPY . .

# 6) uvicorn 실행 (컨테이너 포트는 8000 가정)
# ENV APP_ENV=dev
# EXPOSE 8000
# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]


