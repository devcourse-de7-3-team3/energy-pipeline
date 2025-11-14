# 프로젝트 Setup Guide

## 2. 프로젝트 구조

```
energy-pipeline/
├── airflow/
│   ├── dags/              # DAG 파일들
│   ├── plugins/           # 커스텀 플러그인
│   ├── logs/              # Airflow 로그 (자동 생성)
│   └── Dockerfile         # Airflow 이미지 빌드 파일
├── dbt/                   # dbt 프로젝트
│   └── energy_model/      # Energy 모델
├── docker-compose.yml     # Docker Compose 설정
├── .env                   # 환경 변수 (생성 필요)
├── .env.example           # 환경 변수 예시
└── README.md              # 이 문서
```
---
## 설정과정
### 1) 깃 클론

```bash
git clone git@github.com:devcourse-de7-3-team3/energy-pipeline.git
cd energy-pipeline
```

### 2) env 파일 복사하여 본인 정보 기입

```bash
# 환경변수 설정 예시파일을 .env로 생성
cp .env.example .env

# vi 혹은 IDE로 .env 파일 수정
# SNOWFLAKE_USER, SNOWFLAKE_PASSWORD 항목 각 본인 계정정보 수정
vi .env
```

### 3) airflow 실행

```bash
# 도커파일 빌드 및 실행
docker compose up -d

# 실행되기 확인
docker ps
```

### 4) 웹서버 접속
http://localhost:8081

### 5) 스노우플레이크 연결정보 Airflow Connection 자동 등록 DAG 실행

#### 5-1) 웹에서 `init_snowflake_connection` DAG 수동 실행
#### 5-2) Admin > Connection 에서 .env 파일 내 기재한 정보에 맞게 연결정보 등록됨 확인
#### 5-4) `test_snowflake_connection` DAG 실행하여 snowflake 정상 연결 확인
---
## 서비스 관리

### 서비스 시작

```bash
docker-compose up -d
```

### 서비스 중지

```bash
docker-compose down
```

### 서비스 재시작

```bash
# 전체 재시작
docker-compose restart

# 특정 서비스만 재시작
docker-compose restart scheduler
docker-compose restart webserver
```

### 컨테이너 상태 확인

```bash
# 실행 중인 컨테이너 확인
docker ps

# 모든 컨테이너 확인 (중지된 것 포함)
docker ps -a

# 상세 정보 확인
docker-compose ps
```

### 리소스 정리

```bash
# 완전 정리: 컨테이너, 네트워크, 볼륨, 이미지, 고아 컨테이너 모두 삭제
docker compose down --volumes --rmi all --remove-orphans
```
---

## 문제 해결

### 초기화 실패 시

컨테이너 초기화가 실패하거나 문제가 발생한 경우:

```bash
# 모든 컨테이너와 볼륨 삭제 후 재시작
docker-compose down -v
docker-compose up -d
```

### 로그 확인

```bash
# 전체 로그 확인 (실시간)
docker-compose logs -f

# 특정 서비스 로그만 확인
docker-compose logs -f scheduler
docker-compose logs -f webserver
docker-compose logs -f postgres
docker-compose logs -f triggerer
```

### 권한 문제 (Linux/Mac)

파일 권한 오류가 발생하는 경우:

```bash
# UID가 올바르게 설정되었는지 확인
cat .env | grep AIRFLOW_UID

# UID가 없거나 잘못된 경우 재설정
echo "AIRFLOW_UID=$(id -u)" >> .env

# 컨테이너 재시작
docker-compose down
docker-compose up -d
```

**일반적인 권한 오류 메시지:**
```
PermissionError: [Errno 13] Permission denied: '/opt/airflow/logs/...'
```

### 포트 충돌

8081 포트가 이미 사용 중인 경우:

```bash
# docker-compose.yml에서 포트 변경
# webserver:
#   ports:
#     - "8082:8080"  # 8081 대신 8082 사용

docker-compose up -d
```

### 컨테이너가 계속 재시작되는 경우

```bash
# 문제가 있는 컨테이너 확인
docker ps -a

# 해당 컨테이너 로그 확인
docker logs energy-airflow-scheduler

# 가장 흔한 원인: DB 연결 실패
# 1. postgres 컨테이너가 정상 실행 중인지 확인
docker ps | grep postgres

# 2. .env 파일의 DB 정보가 올바른지 확인
cat .env | grep POSTGRES
```

### 웹서버 접속이 안 되는 경우

```bash
# 1. 웹서버 컨테이너 상태 확인
docker ps | grep webserver

# 2. 헬스체크 상태 확인
docker inspect energy-airflow-webserver | grep -A 5 Health

# 3. 웹서버 로그 확인
docker logs energy-airflow-webserver

# 4. 포트가 올바르게 매핑되었는지 확인
docker ps | grep 8081
```
---
## 도커 컴포즈 실행 순서
```bash
┌────────────────────────────┐
│        docker compose up   │
└───────────────┬────────────┘
                ▼
      (1) Network 생성
                │
                ▼
      (2) Volume 생성
                │
                ▼
      (3) Postgres 시작
                │
                │ healthcheck: pg_isready
                ▼
  ┌──────────────────────────────┐
  │ Postgres: healthy 상태 도달    │
  └──────────────────────────────┘
                │
                ▼
      (4) airflow-init 시작
                │
                │ airflow db init
                │ airflow users create
                │ Admin 계정 생성
                ▼
  ┌──────────────────────────────┐
  │ airflow-init 종료 (success)   │
  └──────────────────────────────┘
                │
                ▼
      (5) Scheduler 시작
                │
                │ airflow scheduler
                │ DAG 파싱 / 스케줄러 루프 시작
                ▼
  ┌──────────────────────────────┐
  │ Scheduler healthcheck OK     │
  └──────────────────────────────┘
                │
                ▼
      (6) Webserver 시작
                │
                │ airflow webserver
                │ UI 제공 (포트 8081:8080)
                ▼
  ┌──────────────────────────────┐
  │ Webserver healthcheck OK     │
  └──────────────────────────────┘
                │
                ▼
      (7) Triggerer 시작
                │
                │ airflow triggerer
                │ Deferrable Operator 백그라운드 관리
                ▼
  ┌──────────────────────────────┐
  │ Triggerer healthcheck OK     │
  └──────────────────────────────┘
                │
                ▼
      (8) Airflow 풀 운영 시작
                │
                ├── Scheduler: DAG 스케줄링
                ├── Webserver: UI 제공
                ├── Triggerer: Deferrable Task 처리
                └── Postgres: 메타데이터 저장
```


