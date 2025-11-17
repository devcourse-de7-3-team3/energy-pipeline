# 🍱 meal_dbt 사용 가이드 (Windows + macOS)

이 폴더는 Snowflake RAW → STAGING → ANALYTICS 전처리 과정 중  
급식 데이터(지역별 테이블)를 통합하기 위한 dbt 프로젝트입니다.

팀원들은 아래 순서를 그대로 따라 하면 dbt 환경 설정이 완료됩니다.

※ 팀원들은 **dbt debug 까지만 실행**하면 됩니다.  
실제 모델 실행(`dbt run`)은 담당자가 별도로 수행합니다.

---

# 1. 가상환경 생성 및 패키지 설치

## 📌 1-1. 프로젝트 루트로 이동

clone 받은 폴더의 **최상단으로 이동**하세요.

예시 (본인 환경에 맞게 조정):

### Windows
```powershell
cd <프로젝트_클론_위치>\energy-pipeline
```

### macOS
```bash
cd <프로젝트_클론_위치>/energy-pipeline
```


---

## 📌 1-2. (최초 1회) 가상환경 생성

### Windows
```powershell
python -m venv venv
```

### macOS
```bash
python3 -m venv venv
```

---

## 📌 1-3. 가상환경 활성화

### Windows
```powershell
venv\Scripts\activate
```

### macOS
```bash
source venv/bin/activate
```

---

## 📌 1-4. requirements 설치

```bash
pip install -r requirements.txt
```

---

# 2. DBT_PROFILES_DIR 설정 (중요)

dbt는 접속 정보(profiles.yml)를 환경변수에서 찾습니다.  
이 프로젝트에서는 아래 경로를 사용합니다:

```
meal_dbt/dbt_profiles/profiles.yml
```

프로젝트 루트 기준으로 위 구조가 존재하면 OK.

## 📌 2-1. 환경변수 설정

### Windows PowerShell
```powershell
$env:DBT_PROFILES_DIR="<프로젝트_루트_경로>\meal_dbt\dbt_profiles"
```

예시:
```powershell
$env:DBT_PROFILES_DIR="$PWD\meal_dbt\dbt_profiles"
```

→ `$PWD` 사용하면 현재 디렉토리를 기준으로 자동 설정됨.

---

### macOS (bash/zsh)
```bash
export DBT_PROFILES_DIR="$PWD/meal_dbt/dbt_profiles"
```

→ 마찬가지로 `$PWD` 를 사용하면 프로젝트 루트 자동 인식됨.

---

# 3. profiles.yml 작성

## 📌 3-1. 예시 파일 복사

### Windows
```powershell
cd meal_dbt\dbt_profiles
copy profiles.example.yml profiles.yml
```

### macOS
```bash
cd meal_dbt/dbt_profiles
cp profiles.example.yml profiles.yml
```

---

## 📌 3-2. profiles.yml 수정

아래 두 항목만 본인의 Snowflake 계정 기준으로 수정:

```yaml
user: "<USER_NAME>"
password: "<PASSWORD>"
```

전체 예시:

```yaml
meal_dbt:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "jkphfwm-ib92612"
      user: "<USER_NAME>"
      password: "<PASSWORD>"
      role: "ANALYTICS_AUTHOR"
      warehouse: "COMPUTE_WH"
      database: "ENERGY_DB"
      schema: "ANALYTICS"
      threads: 4
```

---

# 4. dbt 연결 확인 (팀원은 이것만 하면 끝)

다시 meal_dbt 폴더로 이동:

```bash
cd ../   # (현재: meal_dbt/dbt_profiles 기준)
cd meal_dbt
```

또는 프로젝트 루트에서:

### Windows
```powershell
cd .\meal_dbt
```

### macOS
```bash
cd meal_dbt
```

---

### 📌 연결 테스트

Windows / macOS 공통:

```bash
dbt debug
```

정상 메시지:
- Using profiles dir at …
- profiles.yml file [OK found and valid]
- **All checks passed!**

👉 여기까지 성공하면 팀원 세팅 완료입니다.

---

# 5. 팀원이 해야 할 전체 순서 요약

```
1. git pull
2. 프로젝트 루트로 이동 (본인 클론 위치 기준)
3. python -m venv venv
4. 가상환경 활성화
5. pip install -r requirements.txt
6. DBT_PROFILES_DIR 환경변수 설정 ($PWD 사용 가능)
7. profiles.example.yml → profiles.yml 복사
8. profiles.yml 에 user/password 입력
9. cd meal_dbt
10. dbt debug  ← 팀원은 여기까지 하면 완료!
```

---

# 끝.
팀원은 debug 까지만 진행하며, 실제 모델 실행은 담당자가 진행합니다.

