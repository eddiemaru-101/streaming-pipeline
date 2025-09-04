@echo off
chcp 65001 >nul
echo ====================================================
echo 암호화폐 프리미엄 계산 파이프라인 자동 시작
echo ====================================================
echo.

echo [1/6] 환경 검증 중...
echo ----------------------------------------

REM Docker Desktop 실행 확인
docker --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Docker가 설치되지 않았거나 실행되지 않았습니다.
    echo    Docker Desktop을 설치하고 실행한 후 다시 시도하세요.
    pause
    exit /b 1
)
echo [OK] Docker 확인됨

REM Maven 확인
mvn --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Maven이 설치되지 않았습니다.
    echo    Maven을 설치한 후 다시 시도하세요.
    pause
    exit /b 1
)
echo [OK] Maven 확인됨

REM Python 확인
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python이 설치되지 않았습니다.
    pause
    exit /b 1
)
echo [OK] Python 확인됨

echo.
echo [2/6] JAR 파일 빌드 중...
echo ----------------------------------------

cd flink-jobs
echo Maven 빌드 실행...
mvn clean package -q
if %errorlevel% neq 0 (
    echo [ERROR] Maven 빌드 실패
    pause
    exit /b 1
)
echo [OK] JAR 파일 빌드 완료

cd ..

echo.
echo [3/6] Docker 컨테이너 시작 중...
echo ----------------------------------------

echo Docker Compose 실행...
docker-compose up -d
if %errorlevel% neq 0 (
    echo [ERROR] Docker Compose 실행 실패
    pause
    exit /b 1
)

echo [OK] Docker 컨테이너 시작됨
echo 서비스 초기화 대기 중 (30초)...
timeout /t 30 /nobreak >nul

echo.
echo [4/6] Flink Job 배포 중...
echo ----------------------------------------

echo JAR 파일 복사...
docker cp flink-jobs/target/premium-calculator-1.0.0.jar crypto-flink-jobmanager:/opt/flink/
if %errorlevel% neq 0 (
    echo [ERROR] JAR 파일 복사 실패
    pause
    exit /b 1
)

echo Flink Job 제출...
docker exec crypto-flink-jobmanager /opt/flink/bin/flink run /opt/flink/premium-calculator-1.0.0.jar
if %errorlevel% neq 0 (
    echo [ERROR] Flink Job 제출 실패
    pause
    exit /b 1
)
echo [OK] Flink Job 배포 완료

echo.
echo [5/6] Python Producer 시작 중...
echo ----------------------------------------

echo 가상환경 활성화...
call venv\Scripts\activate

echo Producer 백그라운드 실행...
start /b python producers/upbit_producer.py
start /b python producers/binance_producer.py

echo [OK] Producer 시작됨
echo 데이터 수집 대기 중 (10초)...
timeout /t 10 /nobreak >nul

echo.
echo [6/6] 시스템 상태 확인 중...
echo ----------------------------------------

echo 서비스 상태 검증...
call check-health.bat

echo.
echo ====================================================
echo 파이프라인 시작 완료!
echo ====================================================
echo.
echo 모니터링 UI 접속 주소:
echo   • Kafka UI:        http://localhost:8080
echo   • Flink Dashboard: http://localhost:8082
echo   • Redis Commander: http://localhost:8083
echo.
echo 종료하려면: stop-pipeline.bat 실행
echo ====================================================
pause