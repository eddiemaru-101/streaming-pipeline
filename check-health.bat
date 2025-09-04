@echo off
chcp 65001 >nul
echo 🔍 시스템 건강 상태 검사 중...
echo.

set "healthy=0"

REM Docker 컨테이너 상태 확인
echo [1/5] Docker 컨테이너 상태...
for /f "tokens=2" %%i in ('docker ps --filter "name=crypto-" --format "table {{.Status}}" ^| findstr /c:"Up"') do (
    set /a healthy+=1
)

docker ps --filter "name=crypto-" --format "table {{.Names}}\t{{.Status}}"
echo ✅ 실행 중인 컨테이너: %healthy%개

if %healthy% lss 6 (
    echo ⚠️  일부 컨테이너가 실행되지 않았습니다.
)

echo.

REM Kafka 토픽 확인
echo [2/5] Kafka 토픽 상태...
docker exec crypto-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>nul
if %errorlevel% equ 0 (
    echo ✅ Kafka 정상 작동
) else (
    echo ❌ Kafka 연결 실패
)

echo.

REM Flink Job 상태 확인
echo [3/5] Flink Job 상태...
docker exec crypto-flink-jobmanager /opt/flink/bin/flink list 2>nul | findstr /c:"RUNNING"
if %errorlevel% equ 0 (
    echo ✅ Flink Job 실행 중
) else (
    echo ❌ Flink Job 실행되지 않음
)

echo.

REM Redis 연결 확인
echo [4/5] Redis 상태...
docker exec crypto-redis redis-cli ping 2>nul | findstr /c:"PONG"
if %errorlevel% equ 0 (
    echo ✅ Redis 정상 작동
) else (
    echo ❌ Redis 연결 실패
)

echo.

REM 데이터 처리 확인
echo [5/5] 실시간 데이터 처리 상태...
echo ⏳ 5초간 TaskManager 로그 확인...
timeout /t 2 /nobreak >nul

docker logs crypto-flink-taskmanager --tail 3 2>nul | findstr /c:"premium"
if %errorlevel% equ 0 (
    echo ✅ 실시간 프리미엄 계산 진행 중
) else (
    echo ⚠️  아직 데이터 처리가 시작되지 않았거나 문제가 있습니다.
    echo    몇 분 더 기다려보세요.
)

echo.

REM 최종 Redis 데이터 샘플 확인
echo 📊 Redis 저장 데이터 샘플:
docker exec crypto-redis redis-cli --raw hgetall premiums 2>nul | head -4
if %errorlevel% neq 0 (
    echo    (아직 데이터가 저장되지 않았습니다)
)

echo.
echo ====================================================
echo 🏥 건강 검사 완료
echo ====================================================