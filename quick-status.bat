@echo off
chcp 65001 >nul
echo 파이프라인 현재 상태 (빠른 확인)
echo ========================================

echo Docker 컨테이너:
docker ps --filter "name=crypto-" --format "  {{.Names}} - {{.Status}}" 2>nul
if %errorlevel% neq 0 echo   (Docker가 실행되지 않음)

echo.
echo Python 프로세스:
tasklist /fi "imagename eq python.exe" /fo table 2>nul | findstr python
if %errorlevel% neq 0 echo   (실행 중인 Python 프로세스 없음)

echo.
echo 최신 Redis 데이터:
docker exec crypto-redis redis-cli --raw hlen premiums 2>nul && echo 개 코인의 프리미엄 데이터 저장됨
if %errorlevel% neq 0 echo   (Redis 데이터 없음 또는 연결 실패)

echo.
echo 접속 URL:
echo   Kafka UI:        http://localhost:8080
echo   Flink Dashboard: http://localhost:8082  
echo   Redis Commander: http://localhost:8083
echo ========================================