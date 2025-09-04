@echo off
chcp 65001 >nul
echo ====================================================
echo 암호화폐 프리미엄 계산 파이프라인 종료
echo ====================================================
echo.

echo [1/3] Python Producer 종료 중...
echo ----------------------------------------

REM Python 프로세스 확인 및 종료
for /f "tokens=2" %%i in ('tasklist /fi "imagename eq python.exe" /fo csv ^| findstr /v "PID"') do (
    if not "%%i"=="" (
        echo Python 프로세스 %%i 종료 중...
        taskkill /pid %%i /f >nul 2>&1
    )
)

echo [OK] Python Producer 종료됨

echo.
echo [2/3] Docker 컨테이너 종료 중...
echo ----------------------------------------

echo Docker Compose 종료...
docker-compose down

if %errorlevel% equ 0 (
    echo [OK] 모든 컨테이너 종료됨
) else (
    echo [WARNING] 일부 컨테이너 종료에 문제가 있었습니다.
)

echo.
echo [3/3] 환경 정리 중...
echo ----------------------------------------

REM 가상환경 비활성화 (만약 활성화되어 있다면)
if defined VIRTUAL_ENV (
    echo 가상환경 비활성화...
    call deactivate 2>nul
)

echo 임시 파일 정리...
del /q *.log 2>nul
del /q data\*.tmp 2>nul

echo [OK] 환경 정리 완료

echo.
echo ====================================================
echo 파이프라인 완전 종료 완료!
echo ====================================================
echo.
echo 다시 시작하려면: start-pipeline.bat 실행
echo 상태 확인하려면: check-health.bat 실행
echo ====================================================
pause