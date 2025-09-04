# 자동화 스크립트 사용 가이드

## 스크립트 파일 목록

| 파일명 | 설명 | 사용 시점 |
|--------|------|-----------|
| `start-pipeline.bat` | 전체 파이프라인 자동 시작 | 처음 시작할 때 |
| `stop-pipeline.bat` | 전체 파이프라인 완전 종료 | 작업 완료 후 |
| `check-health.bat` | 상세 건강 상태 검사 | 문제 발생 시 |
| `quick-status.bat` | 빠른 상태 확인 | 현재 상태 궁금할 때 |

## 사용법

### 1. 첫 시작 (완전 자동화)
```cmd
start-pipeline.bat
```

**수행 작업:**
- 환경 검증 (Docker, Maven, Python)
- JAR 파일 자동 빌드
- Docker 컨테이너 시작
- Flink Job 자동 배포
- Python Producer 백그라운드 실행
- 시스템 상태 자동 검증

### 2. 상태 확인
```cmd
# 빠른 확인
quick-status.bat

# 상세 건강 검사
check-health.bat
```

### 3. 완전 종료
```cmd
stop-pipeline.bat
```

## 문제 해결

### 일반적인 문제들

1. **"Docker가 실행되지 않았습니다"**
   - Docker Desktop 실행 후 다시 시도

2. **"Maven 빌드 실패"**
   - Java 11+ 설치 확인
   - `flink-jobs/pom.xml` 파일 존재 확인

3. **"일부 컨테이너가 실행되지 않았습니다"**
   - `docker-compose down` 후 다시 실행
   - 포트 충돌 확인 (8080, 8082, 8083, 9092, 6379)

4. **"아직 데이터 처리가 시작되지 않았습니다"**
   - 2-3분 더 기다린 후 `check-health.bat` 재실행

### 수동 디버깅 명령어

```cmd
# 컨테이너 로그 확인
docker logs crypto-flink-taskmanager
docker logs crypto-kafka

# Flink Job 상태 확인
docker exec crypto-flink-jobmanager /opt/flink/bin/flink list

# Redis 데이터 확인
docker exec -it crypto-redis redis-cli
> keys *
> hgetall premiums
```

## 모니터링 UI

스크립트 실행 후 브라우저에서 접속:

- **Kafka UI**: http://localhost:8080 (토픽, 메시지 모니터링)
- **Flink Dashboard**: http://localhost:8082 (Job 상태, 처리량)
- **Redis Commander**: http://localhost:8083 (저장된 데이터 확인)

## 사용 팁

1. **최초 실행**: `start-pipeline.bat` 실행 후 5-10분 정도 기다리세요
2. **상태 확인**: `quick-status.bat`로 수시 확인 가능
3. **문제 발생**: `check-health.bat`로 상세 진단
4. **완전 종료**: 작업 완료 시 반드시 `stop-pipeline.bat` 실행

## 성공 기준

다음 조건들이 모두 만족되면 성공:

- 6개 Docker 컨테이너 모두 실행 중
- Kafka 토픽 생성됨 (upbit-prices, binance-prices)
- Flink Job 상태가 RUNNING
- Redis에 premium 데이터 저장됨
- TaskManager 로그에서 실시간 계산 확인

**모든 조건이 만족되면 실시간 김치프리미엄 계산 파이프라인이 성공적으로 작동합니다.**