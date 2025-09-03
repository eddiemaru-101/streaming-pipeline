import asyncio
import websockets
import json
import uuid
from kafka import KafkaProducer
from datetime import datetime
import logging
import time
from collections import deque
import os

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class UpbitKafkaProducer:
    def __init__(self, kafka_servers=['localhost:9093'], topic='upbit-prices'):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3,
            retry_backoff_ms=300
        )
        # TPS 측정용 변수
        self.message_timestamps = deque()
        self.total_messages = 0
        self.last_tps_log = time.time()
        
        # 로그 파일 설정
        os.makedirs('../data', exist_ok=True)
        self.log_file = '../data/upbit_tps.log'
        
        logger.info(f"Kafka Producer 초기화 완료 - Topic: {topic}")
    
    async def connect_and_stream(self):
        uri = "wss://api.upbit.com/websocket/v1"
        
        while True:  # 재연결 로직
            try:
                async with websockets.connect(uri) as websocket:
                    logger.info("Upbit WebSocket Connected!")
                    
                    # 구독 메시지
                    subscribe_msg = [
                        {"ticket": str(uuid.uuid4())},
                        {
                            "type": "ticker", 
                            "codes": [
                                "KRW-BTC", "KRW-ETH", "KRW-XRP", "KRW-ADA", "KRW-SOL", 
                                "KRW-DOGE", "KRW-AVAX", "KRW-LINK", "KRW-DOT", "KRW-MATIC", 
                                "KRW-ATOM", "KRW-NEAR", "KRW-ALGO", "KRW-FLOW", "KRW-ICP", 
                                "KRW-SAND", "KRW-MANA", "KRW-AXS", "KRW-CHZ", "KRW-ENJ"
                            ],
                            "isOnlyRealtime": True
                        }
                    ]
                    
                    # 구독 요청 전송
                    await websocket.send(json.dumps(subscribe_msg))
                    logger.info("Subscription request sent")
                    
                    # 데이터 수신 및 Kafka 전송
                    while True:
                        message = await websocket.recv()
                        data = json.loads(message)
                        
                        # 데이터 가공 (UI 친화적으로 간소화)
                        processed_data = {
                            'exchange': 'upbit',
                            'symbol': data.get('code', 'Unknown'),
                            'price': round(float(data.get('trade_price', 0)), 2),
                            'volume_24h': round(float(data.get('acc_trade_volume_24h', 0)), 2),
                            'change_rate': round(float(data.get('signed_change_rate', 0)) * 100, 2),
                            'timestamp': datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
                            'high_price': round(float(data.get('high_price', 0)), 2),
                            'low_price': round(float(data.get('low_price', 0)), 2)
                        }
                        
                        # Kafka로 전송
                        self.producer.send(self.topic, processed_data)
                        
                        # TPS 측정
                        current_time = time.time()
                        self.message_timestamps.append(current_time)
                        self.total_messages += 1
                        
                        # 60초 이전 데이터 제거
                        while self.message_timestamps and current_time - self.message_timestamps[0] > 60:
                            self.message_timestamps.popleft()
                        
                        # 30초마다 TPS 로그
                        if current_time - self.last_tps_log > 30:
                            self.log_tps(current_time)
                            self.last_tps_log = current_time
                        
                        logger.info(f"{processed_data['symbol']}: {processed_data['price']:,}KRW ({processed_data['change_rate']:+.2f}%)")
                        
            except websockets.exceptions.ConnectionClosed:
                logger.warning("WebSocket 연결 끊김, 5초 후 재연결...")
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"❌ 에러 발생: {e}, 10초 후 재시도...")
                await asyncio.sleep(10)
    
    def log_tps(self, current_time):
        """TPS 계산 및 파일 로그"""
        current_tps = len(self.message_timestamps)
        avg_tps = current_tps / 60.0 if current_tps > 0 else 0.0
        
        log_message = f"{datetime.now().isoformat()},UPBIT,{current_tps},{avg_tps:.2f},{self.total_messages}\n"
        
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(log_message)
        
        logger.info(f"📊 TPS: {avg_tps:.2f}/s (최근 60초: {current_tps}개, 총: {self.total_messages}개)")
    
    def close(self):
        # 종료시 최종 TPS 로그
        self.log_tps(time.time())
        self.producer.close()
        logger.info("Kafka Producer 종료")

async def main():
    producer = UpbitKafkaProducer()
    
    try:
        await producer.connect_and_stream()
    except KeyboardInterrupt:
        logger.info("프로그램 종료 중...")
    finally:
        producer.close()

if __name__ == "__main__":
    asyncio.run(main())