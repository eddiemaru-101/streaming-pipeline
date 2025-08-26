import asyncio
import websockets
import json
import uuid
from kafka import KafkaProducer
from datetime import datetime
import logging

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
        logger.info(f"Kafka Producer 초기화 완료 - Topic: {topic}")
    
    async def connect_and_stream(self):
        uri = "wss://api.upbit.com/websocket/v1"
        
        while True:  # 재연결 로직
            try:
                async with websockets.connect(uri) as websocket:
                    logger.info("🔗 업비트 WebSocket 연결 성공!")
                    
                    # 구독 메시지
                    subscribe_msg = [
                        {"ticket": str(uuid.uuid4())},
                        {
                            "type": "ticker", 
                            "codes": ["KRW-BTC", "KRW-ETH", "KRW-XRP", "KRW-ADA"],
                            "isOnlyRealtime": True
                        }
                    ]
                    
                    # 구독 요청 전송
                    await websocket.send(json.dumps(subscribe_msg))
                    logger.info("📡 구독 요청 전송 완료")
                    
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
                        
                        logger.info(f"📊 {processed_data['symbol']}: {processed_data['price']:,}원 ({processed_data['change_rate']:+.2f}%)")
                        
            except websockets.exceptions.ConnectionClosed:
                logger.warning("WebSocket 연결 끊김, 5초 후 재연결...")
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"❌ 에러 발생: {e}, 10초 후 재시도...")
                await asyncio.sleep(10)
    
    def close(self):
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