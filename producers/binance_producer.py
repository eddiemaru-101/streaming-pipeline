import asyncio
import websockets
import json
from kafka import KafkaProducer
from datetime import datetime
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BinanceKafkaProducer:
    def __init__(self, kafka_servers=['localhost:9093'], topic='binance-prices'):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3,
            retry_backoff_ms=300
        )
        # 바이낸스 여러 심볼 스트림
        self.symbols = ['btcusdt', 'ethusdt', 'xrpusdt', 'adausdt']
        logger.info(f"Kafka Producer 초기화 완료 - Topic: {topic}")
    
    async def connect_and_stream(self):
        # 여러 심볼을 한번에 구독하는 URI
        streams = '/'.join([f"{symbol}@ticker" for symbol in self.symbols])
        uri = f"wss://stream.binance.com:9443/ws/{streams}"
        
        while True:  # 재연결 로직
            try:
                async with websockets.connect(uri) as websocket:
                    logger.info("🔗 바이낸스 WebSocket 연결 성공!")
                    logger.info(f"📡 구독 심볼: {', '.join(self.symbols)}")
                    
                    # 데이터 수신 및 Kafka 전송
                    while True:
                        message = await websocket.recv()
                        data = json.loads(message)
                        
                        # 데이터 가공
                        processed_data = {
                            'exchange': 'binance',
                            'symbol': data.get('s', 'Unknown'),  # BTCUSDT
                            'price': float(data.get('c', 0)),    # 현재가
                            'volume': float(data.get('v', 0)),   # 24h 볼륨
                            'change_rate': float(data.get('P', 0)),  # 변화율
                            'timestamp': datetime.now().isoformat(),
                            'raw_data': data
                        }
                        
                        # USD를 KRW로 대략 변환 (실시간 환율 적용하면 더 정확)
                        usd_to_krw = 1320  # 임시 환율
                        processed_data['price_krw'] = processed_data['price'] * usd_to_krw
                        
                        # Kafka로 전송
                        self.producer.send(self.topic, processed_data)
                        
                        logger.info(f"📊 {processed_data['symbol']}: ${processed_data['price']} (≈{processed_data['price_krw']:,.0f}원) ({processed_data['change_rate']:+.2f}%)")
                        
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
    producer = BinanceKafkaProducer()
    
    try:
        await producer.connect_and_stream()
    except KeyboardInterrupt:
        logger.info("프로그램 종료 중...")
    finally:
        producer.close()

if __name__ == "__main__":
    asyncio.run(main())