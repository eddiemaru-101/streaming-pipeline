import asyncio
import websockets
import json
import uuid

async def test_upbit_websocket():
    uri = "wss://api.upbit.com/websocket/v1"
    
    async with websockets.connect(uri) as websocket:
        print("🔗 업비트 WebSocket 연결 성공!")
        
        # 구독 메시지 (함수 안으로 이동!)
        subscribe_msg = [
            {"ticket": str(uuid.uuid4())},
            {
                "type": "ticker", 
                "codes": ["KRW-BTC", "KRW-ETH"],
                "isOnlyRealtime": True
            }
        ]
        

        # 구독 요청 전송 (함수 안으로 이동!)
        await websocket.send(json.dumps(subscribe_msg))  # dumps로 수정!
        print("📡 구독 요청 전송 완료")



        print("📊 데이터 수신 시작...")
        for i in range(5):
            try:
                message = await websocket.recv()
                data = json.loads(message)
                #print(data)

                market = data.get('code', 'Unkown')  #  'code' 키의 값을 가져오고, 없으면 'Unknown' 반환
                price = data.get('trade_price',0)  #업비트에서 현재가를 나타내는 필드명, 없으면 0 반환
                change_rate = data.get('signed_change_rate', 0) * 100 #전일 대비 변화율을 퍼센트로 변환
                print(f"{i+1}. {market}: {price:,}원 ({change_rate:+.2f}%)")
            except Exception as e:
                pring(f'❌ 에러: {e}')
                break
        print("✅ 테스트 완료!")
           

# 실행
asyncio.run(test_upbit_websocket())