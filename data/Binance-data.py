import asyncio
import websockets
import json

async def binance_websocket():
    uri = "wss://stream.binance.com:9443/ws/btcusdt@ticker"
    
    async with websockets.connect(uri) as websocket:
        print("🔗 바이낸스 WebSocket 연결!")
        
        for i in range(5):
            message = await websocket.recv()
            data = json.loads(message)
            
            symbol = data['s']  # BTCUSDT
            price = data['c']   # 현재가
            change = data['P']  # 변화율
            
            print(f"{symbol}: ${price} ({change}%)")
        print(data)
asyncio.run(binance_websocket())
