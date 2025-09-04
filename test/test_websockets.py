import asyncio
import websockets
import json
import uuid

async def test_binance():
    print("Testing Binance WebSocket...")
    symbols = [
        'btcusdt', 'ethusdt', 'xrpusdt', 'adausdt', 'solusdt', 
        'dogeusdt', 'avaxusdt', 'linkusdt', 'dotusdt', 'maticusdt', 
        'atomusdt', 'nearusdt', 'algousdt', 'flowusdt', 'icpusdt', 
        'sandusdt', 'manausdt', 'axsusdt', 'chzusdt', 'enjusdt'
    ]
    
    streams = '/'.join([f"{symbol}@ticker" for symbol in symbols])
    uri = f"wss://stream.binance.com:9443/ws/{streams}"
    
    try:
        async with websockets.connect(uri) as websocket:
            print(f"Binance connected - {len(symbols)} symbols")
            
            for i in range(5):
                message = await websocket.recv()
                data = json.loads(message)
                symbol = data.get('s', 'Unknown')
                price = float(data.get('c', 0))
                change = float(data.get('P', 0))
                print(f"  {symbol}: ${price:.4f} ({change:+.2f}%)")
                
    except Exception as e:
        print(f"Binance error: {e}")

async def test_upbit():
    print("\nTesting Upbit WebSocket...")
    
    codes = [
        "KRW-BTC", "KRW-ETH", "KRW-XRP", "KRW-ADA", "KRW-SOL", 
        "KRW-DOGE", "KRW-AVAX", "KRW-LINK", "KRW-DOT", "KRW-MATIC", 
        "KRW-ATOM", "KRW-NEAR", "KRW-ALGO", "KRW-FLOW", "KRW-ICP", 
        "KRW-SAND", "KRW-MANA", "KRW-AXS", "KRW-CHZ", "KRW-ENJ"
    ]
    
    uri = "wss://api.upbit.com/websocket/v1"
    
    try:
        async with websockets.connect(uri) as websocket:
            print(f"Upbit connected - {len(codes)} symbols")
            
            subscribe_msg = [
                {"ticket": str(uuid.uuid4())},
                {
                    "type": "ticker", 
                    "codes": codes,
                    "isOnlyRealtime": True
                }
            ]
            
            await websocket.send(json.dumps(subscribe_msg))
            
            for i in range(5):
                message = await websocket.recv()
                data = json.loads(message)
                symbol = data.get('code', 'Unknown')
                price = float(data.get('trade_price', 0))
                change = float(data.get('signed_change_rate', 0)) * 100
                print(f"  {symbol}: {price:,.0f}KRW ({change:+.2f}%)")
                
    except Exception as e:
        print(f"Upbit error: {e}")

async def main():
    await test_binance()
    await test_upbit()

if __name__ == "__main__":
    asyncio.run(main())