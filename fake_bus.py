import trio
from trio_websocket import open_websocket_url
import json
import time


async def send_bus_positions():
    # Загружаем маршрут
    with open('156.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
    route_name = data['name']
    coordinates = data['coordinates']
    bus_id = f"{route_name}-0"  # например "156-0"
    
    try:
        async with open_websocket_url('ws://127.0.0.1:8080') as ws:
            print("Подключено к серверу")
            for lat, lng in coordinates:
                msg = {
                    "busId": bus_id,
                    "lat": lat,
                    "lng": lng,
                    "route": route_name
                }
                await ws.send_message(json.dumps(msg))
                print(f"Отправлено: {msg}")
                await trio.sleep(1)  # задержка 1 секунды между точками
    except OSError as ose:
        print(f"Ошибка подключения: {ose}")


async def main():
    await send_bus_positions()


if __name__ == "__main__":
    trio.run(main)
