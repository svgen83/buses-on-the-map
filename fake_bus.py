import os
import json
import time
import trio
from trio_websocket import open_websocket_url



SEND_INTERVAL = 2

def load_routes(directory_path='routes'):
    """Генератор, загружающий все JSON-файлы из папки как маршруты."""
    for filename in os.listdir(directory_path):
        if filename.endswith('.json'):
            filepath = os.path.join(directory_path, filename)
            with open(filepath, 'r', encoding='utf8') as f:
                yield json.load(f)


async def run_bus(url, bus_id, route):
    coordinates = route['coordinates']
    route_name = route['name']
    try:
        async with open_websocket_url(url) as ws:
            print(f"[{bus_id}] Подключён к серверу")
            for lat, lng in coordinates:
                msg = {
                    'busId': bus_id,
                    'lat': lat,
                    'lng': lng,
                    'route': route_name,
                }
                await ws.send_message(json.dumps(msg, ensure_ascii=False))
                await trio.sleep(SEND_INTERVAL)
            print(f"[{bus_id}] Маршрут завершён, отключаюсь")
    except OSError as e:
        print(f"[{bus_id}] Ошибка подключения: {e}")


async def main():
    url = 'ws://127.0.0.1:8080'
    routes = list(load_routes('routes'))[:25]
    if not routes:
        print("Маршруты не найдены в папке 'routes'")
        return

    print(f"Загружено маршрутов: {len(routes)}")
    async with trio.open_nursery() as nursery:
        for route in routes:
            bus_id = f"{route['name']}-0"
            nursery.start_soon(run_bus, url, bus_id, route)

if __name__ == '__main__':
    trio.run(main)
