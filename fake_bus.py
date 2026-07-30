import os
import json
import random
import trio
from itertools import cycle, islice
from trio_websocket import open_websocket_url


SEND_INTERVAL = 2
BUSES_PER_ROUTE = 10
MAX_ROUTES = 50


def load_routes(directory_path='routes'):
    for filename in os.listdir(directory_path):
        if filename.endswith('.json'):
            filepath = os.path.join(directory_path, filename)
            with open(filepath, 'r', encoding='utf8') as f:
                yield json.load(f)


def generate_bus_id(route_id, bus_index):
    return f"{route_id}-{bus_index}"


async def run_bus(url, bus_id, route, start_index=0):
    coordinates = route['coordinates']
    route_name = route['name']
    cycled_coords = cycle(coordinates)
    shifted_coords = islice(cycled_coords, start_index, None)
    try:
        async with open_websocket_url(url) as ws:
            print(f"[{bus_id}] Подключён к серверу, старт с точки {start_index}")
            for lat, lng in shifted_coords:
                msg = {
                    'busId': bus_id,
                    'lat': lat,
                    'lng': lng,
                    'route': route_name,
                }
                await ws.send_message(json.dumps(msg, ensure_ascii=False))
                await trio.sleep(SEND_INTERVAL)
    except OSError as e:
        print(f"[{bus_id}] Ошибка подключения: {e}")


async def main():
    url = 'ws://127.0.0.1:8080'
    routes = list(load_routes('routes'))[:MAX_ROUTES]
    if not routes:
        print("Маршруты не найдены в папке 'routes'")
        return

    print(f"Загружено маршрутов: {len(routes)}")
    async with trio.open_nursery() as nursery:
        for route in routes:
            route_id = route['name']
            coords_len = len(route['coordinates'])
            for bus_index in range(BUSES_PER_ROUTE):
                bus_id = generate_bus_id(route_id, bus_index)
                start_index = random.randint(0, coords_len - 1)
                nursery.start_soon(run_bus, url, bus_id, route, start_index)


if __name__ == '__main__':
    trio.run(main)
