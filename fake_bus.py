import os
import json
import random
import trio
from itertools import cycle, islice
from trio_websocket import open_websocket_url


NUM_BUSES = 20000
NUM_WORKERS = 10
SEND_INTERVAL = 1
MAX_ROUTES = None
URL = 'ws://127.0.0.1:8080'

def load_routes(directory_path='routes'):
    for filename in os.listdir(directory_path):
        if filename.endswith('.json'):
            filepath = os.path.join(directory_path, filename)
            with open(filepath, 'r', encoding='utf8') as f:
                yield json.load(f)


def generate_bus_id(route_name, index):
    return f"{route_name}-{index}"


async def run_bus(send_channel, route, bus_id, start_index, interval):
    coords = route['coordinates']
    route_name = route['name']
    cycled = cycle(coords)
    shifted = islice(cycled, start_index, None)  # начинаем со случайной точки
    for lat, lng in shifted:
        msg = {
            'busId': bus_id,
            'lat': lat,
            'lng': lng,
            'route': route_name,
        }
        await send_channel.send(msg)
        await trio.sleep(interval)


async def send_updates(url, receive_channel):
    try:
        async with open_websocket_url(url) as ws:
            while True:
                msg = await receive_channel.receive()
                await ws.send_message(json.dumps(msg, ensure_ascii=False))
    except OSError as e:
        print(f"Воркер: ошибка подключения: {e}")


async def main():
    routes = list(load_routes('routes'))
    if MAX_ROUTES is not None:
        routes = routes[:MAX_ROUTES]
    if not routes:
        print("Маршруты не найдены в папке 'routes'")
        return
    print(f"Загружено маршрутов: {len(routes)}")

    send_channels = []
    receive_channels = []
    for _ in range(NUM_WORKERS):
        send, receive = trio.open_memory_channel(0)
        send_channels.append(send)
        receive_channels.append(receive)

    async with trio.open_nursery() as nursery:
        for recv in receive_channels:
            nursery.start_soon(send_updates, URL, recv)

        buses_per_route = NUM_BUSES // len(routes) + 1
        bus_counter = 0
        for route in routes:
            coords_len = len(route['coordinates'])
            route_name = route['name']
            for i in range(buses_per_route):
                if bus_counter >= NUM_BUSES:
                    break
                bus_id = generate_bus_id(route_name, i)
                start_index = random.randint(0, coords_len - 1)
                send_channel = random.choice(send_channels)
                nursery.start_soon(
                    run_bus,
                    send_channel,
                    route,
                    bus_id,
                    start_index,
                    SEND_INTERVAL
                )
                bus_counter += 1
            if bus_counter >= NUM_BUSES:
                break

        print(f"Запущено {bus_counter} автобусов на {NUM_WORKERS} сокетах")
        await trio.sleep_forever()

if __name__ == '__main__':
    trio.run(main)
