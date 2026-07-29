import trio
from trio_websocket import serve_websocket, ConnectionClosed
import json


def load_way(path_name):
    with open(path_name, 'r', encoding='utf-8') as file:
        route_data = json.load(file)
        name = route_data['name']
        coordinates = route_data['coordinates']
        return name, coordinates


def update_bus(bus, coordinates, speed):
    if bus['point_index'] >= len(coordinates) - 1:
        bus['lat'] = coordinates[-1][0]
        bus['lng'] = coordinates[-1][1]
        return

    bus['t'] += speed
    if bus['t'] >= 1.0:
        bus['point_index'] += 1
        bus['t'] = 0.0
        if bus['point_index'] >= len(coordinates) - 1:
            bus['lat'] = coordinates[-1][0]
            bus['lng'] = coordinates[-1][1]
            return

    i = bus['point_index']
    lat1, lng1 = coordinates[i]
    lat2, lng2 = coordinates[i + 1]
    bus['lat'] = lat1 + (lat2 - lat1) * bus['t']
    bus['lng'] = lng1 + (lng2 - lng1) * bus['t']


def make_buses_message(bus):
    return {
        'msgType': 'Buses',
        'buses': [
            {
                'busId': bus['busId'],
                'lat': bus['lat'],
                'lng': bus['lng'],
                'route': bus['route'],
            }
        ]
    }


async def broadcast_buses(active_connections,
                          bus, coordinates,
                          speed):
    while True:
        update_bus(bus, coordinates, speed)
        message = json.dumps(make_buses_message(bus))
        for ws in list(active_connections):
            try:
                await ws.send_message(message)
            except ConnectionClosed:
                pass
        await trio.sleep(1)


async def handle_client(request, active_connections, bus, coordinates, speed):
    ws = await request.accept()
    print("Клиент подключён")
    active_connections.add(ws)

    try:
        await ws.send_message(json.dumps(make_buses_message(bus)))
        while True:
            try:
                await ws.get_message()
            except ConnectionClosed:
                break
    finally:
        active_connections.discard(ws)
        print("Клиент отключён")

async def main():
    # Загружаем имя маршрута и координаты из файла
    route_name, coordinates = load_way('156.json')
    SPEED = 1.0

    # Создаём автобус с данными из файла
    bus = {
        'busId': route_name,       # например, "156"
        'route': route_name,       # например, "156"
        'lat': coordinates[0][0],
        'lng': coordinates[0][1],
        'point_index': 0,
        't': 0.0,
    }
    active_connections = set()

    async def ws_handler(request):
        await handle_client(request, active_connections, bus, coordinates, SPEED)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(broadcast_buses, active_connections, bus, coordinates, SPEED)
        await serve_websocket(ws_handler, '127.0.0.1', 8000, ssl_context=None)

if __name__ == "__main__":
    trio.run(main)
