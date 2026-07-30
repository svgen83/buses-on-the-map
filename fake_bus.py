import os
import json
import random
import argparse
import logging
import trio
from itertools import cycle, islice
from trio_websocket import open_websocket_url


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', default='ws://127.0.0.1:8080')
    parser.add_argument('--routes_number', type=int, default=None)
    parser.add_argument('--buses_per_route', type=int, default=3)
    parser.add_argument('--websockets_number', type=int, default=10)
    parser.add_argument('--emulator_id', default='')
    parser.add_argument('--refresh_timeout', type=float, default=1.0)
    parser.add_argument('-v', '--verbose', action='count', default=0)
    return parser.parse_args()


def setup_logging(verbosity):
    levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    level = levels[min(verbosity, len(levels)-1)]
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s')


def load_routes(directory_path='routes'):
    for filename in os.listdir(directory_path):
        if filename.endswith('.json'):
            filepath = os.path.join(directory_path, filename)
            with open(filepath, 'r', encoding='utf8') as f:
                yield json.load(f)


def generate_bus_id(emulator_id, route_name, index):
    if emulator_id:
        return f"{emulator_id}-{route_name}-{index}"
    return f"{route_name}-{index}"


async def run_bus(send_channel,
                  route, bus_id,
                  start_index, interval):
    coords = route['coordinates']
    route_name = route['name']
    cycled = cycle(coords)
    shifted = islice(cycled, start_index, None)
    for lat, lng in shifted:
        msg = {'busId': bus_id,
               'lat': lat,
               'lng': lng,
               'route': route_name}
        await send_channel.send(msg)
        await trio.sleep(interval)


async def send_updates(url, receive_channel):
    try:
        async with open_websocket_url(url) as ws:
            while True:
                msg = await receive_channel.receive()
                await ws.send_message(
                    json.dumps(msg, ensure_ascii=False))
    except OSError as e:
        logging.error(f"Ошибка подключения: {e}")


async def main():
    args = parse_args()
    setup_logging(args.verbose)

    all_routes = list(load_routes('routes'))
    if args.routes_number is not None:
        routes = all_routes[:args.routes_number]
    else:
        routes = all_routes

    if not routes:
        logging.error("Маршруты не найдены в папке 'routes'")
        return

    logging.info(f"Загружено маршрутов: {len(routes)}")

    send_channels = []
    receive_channels = []
    for _ in range(args.websockets_number):
        send, receive = trio.open_memory_channel(0)
        send_channels.append(send)
        receive_channels.append(receive)

    async with trio.open_nursery() as nursery:
        for recv in receive_channels:
            nursery.start_soon(
                send_updates,
                args.server, recv)

        bus_counter = 0
        for route in routes:
            coords_len = len(route['coordinates'])
            route_name = route['name']
            for i in range(args.buses_per_route):
                bus_id = generate_bus_id(
                    args.emulator_id,
                    route_name, i)
                start_index = random.randint(
                    0, coords_len - 1)
                send_channel = random.choice(
                    send_channels)
                nursery.start_soon(
                    run_bus, send_channel,
                    route, bus_id, start_index,
                    args.refresh_timeout)
                bus_counter += 1

        logging.info(
            f"Запущено {bus_counter} автобусов на {args.websockets_number} сокетах")

        try:
            await trio.sleep_forever()
        except KeyboardInterrupt:
            logging.info("Остановка по Ctrl+C")
            nursery.cancel_scope.cancel()


if __name__ == '__main__':
    trio.run(main)
