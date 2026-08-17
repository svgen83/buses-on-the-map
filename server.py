import json
import logging
import trio
from trio_websocket import serve_websocket, ConnectionClosed
from functools import partial


logging.basicConfig(level=logging.DEBUG, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

for name in logging.root.manager.loggerDict:
    if name != __name__:
        logging.getLogger(name).disabled = True

logging.root.setLevel(logging.DEBUG)


def is_inside(bounds, lat, lng):
    if not bounds:
        return False
    return (bounds['south_lat'] <= lat <= bounds['north_lat'] and
            bounds['west_lng'] <= lng <= bounds['east_lng'])


def filter_buses_by_bounds(buses, bounds):
    if not bounds:
        return list(buses.values())
    
    filtered = []
    for bus in buses.values():
        if is_inside(bounds, bus.get('lat'), bus.get('lng')):
            filtered.append(bus)
    return filtered


async def send_buses(ws, bounds, buses):
    filtered_buses = filter_buses_by_bounds(buses, bounds)
    if bounds:
        logger.debug(f"{len(filtered_buses)} buses inside bounds")
    message = {
        'msgType': 'Buses',
        'buses': filtered_buses,
    }
    await ws.send_message(json.dumps(message, ensure_ascii=False))


async def handle_imitation(request, buses):
    ws = await request.accept()
    logger.info("Имитатор подключился")
    try:
        while True:
            message = await ws.get_message()
            try:
                data = json.loads(message)
                bus_id = data.get('busId')
                if bus_id:
                    buses[bus_id] = {
                        'busId': bus_id,
                        'lat': data.get('lat'),
                        'lng': data.get('lng'),
                        'route': data.get('route', ''),
                    }
            except json.JSONDecodeError:
                logger.warning(f"Некорректный JSON от имитатора: {message}")
    except ConnectionClosed:
        logger.info("Имитатор отключился")
    except Exception as e:
        logger.error(f"Ошибка в handle_imitation: {e}")


async def talk_to_browser(request, buses):
    ws = await request.accept()
    logger.info("Браузер подключён")
    bounds = None
    
    try:
        await send_buses(ws, bounds, buses)
        
        async with trio.open_nursery() as nursery:
            async def receive_loop():
                nonlocal bounds
                while True:
                    msg = await ws.get_message()
                    try:
                        data = json.loads(msg)
                        if data.get('msgType') == 'newBounds':
                            bounds_data = data.get('data', {})
                            bounds = {
                                'south_lat': bounds_data.get('south_lat'),
                                'north_lat': bounds_data.get('north_lat'),
                                'west_lng': bounds_data.get('west_lng'),
                                'east_lng': bounds_data.get('east_lng'),
                            }
                            logger.debug(f"{msg}")
                            await send_buses(ws, bounds, buses)
                    except json.JSONDecodeError:
                        logger.warning(f"Некорректный JSON от браузера: {msg}")

            nursery.start_soon(receive_loop)
    except ConnectionClosed:
        logger.info("Браузер отключён")
    except Exception as e:
        logger.error(f"Ошибка в talk_to_browser: {e}")

async def main():
    buses = {}
    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            serve_websocket,
            partial(handle_imitation, buses=buses),
            '127.0.0.1',
            8080,
            None
        )
        nursery.start_soon(
            serve_websocket,
            partial(talk_to_browser, buses=buses),
            '127.0.0.1',
            8000,
            None
        )
        await trio.sleep_forever()

if __name__ == '__main__':
    trio.run(main)
