import json
import logging
import argparse
import contextlib
from dataclasses import dataclass, asdict
from functools import partial

import trio
from trio_websocket import serve_websocket, ConnectionClosed


def setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format='%(levelname)s:%(name)s:%(message)s')
    logger = logging.getLogger(__name__)
    # Отключаем спам от библиотек
    for name in logging.root.manager.loggerDict:
        if name != __name__:
            logging.getLogger(name).disabled = True
    logging.root.setLevel(logging.WARNING)
    return logger


@dataclass
class Bus:
    busId: str
    lat: float
    lng: float
    route: str = ''


@dataclass
class WindowBounds:
    south_lat: float
    north_lat: float
    west_lng: float
    east_lng: float

    def is_inside(self, lat: float, lng: float) -> bool:
        return (self.south_lat <= lat <= self.north_lat and
                self.west_lng <= lng <= self.east_lng)

    def update(self, south_lat: float, north_lat: float, west_lng: float, east_lng: float) -> None:
        self.south_lat = south_lat
        self.north_lat = north_lat
        self.west_lng = west_lng
        self.east_lng = east_lng


def validate_bus_message(data):
    errors = []
    if not isinstance(data, dict):
        errors.append("Message must be a JSON object")
        return errors

    bus_id = data.get('busId')
    if not bus_id:
        errors.append("Requires busId specified")
    elif not isinstance(bus_id, str):
        errors.append("busId must be a string")

    lat = data.get('lat')
    if lat is None:
        errors.append("Requires lat specified")
    elif not isinstance(lat, (int, float)) or isinstance(lat, bool):
        errors.append("lat must be a number")

    lng = data.get('lng')
    if lng is None:
        errors.append("Requires lng specified")
    elif not isinstance(lng, (int, float)) or isinstance(lng, bool):
        errors.append("lng must be a number")

    route = data.get('route')
    if route is not None and not isinstance(route, str):
        errors.append("route must be a string")

    return errors


def validate_browser_message(data):
    errors = []
    if not isinstance(data, dict):
        errors.append("Message must be a JSON object")
        return errors

    msg_type = data.get('msgType')
    if not msg_type:
        errors.append("Requires msgType specified")
        return errors

    if msg_type == 'newBounds':
        bdata = data.get('data')
        if not isinstance(bdata, dict):
            errors.append("Requires 'data' object for newBounds")
            return errors
        required = ['south_lat', 'north_lat', 'west_lng', 'east_lng']
        for key in required:
            val = bdata.get(key)
            if val is None:
                errors.append(f"Missing '{key}' in data")
            elif not isinstance(val, (int, float)) or isinstance(val, bool):
                errors.append(f"'{key}' must be a number")
    else:
        errors.append(f"Unknown msgType: {msg_type}")

    return errors


async def send_error(ws, errors):
    msg = {"msgType": "Errors", "errors": errors}
    await ws.send_message(json.dumps(msg, ensure_ascii=False))
    await ws.aclose()


def filter_buses_by_bounds(buses_dict: dict, bounds) -> list:
    if bounds is None:
        return list(buses_dict.values())
    return [b for b in buses_dict.values() if bounds.is_inside(b.lat, b.lng)]


async def send_buses(ws, bounds, buses: dict, logger):
    filtered = filter_buses_by_bounds(buses, bounds)
    if bounds is not None:
        logger.debug(f"{len(filtered)} buses inside bounds")
    else:
        logger.debug("Отправка всех автобусов (границы не заданы)")
    message = {
        'msgType': 'Buses',
        'buses': [asdict(b) for b in filtered],
    }
    await ws.send_message(json.dumps(message, ensure_ascii=False))


async def handle_imitation(request, buses: dict, logger):
    ws = await request.accept()
    logger.info("Имитатор подключился")
    try:
        while True:
            raw = await ws.get_message()

            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning(f"Некорректный JSON от имитатора: {raw!r}")
                await send_error(ws, ["Requires valid JSON"])
                return

            errors = validate_bus_message(data)
            if errors:
                logger.warning(f"Ошибки валидации от имитатора: {errors}. Данные: {data}")
                await send_error(ws, errors)
                return

            bus_id = data['busId']
            buses[bus_id] = Bus(
                busId=bus_id,
                lat=float(data['lat']),
                lng=float(data['lng']),
                route=data.get('route', ''),
            )
            logger.debug(f"Обновлён автобус {bus_id}: {data}")
    except ConnectionClosed:
        logger.info("Имитатор отключился")
    except Exception as e:
        logger.error(f"Ошибка в handle_imitation: {e}")


async def talk_to_browser(request, buses: dict, logger):
    ws = await request.accept()
    logger.info("Браузер подключён")
    bounds = None

    try:
        async with trio.open_nursery() as nursery:

            async def send_loop():
                while True:
                    await send_buses(ws, bounds, buses, logger)
                    await trio.sleep(1)

            async def receive_loop():
                nonlocal bounds
                while True:
                    raw = await ws.get_message()

                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        logger.warning(f"Некорректный JSON от браузера: {raw!r}")
                        await send_error(ws, ["Requires valid JSON"])
                        return

                    errors = validate_browser_message(data)
                    if errors:
                        logger.warning(f"Ошибки валидации от браузера: {errors}. Данные: {data}")
                        await send_error(ws, errors)
                        return

                    if data['msgType'] == 'newBounds':
                        bdata = data['data']
                        if bounds is None:
                            bounds = WindowBounds(
                                south_lat=bdata['south_lat'],
                                north_lat=bdata['north_lat'],
                                west_lng=bdata['west_lng'],
                                east_lng=bdata['east_lng'],
                            )
                        else:
                            bounds.update(
                                south_lat=bdata['south_lat'],
                                north_lat=bdata['north_lat'],
                                west_lng=bdata['west_lng'],
                                east_lng=bdata['east_lng'],
                            )
                        logger.debug(f"{raw}")

            nursery.start_soon(send_loop)
            nursery.start_soon(receive_loop)
    except ConnectionClosed:
        logger.info("Браузер отключён")
    except Exception as e:
        logger.error(f"Ошибка в talk_to_browser: {e}")


async def main(bus_port: int, browser_port: int, verbose: bool):
    logger = setup_logging(verbose)
    buses: dict = {}
    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            serve_websocket,
            partial(handle_imitation, buses=buses, logger=logger),
            '127.0.0.1',
            bus_port,
            None
        )
        nursery.start_soon(
            serve_websocket,
            partial(talk_to_browser, buses=buses, logger=logger),
            '127.0.0.1',
            browser_port,
            None
        )
        logger.info(f"Сервер запущен: имитаторы на порту {bus_port}, браузеры на порту {browser_port}")
        await trio.sleep_forever()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Сервер отслеживания автобусов")
    parser.add_argument('--bus-port', type=int, default=8080,
                        help='Порт для подключения имитаторов автобусов (по умолчанию 8080)')
    parser.add_argument('--browser-port', type=int, default=8000,
                        help='Порт для подключения браузеров (по умолчанию 8000)')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Включить отладочный вывод (DEBUG)')
    args = parser.parse_args()

    with contextlib.suppress(KeyboardInterrupt):
        trio.run(main, args.bus_port, args.browser_port, args.verbose)
