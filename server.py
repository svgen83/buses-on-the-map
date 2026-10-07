import json
import logging
from dataclasses import dataclass, asdict
from functools import partial

import trio
from trio_websocket import serve_websocket, ConnectionClosed


logging.basicConfig(level=logging.DEBUG, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)


for name in logging.root.manager.loggerDict:
    if name != __name__:
        logging.getLogger(name).disabled = True
logging.root.setLevel(logging.WARNING)


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


def filter_buses_by_bounds(buses_dict: dict[str, Bus], bounds: WindowBounds | None) -> list[Bus]:
    if bounds is None:
        return list(buses_dict.values())

    filtered = []
    for bus in buses_dict.values():
        if bounds.is_inside(bus.lat, bus.lng):
            filtered.append(bus)
    return filtered


async def send_buses(ws, bounds: WindowBounds | None, buses: dict[str, Bus]):
    filtered = filter_buses_by_bounds(buses, bounds)
    if bounds is not None:
        logger.debug(f"{len(filtered)} buses inside bounds")
    else:
        logger.debug(f"Отправка всех автобусов (границы не заданы)")

    buses_list = [asdict(b) for b in filtered]
    message = {
        'msgType': 'Buses',
        'buses': buses_list,
    }
    await ws.send_message(json.dumps(message, ensure_ascii=False))


async def handle_imitation(request, buses: dict[str, Bus]):
    ws = await request.accept()
    logger.info("Имитатор подключился")
    try:
        while True:
            raw = await ws.get_message()
            try:
                data = json.loads(raw)
                bus_id = data.get('busId')
                if bus_id:
                    buses[bus_id] = Bus(
                        busId=bus_id,
                        lat=data.get('lat', 0.0),
                        lng=data.get('lng', 0.0),
                        route=data.get('route', ''),
                    )
            except json.JSONDecodeError:
                logger.warning(f"Некорректный JSON от имитатора: {raw}")
    except ConnectionClosed:
        logger.info("Имитатор отключился")
    except Exception as e:
        logger.error(f"Ошибка в handle_imitation: {e}")


async def talk_to_browser(request, buses: dict[str, Bus]):
    ws = await request.accept()
    logger.info("Браузер подключён")
    bounds: WindowBounds | None = None
    try:
        async with trio.open_nursery() as nursery:
            async def send_loop():
                while True:
                    await send_buses(ws, bounds, buses)
                    await trio.sleep(1)

            async def receive_loop():
                nonlocal bounds
                while True:
                    raw = await ws.get_message()
                    try:
                        data = json.loads(raw)
                        if data.get('msgType') == 'newBounds':
                            bdata = data.get('data', {})
                            if bounds is None:
                                bounds = WindowBounds(
                                    south_lat=bdata.get('south_lat', 0.0),
                                    north_lat=bdata.get('north_lat', 0.0),
                                    west_lng=bdata.get('west_lng', 0.0),
                                    east_lng=bdata.get('east_lng', 0.0),
                                )
                            else:
                                bounds.update(
                                    south_lat=bdata.get('south_lat', 0.0),
                                    north_lat=bdata.get('north_lat', 0.0),
                                    west_lng=bdata.get('west_lng', 0.0),
                                    east_lng=bdata.get('east_lng', 0.0),
                                )
                            logger.debug(f"{raw}")
                    except json.JSONDecodeError:
                        logger.warning(f"Некорректный JSON от браузера: {raw}")

            nursery.start_soon(send_loop)
            nursery.start_soon(receive_loop)
    except ConnectionClosed:
        logger.info("Браузер отключён")
    except Exception as e:
        logger.error(f"Ошибка в talk_to_browser: {e}")


async def main():
    buses: dict[str, Bus] = {}
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
