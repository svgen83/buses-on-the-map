import argparse
import contextlib
import json
import logging
from functools import partial
from typing import Literal

import trio
from pydantic import BaseModel, ValidationError, field_validator
from trio_websocket import ConnectionClosed, serve_websocket


def parse_args():
    parser = argparse.ArgumentParser(
        description="Сервер отслеживания автобусов"
    )
    parser.add_argument(
        '--bus-port',
        type=int,
        default=8080,
        help='Порт для подключения имитаторов (по умолчанию 8080)',
    )
    parser.add_argument(
        '--browser-port',
        type=int,
        default=8000,
        help='Порт для подключения браузеров (по умолчанию 8000)',
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Включить отладочный вывод (DEBUG)',
    )
    return parser.parse_args()


def setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(levelname)s:%(name)s:%(message)s',
    )
    logger = logging.getLogger(__name__)

    for name in logging.root.manager.loggerDict:
        if name != __name__:
            logging.getLogger(name).disabled = True
    logging.root.setLevel(logging.WARNING)
    return logger


class Bus(BaseModel):
    busId: str
    lat: float
    lng: float
    route: str = ''

    @field_validator('busId', mode='before')
    @classmethod
    def validate_bus_id(cls, value):
        if value is None or value == '':
            raise ValueError("Requires busId specified")
        if not isinstance(value, str):
            raise ValueError("busId must be a string")
        return value

    @field_validator('lat', 'lng', mode='before')
    @classmethod
    def validate_number(cls, value, info):
        if value is None:
            raise ValueError(
                f"Requires {info.field_name} specified"
            )
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(
                f"{info.field_name} must be a number"
            )
        return float(value)

    @field_validator('route', mode='before')
    @classmethod
    def validate_route(cls, value):
        if value is None:
            return ''
        if not isinstance(value, str):
            raise ValueError("route must be a string")
        return value


class WindowBounds(BaseModel):
    south_lat: float
    north_lat: float
    west_lng: float
    east_lng: float

    @field_validator(
        'south_lat', 'north_lat', 'west_lng', 'east_lng',
        mode='before',
    )
    @classmethod
    def validate_coord(cls, value, info):
        if value is None:
            raise ValueError(
                f"Missing '{info.field_name}' in data"
            )
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(
                f"'{info.field_name}' must be a number"
            )
        return float(value)

    def is_inside(self, lat: float, lng: float):
        return (
            self.south_lat <= lat <= self.north_lat
            and self.west_lng <= lng <= self.east_lng
        )

    def update(
        self,
        south_lat: float,
        north_lat: float,
        west_lng: float,
        east_lng: float
    ):
        self.south_lat = south_lat
        self.north_lat = north_lat
        self.west_lng = west_lng
        self.east_lng = east_lng


class NewBoundsMessage(BaseModel):
    msgType: Literal['newBounds']
    data: WindowBounds


def format_validation_errors(exc: ValidationError):
    messages = []

    for err in exc.errors():
        loc = err['loc']
        field = loc[-1] if loc else ''
        err_type = err['type']
        message = err['msg']

        if message.startswith('Value error, '):
            message = message[len('Value error, '):]

        if err_type == 'missing':
            if field == 'busId':
                message = "Requires busId specified"
            elif field in ('lat', 'lng'):
                message = f"Requires {field} specified"
            elif field in (
                'south_lat', 'north_lat', 'west_lng', 'east_lng',
            ):
                message = f"Missing '{field}' in data"
            elif field == 'data':
                message = "Requires 'data' object for newBounds"
        elif err_type == 'model_type' and field == 'data':
            message = "Requires 'data' object for newBounds"

        messages.append(message)

    return messages


async def send_error(ws, errors):
    msg = {"msgType": "Errors", "errors": errors}
    await ws.send_message(json.dumps(msg, ensure_ascii=False))
    await ws.aclose()


def filter_buses_by_bounds(
    buses_dict: dict[str, Bus],
    bounds: WindowBounds | None
):
    if bounds is None:
        return list(buses_dict.values())
    return [
        bus for bus in buses_dict.values()
        if bounds.is_inside(bus.lat, bus.lng)
    ]


async def send_buses(
    ws, bounds: WindowBounds | None,
    buses: dict[str, Bus],
    logger
):
    filtered = filter_buses_by_bounds(buses, bounds)
    if bounds is not None:
        logger.debug(f"{len(filtered)} buses inside bounds")
    else:
        logger.debug("Отправка всех автобусов (границы не заданы)")

    message = {
        'msgType': 'Buses',
        'buses': [bus.model_dump() for bus in filtered],
    }
    await ws.send_message(json.dumps(message, ensure_ascii=False))


async def handle_imitation(request, buses: dict[str, Bus], logger):
    ws = await request.accept()
    logger.info("Имитатор подключился")

    try:
        while True:
            raw = await ws.get_message()

            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning(
                    f"Некорректный JSON от имитатора: {raw!r}"
                )
                await send_error(ws, ["Requires valid JSON"])
                return

            if not isinstance(data, dict):
                logger.warning(
                    "Ожидался JSON-объект от имитатора, "
                    f"получено: {data!r}"
                )
                await send_error(
                    ws, ["Message must be a JSON object"]
                )
                return

            try:
                bus = Bus.model_validate(data)
            except ValidationError as exc:
                errors = format_validation_errors(exc)
                logger.warning(
                    f"Ошибки валидации от имитатора: {errors}. "
                    f"Данные: {data}"
                )
                await send_error(ws, errors)
                return

            buses[bus.busId] = bus
            logger.debug(f"Обновлён автобус {bus.busId}: {data}")

    except ConnectionClosed:
        logger.info("Имитатор отключился")
    except Exception as exc:
        logger.error(f"Ошибка в handle_imitation: {exc}")


async def talk_to_browser(request, buses: dict[str, Bus], logger):
    ws = await request.accept()
    logger.info("Браузер подключён")
    bounds: WindowBounds | None = None

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
                        logger.warning(
                            f"Некорректный JSON от браузера: {raw!r}"
                        )
                        await send_error(ws, ["Requires valid JSON"])
                        return

                    if not isinstance(data, dict):
                        logger.warning(
                            "Ожидался JSON-объект от браузера: "
                            f"{data!r}"
                        )
                        await send_error(
                            ws, ["Message must be a JSON object"]
                        )
                        return

                    msg_type = data.get('msgType')
                    if not msg_type:
                        await send_error(
                            ws, ["Requires msgType specified"]
                        )
                        return

                    if msg_type != 'newBounds':
                        await send_error(
                            ws, [f"Unknown msgType: {msg_type}"]
                        )
                        return

                    try:
                        message = NewBoundsMessage.model_validate(data)
                    except ValidationError as exc:
                        errors = format_validation_errors(exc)
                        logger.warning(
                            "Ошибки валидации от браузера: "
                            f"{errors}. Данные: {data}"
                        )
                        await send_error(ws, errors)
                        return

                    bounds = message.data
                    logger.debug(f"{raw}")

            nursery.start_soon(send_loop)
            nursery.start_soon(receive_loop)

    except ConnectionClosed:
        logger.info("Браузер отключён")
    except Exception as exc:
        logger.error(f"Ошибка в talk_to_browser: {exc}")


async def main(bus_port: int, browser_port: int, verbose: bool):
    logger = setup_logging(verbose)
    buses: dict[str, Bus] = {}

    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            serve_websocket,
            partial(handle_imitation, buses=buses, logger=logger),
            '127.0.0.1',
            bus_port,
            None,
        )
        nursery.start_soon(
            serve_websocket,
            partial(talk_to_browser, buses=buses, logger=logger),
            '127.0.0.1',
            browser_port,
            None,
        )

        logger.info(
            f"Сервер запущен: имитаторы на порту {bus_port}, "
            f"браузеры на порту {browser_port}"
        )
        await trio.sleep_forever()


if __name__ == '__main__':
    args = parse_args()
    with contextlib.suppress(KeyboardInterrupt):
        trio.run(main, args.bus_port, args.browser_port, args.verbose)
