import json
import logging
import trio
from trio_websocket import serve_websocket, ConnectionClosed
from functools import partial


logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s:%(name)s:%(message)s'
)
logger = logging.getLogger(__name__)


logging.getLogger('trio_websocket').setLevel(logging.WARNING)


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
    try:
        async with trio.open_nursery() as nursery:
            async def send_loop():
                while True:
                    buses_list = list(buses.values())
                    message = {
                        'msgType': 'Buses',
                        'buses': buses_list,
                    }
                    await ws.send_message(json.dumps(message, ensure_ascii=False))
                    await trio.sleep(1)

            async def receive_loop():
                while True:
                    msg = await ws.get_message()
                    logger.debug(f"Получено от браузера: {msg}")

            nursery.start_soon(send_loop)
            nursery.start_soon(receive_loop)
    except ConnectionClosed:
        logger.info("Браузер отключён")
    except Exception as e:
        logger.error(f"Ошибка в talk_to_browser: {e}")


async def main():
    buses = {}  # общее состояние автобусов
    async with trio.open_nursery() as nursery:
        # Сервер для имитаторов (порт 8080)
        nursery.start_soon(
            serve_websocket,
            partial(handle_imitation, buses=buses),
            '127.0.0.1',
            8080,
            None
        )
        # Сервер для браузеров (порт 8000)
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
