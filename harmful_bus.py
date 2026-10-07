import json
import logging
import trio
from trio_websocket import open_websocket_url


logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s:%(name)s:%(message)s'
)
logger = logging.getLogger(__name__)

for name in logging.root.manager.loggerDict:
    if name != __name__:
        logging.getLogger(name).disabled = True
logging.root.setLevel(logging.WARNING)


async def test_case(url, message, label):
    logger.info(f"--- {label} ---")
    logger.info(f"Отправлено: {message}")
    try:
        async with open_websocket_url(url) as ws:
            await ws.send_message(message)
            response = await ws.get_message()
            logger.info(f"Response: {response}")
    except OSError as e:
        logger.error(f"Ошибка подключения: {e}")
    except Exception as e:
        logger.error(f"Ошибка: {e}")


async def harmful_bus():
    url = 'ws://127.0.0.1:8080'

    tests = [
        ("this is not json", "Невалидный JSON"),
        (json.dumps({"lat": 55.7, "lng": 37.6}), "Нет busId"),
        (json.dumps({"busId": "abc", "lng": 37.6}), "Нет lat"),
        (json.dumps({"busId": "abc", "lat": 55.7}), "Нет lng"),
        (
            json.dumps({"busId": "abc", "lat": "55.7", "lng": 37.6}),
            "lat — строка",
        ),
        (
            json.dumps({"busId": "abc", "lat": 55.7, "lng": "37.6"}),
            "lng — строка",
        ),
        (
            json.dumps({"busId": 123, "lat": 55.7, "lng": 37.6}),
            "busId — число",
        ),
        (
            json.dumps({
                "busId": "abc",
                "lat": 55.7,
                "lng": 37.6,
                "route": 123,
            }),
            "route — число",
        ),
        (json.dumps([1, 2, 3]), "JSON — массив"),
        (
            json.dumps({
                "busId": "abc-0",
                "lat": 55.7,
                "lng": 37.6,
                "route": "156",
            }),
            "Корректное сообщение",
        ),
    ]

    for message, label in tests:
        await test_case(url, message, label)


if __name__ == '__main__':
    try:
        trio.run(harmful_bus)
    except KeyboardInterrupt:
        logger.info("Прервано пользователем")
