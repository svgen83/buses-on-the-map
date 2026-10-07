import json
import trio
from trio_websocket import open_websocket_url


async def harmful_bus():
    url = 'ws://127.0.0.1:8080'
    try:
        async with open_websocket_url(url) as ws:
            print("Подключено к серверу (как имитатор)")

            await ws.send_message("not a json")
            response = await ws.get_message()
            print(f"Response: {response}")

    except OSError as e:
        print(f"Ошибка подключения: {e}")

    tests = [
        json.dumps({"lat": 55.7, "lng": 37.6}),                       # нет busId
        json.dumps({"busId": "abc", "lng": 37.6}),                    # нет lat
        json.dumps({"busId": "abc", "lat": 55.7}),                    # нет lng
        json.dumps({"busId": "abc", "lat": "55.7", "lng": 37.6}),     # lat — строка
        json.dumps({"busId": "abc", "lat": 55.7, "lng": 37.6, "route": 123}),  # route — число
    ]

    for test in tests:
        try:
            async with open_websocket_url(url) as ws:
                await ws.send_message(test)
                response = await ws.get_message()
                print(f"Response: {response}")
        except OSError as e:
            print(f"Ошибка подключения: {e}")


if __name__ == '__main__':
    trio.run(harmful_bus)
