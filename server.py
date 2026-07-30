import json
import trio
from trio_websocket import serve_websocket, ConnectionClosed
from functools import partial


async def handle_imitation(request, buses):
    ws = await request.accept()
    print("Имитатор подключился")
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
                    print(f"Обновлён автобус {bus_id}: {data}")
            except json.JSONDecodeError:
                print(f"Получено некорректное JSON-сообщение: {message}")
    except ConnectionClosed:
        print("Имитатор отключился")


async def talk_to_browser(request, buses):
    ws = await request.accept()
    print("Браузер подключён")
    try:
        while True:
            buses_list = list(buses.values())
            message = {
                'msgType': 'Buses',
                'buses': buses_list,
            }
            await ws.send_message(json.dumps(message, ensure_ascii=False))
            await trio.sleep(1)
    except ConnectionClosed:
        print("Браузер отключён")


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
