import trio
from trio_websocket import serve_websocket, ConnectionClosed
import json

async def bus_receiver(request):
    ws = await request.accept()
    print("Имитатор подключился")
    while True:
        try:
            message = await ws.get_message()
            print(message)
        except ConnectionClosed:
            print("Имитатор отключился")
            break

async def main():
    await serve_websocket(bus_receiver, '127.0.0.1', 8080, ssl_context=None)

if __name__ == "__main__":
    trio.run(main)
