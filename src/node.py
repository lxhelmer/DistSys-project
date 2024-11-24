import asyncio
import json

with open('config.json', 'r') as f:
    config = json.load(f)

HOST = config['host']
PORT = config['port']
CONTROLLER_HOST = config['controller_host']
CONTROLLER_PORT = config['controller_port']

async def handle_connection(reader, writer):
    addr = writer.get_extra_info('peername')
    print(f"Connected by {addr}")
    while True:
        data = await reader.read(1024)
        if not data:
            break
        print(f"Received from {addr}: {data.decode()}")
        await asyncio.sleep(0.1)
        writer.write(b"Message received")
        await writer.drain()

async def listen_for_messages(reader):
    while True:
        data = await reader.read(1024)
        if not data:
            break
        print(f"Received acknowledgment: {data.decode()}")

async def connect_to_peer():
    while True:
        try:
            reader, writer = await asyncio.open_connection(CONTROLLER_HOST, CONTROLLER_PORT)
            return reader, writer
        except ConnectionRefusedError:
            print(f"{CONTROLLER_HOST}:{CONTROLLER_PORT} not available, retrying...")
            await asyncio.sleep(1)

async def send_user_input(writer):
    while True:
        message = await asyncio.get_event_loop().run_in_executor(None, input, "Enter message: ")
        writer.write(message.encode())
        await writer.drain()

async def start_node():
    server = await asyncio.start_server(handle_connection, HOST, PORT)
    print(f"Listening on {HOST}:{PORT}")
    
    asyncio.create_task(server.serve_forever())
    
    reader, writer = await connect_to_peer()
    asyncio.create_task(listen_for_messages(reader))
    asyncio.create_task(send_user_input(writer))

    # Keep the node running
    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(start_node())