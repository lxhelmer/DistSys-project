import asyncio

HOST = "svm-11-2.cs.helsinki.fi"
PORT = 64553  # Port for node2 to listen on
PEER_HOST = "svm-11.cs.helsinki.fi"
PEER_PORT = 64552  # Port for node2 to connect to node1

async def handle_connection(reader, writer):
    addr = writer.get_extra_info('peername')
    print(f"Node2 connected by {addr}")
    while True:
        data = await reader.read(1024)
        if not data:
            break
        print(f"Node2 received from {addr}: {data.decode()}")
        await asyncio.sleep(0.1)  # Add a small delay before sending acknowledgment
        writer.write(b"Message received by Node2")
        await writer.drain()

async def listen_for_messages(reader):
    while True:
        data = await reader.read(1024)
        if not data:
            break
        print(f"Node2 received acknowledgment: {data.decode()}")

async def connect_to_peer():
    while True:
        try:
            reader, writer = await asyncio.open_connection(PEER_HOST, PEER_PORT)
            return reader, writer
        except ConnectionRefusedError:
            print("Node1 not available, retrying...")
            await asyncio.sleep(1)

async def start_node():
    server = await asyncio.start_server(handle_connection, HOST, PORT)
    print(f"Node2 listening on {HOST}:{PORT}")
    
    asyncio.create_task(server.serve_forever())
    
    reader, writer = await connect_to_peer()
    asyncio.create_task(listen_for_messages(reader))
    writer.write(b"Hello from Node2")
    await writer.drain()

if __name__ == "__main__":
    asyncio.run(start_node())