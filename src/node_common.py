import asyncio
import json

ACK_PREFIX = "ACK:"

async def handle_connection(reader, writer, node_name, node_info_callback=None):
    addr = writer.get_extra_info('peername')
    print(f"{node_name} connected by {addr}")
    initial_message = True
    while True:
        data = await reader.read(1024)
        if not data:
            break
        message = data.decode()
        print(f"{node_name} received from {addr}: {message}")
        if initial_message and node_info_callback:
            await node_info_callback(addr, message, writer)  # Pass writer to the callback
            initial_message = False
        else:
            if not message.startswith(ACK_PREFIX):
                acknowledgment = f"{ACK_PREFIX} Message received by {node_name}"
                writer.write(acknowledgment.encode())
                await writer.drain()
                print(f"{node_name} sent acknowledgment to {addr}: {acknowledgment}")
    print(f"{node_name} connection closed by {addr}")

async def listen_for_messages(reader, writer, node_name):
    while True:
        data = await reader.read(1024)
        if not data:
            break
        message = data.decode()
        if message.startswith(ACK_PREFIX):
            print(f"{node_name} received acknowledgment: {message}")
        else:
            print(f"{node_name} received message: {message}")
            # Send acknowledgment for received message
            acknowledgment = f"{ACK_PREFIX} Message received by {node_name}"
            writer.write(acknowledgment.encode())
            await writer.drain()
            print(f"{node_name} sent acknowledgment for message: {acknowledgment}")
    print(f"{node_name} connection closed")

async def connect_to_peer(peer_host, peer_port, node_name):
    while True:
        try:
            reader, writer = await asyncio.open_connection(peer_host, peer_port)
            print(f"{node_name} connected to peer at {peer_host}:{peer_port}")
            return reader, writer
        except ConnectionRefusedError:
            print(f"{node_name} not available, retrying...")
            await asyncio.sleep(1)

async def send_user_input(writer, node_name):
    loop = asyncio.get_running_loop()
    while True:
        message = await loop.run_in_executor(None, input, f"{node_name} > ")
        writer.write(message.encode())
        await writer.drain()
        print(f"{node_name} sent: {message}")

async def start_node(host, port, peer_host, peer_port, node_name, node_info_callback=None):
    server = await asyncio.start_server(lambda r, w: handle_connection(r, w, node_name, node_info_callback), host, port)
    print(f"{node_name} listening on {host}:{port}")
    
    asyncio.create_task(server.serve_forever())
    
    if peer_host and peer_port:
        reader, writer = await connect_to_peer(peer_host, peer_port, node_name)
        asyncio.create_task(listen_for_messages(reader, writer, node_name))
        asyncio.create_task(send_user_input(writer, node_name))
        writer.write(f"{host}:{port}".encode())  # Send node's own host and port
        await writer.drain()
        print(f"{node_name} sent its info to the controller")

    # Keep the node running
    while True:
        await asyncio.sleep(1)

def load_config():
    with open('config.json', 'r') as f:
        return json.load(f)