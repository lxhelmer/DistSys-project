import asyncio
from node_common import start_node, load_config, handle_connection, send_user_input, connect_to_peer, listen_for_messages

config = load_config()

HOST = config["CONTROLLER_HOST"]
PORT = config["CONTROLLER_PORT"]

connected_nodes = {}

async def store_node_info(addr, message, writer):
    if addr not in connected_nodes:
        print(f"Storing node info: {message} from {addr}")
        connected_nodes[addr] = writer
        await connect_to_first_peer(message)

async def connect_to_first_peer(message):
    global peer_connection
    try:
        peer_host, peer_port = message.split(':')
        peer_connection = await connect_to_peer(peer_host, int(peer_port), "Controller Node")
        reader, writer = peer_connection
        asyncio.create_task(listen_for_messages(reader, writer, "Controller Node"))
        asyncio.create_task(send_user_input(writer, "Controller Node"))
    except ValueError as e:
        print(f"Error parsing message '{message}': {e}")

async def broadcast_message(message):
    for addr, writer in connected_nodes.items():
        writer.write(message.encode())
        await writer.drain()
        print(f"Controller Node sent to {addr}: {message}")

async def send_user_input(writer, node_name):
    loop = asyncio.get_running_loop()
    while True:
        message = await loop.run_in_executor(None, input, f"{node_name} > ")
        await broadcast_message(message)

async def start_controller_node():
    server = await asyncio.start_server(lambda r, w: handle_connection(r, w, "Controller Node", store_node_info), HOST, PORT)
    print(f"Controller Node listening on {HOST}:{PORT}")
    
    asyncio.create_task(server.serve_forever())

    # Keep the node running
    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(start_controller_node())