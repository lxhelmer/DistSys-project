import asyncio
import json

# Load configuration from config.json
with open('config.json', 'r') as f:
    config = json.load(f)

NODE_ID = str(config['NODE_ID'])  # Ensure NODE_ID is a string
NODE_HOST = config['NODE_HOST']
NODE_PORT = config['NODE_PORT']
CONTROLLER_HOST = config['CONTROLLER_HOST']
CONTROLLER_PORT = config['CONTROLLER_PORT']

# Dictionary to store node information
nodes_info = {}

async def handle_controller(reader, writer):
    buffer = ""
    while True:
        data = await reader.read(100)
        if data:
            buffer += data.decode()
            try:
                updated_nodes_info = json.loads(buffer)
                buffer = ""  # Clear buffer after successful parsing
                for node_id, info in updated_nodes_info.items():
                    if str(node_id) != NODE_ID:  # Ensure comparison is with string
                        nodes_info[str(node_id)] = info
                # Remove own info if it exists
                nodes_info.pop(NODE_ID, None)
                print(f"Updated nodes info: {nodes_info}")
            except json.JSONDecodeError:
                # Wait for more data to complete the JSON object
                continue
        await asyncio.sleep(1)

async def connect_to_controller():
    reader, writer = await asyncio.open_connection(CONTROLLER_HOST, CONTROLLER_PORT)
    print(f"Connected to controller at {CONTROLLER_HOST}:{CONTROLLER_PORT}")
    
    # Send node ID, host, and port to the controller
    node_info = {
        "NODE_ID": NODE_ID,
        "NODE_HOST": NODE_HOST,
        "NODE_PORT": NODE_PORT
    }
    writer.write(json.dumps(node_info).encode())
    await writer.drain()
    
    # Start a task to handle incoming data from the controller
    asyncio.create_task(handle_controller(reader, writer))

async def send_message_to_nodes(message):
    for node_id, info in nodes_info.items():
        try:
            reader, writer = await asyncio.open_connection(info['NODE_HOST'], info['NODE_PORT'])
            writer.write(message.encode())
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            print(f"Sent message to {node_id}")
        except Exception as e:
            print(f"Failed to send message to {node_id}: {e}")

async def main():
    server = await asyncio.start_server(handle_controller, NODE_HOST, NODE_PORT)
    addr = server.sockets[0].getsockname()
    print(f'Node serving on {addr}')

    await connect_to_controller()

    async with server:
        asyncio.create_task(server.serve_forever())

        # Wait for nodes info to be updated before asking for input
        while not nodes_info:
            await asyncio.sleep(1)

        while True:
            message = input("Enter message to send to all nodes: ")
            await send_message_to_nodes(message)

asyncio.run(main())