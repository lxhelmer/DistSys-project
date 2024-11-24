import asyncio
import json

# Load configuration from config.json
with open('config.json', 'r') as f:
    config = json.load(f)

CONTROLLER_HOST = config['CONTROLLER_HOST']
CONTROLLER_PORT = config['CONTROLLER_PORT']

# Dictionary to store node information
nodes_info = {}

async def handle_client(reader, writer):
    addr = writer.get_extra_info('peername')
    print(f"Received connection from {addr}")
    
    # Read node information from the client
    data = await reader.read(100)
    node_info = json.loads(data.decode())
    
    # Save node information if not already saved
    node_id = node_info['NODE_ID']
    if node_id not in nodes_info:
        nodes_info[node_id] = node_info
        print(f"Saved node info: {node_info}")
        print("All nodes' info:")
        for node in nodes_info.values():
            print(node)
        
        # Send updated nodes info to all connected nodes
        for node in nodes_info.values():
            await send_nodes_info(node)
    else:
        print(f"Node {node_id} is already saved.")
    
    # Keep the connection open
    while True:
        data = await reader.read(100)
        if not data:
            break
    writer.close()
    await writer.wait_closed()

async def send_nodes_info(node):
    try:
        reader, writer = await asyncio.open_connection(node['NODE_HOST'], node['NODE_PORT'])
        writer.write(json.dumps(nodes_info).encode())
        await writer.drain()
        writer.close()
        await writer.wait_closed()
    except Exception as e:
        print(f"Failed to send nodes info to {node['NODE_ID']}: {e}")

async def main():
    server = await asyncio.start_server(handle_client, CONTROLLER_HOST, CONTROLLER_PORT)
    addr = server.sockets[0].getsockname()
    print(f'Serving on {addr}')

    async with server:
        await server.serve_forever()

asyncio.run(main())