import asyncio
from node_common import start_node, load_config

config = load_config()

HOST = config["NODE_HOST"]
PORT = config["NODE_PORT"]
CONTROLLER_HOST = config["CONTROLLER_HOST"]
CONTROLLER_PORT = config["CONTROLLER_PORT"]

async def send_node_info(addr, message, writer):
    node_info = f"{HOST}:{PORT}"
    writer.write(node_info.encode())
    await writer.drain()
    print(f"Node sent its info: {node_info}")

async def start_regular_node():
    await start_node(HOST, PORT, CONTROLLER_HOST, CONTROLLER_PORT, "Node", send_node_info)

if __name__ == "__main__":
    asyncio.run(start_regular_node())