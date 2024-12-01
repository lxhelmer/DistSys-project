import socket
import threading
import json

with open('config.json', 'r') as config_file:
    config = json.load(config_file)

CONTROLLER_HOST = config['CONTROLLER_HOST']
CONTROLLER_PORT = config['CONTROLLER_PORT']

NODES = []

def handle_client_connection(client_socket):
    try:
        while True:
            data = client_socket.recv(1024)
            if not data:
                break
            node_info = json.loads(data.decode('utf-8'))
            print(f"Received node info: {node_info}")
            update_nodes_list(node_info)
            send_nodes_list_to_all()
    except socket.error as e:
        print(f"Socket error: {e}")
    finally:
        client_socket.close()

def listen_for_connection(host, port):
    try:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((host, port))
        server_socket.listen(5)
        print(f"Listening on {host}:{port}")
        
        while True:
            client_socket, addr = server_socket.accept()
            print(f"Connection from {addr}")
            client_thread = threading.Thread(target=handle_client_connection, args=(client_socket,))
            client_thread.start()
    except socket.error as e:
        print(f"Socket error: {e}")

def update_nodes_list(node_info):
    global NODES
    if node_info not in NODES:
        NODES.append(node_info)
        print(f"Node added: {node_info}")
    else:
        print(f"Node already in list: {node_info}")

def send_nodes_list_to_all():
    global NODES
    nodes_list_json = json.dumps(NODES)
    for node in NODES:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((node['HOST'], node['PORT']))
            s.sendall(nodes_list_json.encode('utf-8'))
            s.close()
            print(f"Sent nodes list to {node['NODE_ID']} at {node['HOST']}:{node['PORT']}")
            print(NODES)
        except socket.error as e:
            print(f"Socket error: {e}")

if __name__ == '__main__':
    listener_thread = threading.Thread(target=listen_for_connection, args=(CONTROLLER_HOST, CONTROLLER_PORT))
    listener_thread.start()