import socket
import threading
import json

with open('config.json', 'r') as config_file:
    config = json.load(config_file)

CONTROLLER_HOST = config['CONTROLLER_HOST']
CONTROLLER_PORT = config['CONTROLLER_PORT']
NODE_HOST = config['NODE_HOST']
NODE_PORT = config['NODE_PORT']
NODE_ID = config['NODE_ID']

NODES = []

def handle_client_connection(client_socket):
    try:
        while True:
            data = client_socket.recv(1024)
            if not data:
                break
            try:
                node_info_list = json.loads(data.decode('utf-8'))
                #print(f"Received node info: {node_info_list}")
                update_nodes_list(node_info_list)
            except json.JSONDecodeError:
                message = data.decode('utf-8')
                print(f"Received message: {message}")
                prompt_for_message()  # Prompt for a new message after receiving one
    except socket.error as e:
        print(f"Socket error: {e}")
    finally:
        client_socket.close()

def listen_for_connection(host, port):
    try:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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
    NODES = []
    for node in node_info:
        if node['NODE_ID'] != NODE_ID:
            NODES.append(node)
            #print(f"Node added: {node}")
    else:
        print(f"Node already in list or is self: {node}")
    #print(NODES)
    prompt_for_message()

def send_node_info_to_controller():
    node_info = {
        'HOST': NODE_HOST,
        'PORT': NODE_PORT,
        'NODE_ID': NODE_ID
    }
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((CONTROLLER_HOST, CONTROLLER_PORT))
        s.sendall(json.dumps(node_info).encode('utf-8'))
        data = s.recv(1024)
        global NODES
        NODES = json.loads(data.decode('utf-8'))
        #print(f"Updated NODES list: {NODES}")
        s.close()
    except socket.error as e:
        print(f"Socket error: {e}")

def prompt_for_message():
    print("Enter the message to send to all nodes: ")
    message = input()
    send_message_to_all_nodes(message)

def send_message_to_all_nodes(message):  
    for node in NODES:
        try:
            #print(node)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((node['HOST'], node['PORT']))
            s.sendall(message.encode('utf-8'))
            s.close()  # Close the connection after sending the message
            #print(f"Message sent to {node['NODE_ID']}")
        except socket.error as e:
            print(f"Socket error: {e}")
    prompt_for_message()  # Prompt for a new message after sending to all nodes

if __name__ == '__main__':
    listener_thread = threading.Thread(target=listen_for_connection, args=(NODE_HOST, NODE_PORT))
    listener_thread.start()
    
    send_node_info_to_controller()