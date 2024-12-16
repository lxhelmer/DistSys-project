import os
import json 
import socket

def send_file_list(client_socket, FILES, NODE_HOST, NODE_PORT, NODE_ID):
    print("Answering file check with", FILES)
    client_socket.send(json.dumps({"type": "file_list", "HOST": NODE_HOST, "PORT": NODE_PORT, "NODE_ID": NODE_ID, "file_list": FILES}).encode('utf-8'))

def handle_file_update(data, client_socket, FILES, CONTROLLER_HOST, CONTROLLER_PORT, NODE_HOST, NODE_PORT, NODE_ID):
    print("Checking local files against received file list")
    recv_files = sorted(data["file_list"])
    for r_file in recv_files:
        if r_file not in FILES:
            print("file missing:",r_file)
            try:
                print("Create file socket")
                file_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                file_socket.connect((CONTROLLER_HOST, CONTROLLER_PORT))
                handle_ask_file(r_file, file_socket, NODE_HOST, NODE_PORT, NODE_ID)
            except socket.error as e:
                print(f" fileSocket error: {e}")
    for l_file in FILES:
        if l_file not in recv_files:
            print("Removing nonmatching file:", l_file)
            os.remove("../data/"+l_file)


def handle_send_file(file_name, client_socket):
    with open("../data/"+file_name, "rb") as f:
        while True:
            send_bytes = f.read(1024)
            print("SENDING")
            print(send_bytes)
            if not (send_bytes):
                break
            client_socket.send(send_bytes)
    print("Sent whole file")

def handle_ask_file(file_name, file_socket, NODE_HOST, NODE_PORT, NODE_ID):
    print("Sending request for missing file", file_name)
    file_socket.send(json.dumps({"type": "file_request", "HOST": NODE_HOST, "PORT": NODE_PORT, "NODE_ID": NODE_ID, "file_name": file_name}).encode('utf-8'))
    with open("../data/"+file_name, "wb") as f:
        while True:
            recv_bytes = file_socket.recv(1024)
            print("RECEIVING")
            print(recv_bytes)
            if not recv_bytes:
                break
            f.write(recv_bytes)
    print("Received whole file")
    file_socket.close()


def check_files():
    return os.listdir('../data')
