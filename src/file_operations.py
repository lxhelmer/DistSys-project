import json

def send_file_list(client_socket, FILES):
    print("Answering file check with", FILES)
    client_socket.send(json.dumps({"type": "file_list", "HOST": NODE_HOST, "PORT": NODE_PORT, "NODE_ID": NODE_ID, "file_list": FILES}).encode('utf-8'))

def handle_file_update(data, client_socket, FILES):
    print("Checking local files against received file list")
    recv_files = sorted(data["file_list"])
    for r_file in recv_files:
        if r_file not in FILES:
            print("file missing:",r_file)
            handle_ask_file("r_file",client_socket)
            with open("../data/"+r_file, "wb") as f:
                while True:
                    recv_bytes = client_socket.recv(BUFFER_SIZE)
                    if not recv_bytes:
                        break
                    f.write(recv_bytes)


def handle_send_file(file_name, client_socket):
    with open("../data/"+file_name, "rb") as f:
        while True:
            send_bytes = f.read(BUFFER_SIZE)
            if not file_bytes:
                break
            client_socket.sendall(send_bytes)


def handle_ask_file(file_name, client_socket):
    print("Sending request for missing file")
    client_socket.send(json.dumps({"type": "file_request", "HOST": NODE_HOST, "PORT": NODE_PORT, "NODE_ID": NODE_ID, "file_name": file_name}).encode('utf-8'))
