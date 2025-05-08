import socket
import threading

tuple_space = {}
client_connections = 0
total_operations = 0
total_reads = 0
total_gets = 0
total_puts = 0
errors = 0


def read(key):
    if key in tuple_space:
        value = tuple_space[key]
        total_operations += 1
        total_reads += 1
        return f"OK ({key}, {value}) read"
    else:
        total_operations += 1
        errors += 1
        return f"ERR {key} does not exist" 

def get(key):
    if key in tuple_space:
        value = tuple_space.pop(key)
        total_operations += 1
        total_gets += 1
        return f"OK ({key}, {value}) removed"
    else:
        total_operations += 1
        errors += 1
        return f"ERR {key} does not exist"

def put(key, value):
    if key in tuple_space:
        total_operations += 1
        errors += 1
        return f"ERR {key} already exists"
    else:
        tuple_space[key] = value
        total_operations += 1
        total_puts += 1
        return f"OK ({key}, {value}) added"

def handle_client(client_socket, addr):
    print(f"New client connected from {addr}")
    
    try:
        while(True):
            message = client_socket.recv(999).decode('utf-8')
            #final = client_socket.recv(999).decode('utf-8')
            # print(f"Client says: {message}")
            if(message == "STOP"):
                break  

            parts = message.split()
            operation = parts[1]
            key = parts[2]

            if operation == "R":
                message2 = read(key)
            elif operation == "G":
                message2 = get(key)
            elif operation == "P":
                if len(parts) < 4:
                    message2 = "ERR Invalid request"
                else:
                    value = " ".join(parts[3:])
                    message2 = put(key, value)
            else:
                message2 = "ERR Invalid operation"
            print(message2)
            response = " " + message2
            client_socket.sendall(response.encode('utf-8'))
        

    except Exception as e:
        print(f"Error in handling client {addr}: {e}")
    finally:
        client_connections -= 1
        client_socket.close()
        print(f"Connection with {addr} has been closed.")

def start_server():
    host = 'localhost'
    port = 54321

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock_addr = (host, port)
    server_socket.bind(sock_addr)
    server_socket.listen(10)
    print("Server is running and ready to accept multiple clients...")

    try:
        while True:
            client_socket, addr = server_socket.accept()
            client_handler = threading.Thread(target=handle_client, args=(client_socket, addr))
            client_handler.start()
    except KeyboardInterrupt:
        print("Shutting down the server...")
    finally:
        server_socket.close()

if __name__ == "__main__":
    start_server()