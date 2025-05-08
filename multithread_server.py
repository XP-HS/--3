import socket
import threading
import time

tuple_space = {}
client_connections = 0
total_operations = 0
total_reads = 0
total_gets = 0
total_puts = 0
errors = 0


def read(key):
    global total_operations, total_reads,errors
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
    global total_operations, total_gets,errors
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
    global total_operations, total_puts,errors
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
    global client_connections,errors
    client_connections += 1
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
            #lprint(message2)
            response = " " + message2
            client_socket.sendall(response.encode('utf-8'))
        

    except Exception as e:
        print(f"Error in handling client {addr}: {e}")
        errors +=1
    finally:
        client_connections -= 1
        client_socket.close()
        print(f"Connection with {addr} has been closed.")

def print_stats():
    while True:
        time.sleep(10)
        with threading.Lock():
            num_tuples = len(tuple_space)
            avg_tuple_size = sum(len(k) + len(v) for k, v in tuple_space.items()) / num_tuples if num_tuples > 0 else 0
            avg_key_size = sum(len(k) for k in tuple_space.keys()) / num_tuples if num_tuples > 0 else 0
            avg_value_size = sum(len(v) for v in tuple_space.values()) / num_tuples if num_tuples > 0 else 0
            print(f"Tuples: {num_tuples}, Avg Tuple Size: {avg_tuple_size:.2f}, Avg Key Size: {avg_key_size:.2f}, Avg Value Size: {avg_value_size:.2f}, Clients: {client_connections}, Operations: {total_operations}, Reads: {total_reads}, Gets: {total_gets}, Puts: {total_puts}, Errors: {errors}")

def start_server():
    host = 'localhost'
    port = 54321

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock_addr = (host, port)
    server_socket.bind(sock_addr)
    server_socket.listen(10)
    print("Server is running and ready to accept multiple clients...")

    stats_thread = threading.Thread(target=print_stats)
    stats_thread.daemon = True
    stats_thread.start()


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