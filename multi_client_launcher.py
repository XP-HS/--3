import threading
import socket
import time
import tkinter as tk
from tkinter import filedialog
import os

def client_task(name, file_path):
    global message
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect(('localhost', 54321))

        if os.path.exists(file_path):
            with open(file_path,'r') as file:
                for line in file:
                    line = line.strip()
                    if line:
                        parts =line.split()
                        operation = parts[0]
                        key = parts[1]
                        
                        if operation == "PUT":
                            value = " ".join(parts[2:])
                            message = str(len(key+value)+7).zfill(3)+" P "+key+" "+value
                        elif operation == "READ":
                            message = str(len(key)+6).zfill(3)+" R "+key
                        elif operation == "GET":
                            message = str(len(key)+6).zfill(3)+" G "+key
                        print(message)
                        client_socket.sendall(message.encode('utf-8'))
    
        final = "STOP"
        client_socket.sendall(final.encode('utf-8'))

        response = client_socket.recv(999).decode('utf-8')
        print(f"{name} received")
        print(f"{message}+{response}")
    except Exception as e:
        print(f"Error for {name}: {e}")
    finally:
        client_socket.close()
        
def main():
    clients = []
    file_paths = [
        "test-workload\\client_1.txt",  
        "test-workload\\client_2.txt",
        "test-workload\\client_3.txt",
        "test-workload\\client_4.txt",
        "test-workload\\client_5.txt",
        "test-workload\\client_6.txt",
        "test-workload\\client_7.txt",
        "test-workload\\client_8.txt",
        "test-workload\\client_9.txt",
        "test-workload\\client_10.txt",
    ]
    for i, file_path in enumerate(file_paths):
        t = threading.Thread(target=client_task, args=(f'client-{i+1}', file_path))
        clients.append(t)
        t.start()
        time.sleep(0.1)
    
    for t in clients:
        t.join()

if __name__ == "__main__":
    main()