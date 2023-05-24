import os
import socket
import threading
import time

# Dirección IP y puerto en el que el nodo escuchará las conexiones
HOST = '192.168.1.74'
PORT = 8100

# Dirección y puerto del servidor de nodos
SERV_HOST = '192.168.1.72'
SERV_PORT = 8000

# Carpeta donde se almacenarán los archivos recibidos
DATA_FOLDER = 'data'

# Arreglo para almacenar la lista de nodos disponibles
nodos_disponibles = []

# Diccionario para almacenar la información de archivos en cada nodo
archivos_nodos = {}

# Mutex para sincronización en el acceso a los arreglos
mutex = threading.Lock()

def discover_nodes():
    # Crea un socket TCP para conectarse al servidor de registro y descubrimiento
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        # Conecta el socket al servidor
        server_socket.connect((SERV_HOST, SERV_PORT))

        # Envía el mensaje de descubrimiento al servidor
        server_socket.sendall("DESCUBRIR".encode())

        # Recibe la respuesta del servidor
        respuesta = server_socket.recv(1024).decode()

        # Cierra la conexión con el servidor
        server_socket.close()

    # Retorna la lista de nodos disponibles
    return respuesta.split(',')

def receive_file(connection):
    # Notifica al cliente que el nodo está listo para recibir
    ready_message = "READY"
    connection.sendall(ready_message.encode())
    # Recibe los datos del archivo (nombre, peso, etc.)
    try:
        file_data = connection.recv(1024).decode()
        file_name, file_size = file_data.split(',')

        # Crea el directorio "data" si no existe
        if not os.path.exists(DATA_FOLDER):
            os.makedirs(DATA_FOLDER)

        # Ruta completa del archivo en el directorio "data"
        file_path = os.path.join(DATA_FOLDER, file_name)

        # Recibe y almacena el archivo en la carpeta "data"
        with open(file_path, 'wb') as file:
            remaining_bytes = int(file_size)
            while remaining_bytes > 0:
                chunk = connection.recv(1024)
                file.write(chunk)
                remaining_bytes -= len(chunk)
        
        connection.sendall("SUCCESS".encode())
        print(f"Archivo {file_name} recibido y almacenado en {file_path}")

        # Replica el archivo en otro nodo
        send_file_replica(file_path)

        # Actualiza la información de archivos en este nodo
        with mutex:
            archivos_nodos[file_name] = str(socket.gethostbyname(socket.gethostname()))

    except UnicodeDecodeError:
        connection.sendall("FAILURE".encode())
        print("Ocurrio un error al recibir el archivo.")

def send_file_replica(file_path):
    i = 0
    while i < 3:
        available_nodes = discover_nodes()
        available_nodes.remove(HOST)

        if len(available_nodes) > 0:
            file_name = os.path.basename(file_path)

            for node_address in available_nodes:
                node_host = node_address

                # Crea un socket TCP
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
                    try:
                        # Conecta el socket al nodo destino
                        client_socket.connect((node_host, 8100))

                        # Envía el comando al nodo
                        command = "ENVIAR"
                        client_socket.sendall(command.encode())

                        # Recibe la respuesta del nodo
                        response = client_socket.recv(1024).decode()

                        if response == "READY":
                            # Obtiene el tamaño del archivo
                            file_size = os.path.getsize(file_path)

                            # Envía los datos del archivo (nombre y tamaño)
                            file_data = f"{file_name},{file_size}"
                            client_socket.sendall(file_data.encode())

                            # Lee y envía el contenido del archivo en bloques
                            with open(file_path, 'rb') as file:
                                for chunk in iter(lambda: file.read(1024), b''):
                                    client_socket.sendall(chunk)
                            
                            response = client_socket.recv(1024).decode()
                            if response == "SUCCESS":
                                print(f"Replica {file_name} enviado correctamente al nodo {node_address}")
                            else:
                                i = i + 1;
                            return

                    except ConnectionRefusedError:
                        print(f"No se pudo conectar al nodo {node_address}. Intentando con otro nodo...")
                    except ConnectionResetError:
                        print(f"Hubo un error de conexion...")

        print("No se encontraron nodos disponibles en el sistema o todos los nodos estaban inaccesibles.")
        i = i + 1;

def receive_replica(connection):
    try:
        # Recibe los datos del archivo (nombre, peso, etc.)
        file_data = connection.recv(1024).decode()
        file_name = file_data

        # Crea el directorio "data" si no existe
        if not os.path.exists(DATA_FOLDER):
            os.makedirs(DATA_FOLDER)

        # Ruta completa del archivo en el directorio "data"
        file_path = os.path.join(DATA_FOLDER, file_name)

        # Recibe y almacena el archivo en la carpeta "data"
        with open(file_path, 'wb') as file:
            while True:
                chunk = connection.recv(1024)
                if not chunk:
                    break
                file.write(chunk)

        connection.sendall("SUCCESS".encode())
        print(f"Réplica del archivo {file_name} recibida y almacenada en {file_path}")

        # Actualiza la información de archivos en este nodo
        with mutex:
            archivos_nodos[file_name] = str(socket.gethostbyname(socket.gethostname()))
    except UnicodeDecodeError:
        connection.sendall("FAILURE".encode())
        print("Ocurrio un error al recibir la replica el archivo.")

def send_file(connection, file_name):
    # Ruta completa del archivo en el directorio "data"
    file_path = os.path.join(DATA_FOLDER, file_name)

    if os.path.isfile(file_path):
        # El archivo existe en este nodo, se envía al usuario
        with open(file_path, 'rb') as file:
            file_data = file.read()

        # Envía los datos del archivo al cliente
        connection.sendall(file_data)
        print(f"Archivo {file_name} enviado al usuario")
    else:
        # El archivo no existe en este nodo
        connection.sendall(b"El archivo solicitado no existe en este nodo")
        print(f"El archivo {file_name} no existe en este nodo")

def register_to_server():
    # Crea un socket TCP para conectarse al servidor de nodos
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as serv:
        try:
            serv.connect((SERV_HOST, SERV_PORT))
            print(f"Anunciando a central en {SERV_HOST}: {SERV_PORT}")

            mensaje = "REGISTRO"
            serv.send(mensaje.encode())

            respuesta = serv.recv(1024).decode()
            print(f"Respuesta del servidor: {respuesta}")

            # Actualiza la lista de nodos disponibles
            with mutex:
                nodos_disponibles.extend(respuesta.split(','))
        except ConnectionRefusedError:
            print("No se pudo establecer la conexión con el servidor de nodos.")
        finally:
            serv.close()

def start_node():
    # Anuncia al servidor de nodos que estamos en línea
    register_to_server()

    # Crea un socket TCP
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        # Enlace del socket a la dirección IP y puerto especificados
        server_socket.bind((HOST, PORT))

        # Escucha las conexiones entrantes
        server_socket.listen()

        while True:
            print("El nodo está listo y esperando conexiones...")

            # Acepta la conexión entrante del cliente
            connection, address = server_socket.accept()

            print(f"Conexión establecida desde {address}")

            # Recibe el comando del cliente
            command = connection.recv(1024).decode()

            if command == 'ENVIAR':
                # El cliente quiere enviar un archivo
                receive_file(connection)
            elif command == 'RECUPERAR':
                # El cliente quiere recuperar un archivo
                file_name = connection.recv(1024).decode()
                print(f"Solicitando archivo: {file_name}")
                send_file(connection, file_name)
            elif command == 'REPLICAR':
                # El cliente quiere enviar una réplica de archivo
                receive_replica(connection)

            # Cierra la conexión con el cliente
            connection.close()

if __name__ == '__main__':
    start_node()
