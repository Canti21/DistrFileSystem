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
            archivos_nodos[file_name] = HOST

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
                        command = "REPLICAR"
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
                                send_file_replica(file_path)
                            return

                    except ConnectionRefusedError:
                        print(f"No se pudo conectar al nodo {node_address}. Intentando con otro nodo...")
                    except ConnectionResetError:
                        print(f"Hubo un error de conexion...")

        print("No se encontraron nodos disponibles en el sistema o todos los nodos estaban inaccesibles.")
        i = i + 1;

def receive_replica(connection):
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

        # Actualiza la información de archivos en este nodo
        with mutex:
            archivos_nodos[file_name] = str(socket.gethostbyname(socket.gethostname()))

    except UnicodeDecodeError:
        connection.sendall("FAILURE".encode())
        print("Ocurrio un error al recibir el archivo.")


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
            elif command == 'DESCARGAR':
                # Recibe el nombre del archivo a descargar
                file_name = connection.recv(1024).decode()

                # Ruta completa del archivo en la carpeta "data"
                file_path = os.path.join(DATA_FOLDER, file_name)
                file_size = os.path.getsize(file_path)

                # Verifica si el archivo existe en el nodo actual
                if os.path.exists(file_path):
                    # Notifica al cliente que el archivo existe en el nodo actual
                    connection.sendall("EXISTE".encode())

                    # El archivo existe en este nodo, envíalo al cliente
                    try:
                        # Notifica al cliente que el nodo está listo para enviar el archivo
                        ready_message = "READY"
                        connection.sendall(ready_message.encode())

                        # Envía los datos del archivo (nombre y tamaño)
                        file_size = os.path.getsize(file_path)
                        file_data = f"{file_name},{file_size}"
                        connection.sendall(file_data.encode())

                        # Envía el contenido del archivo en bloques
                        with open(file_data, 'rb') as file:
                            for chunk in iter(lambda: file.read(1024), b''):
                                connection.sendall(chunk)

                        response = connection.recv(1024).decode()
                        if response == "SUCCESS":
                            print(f"Archivo {file_name} enviado correctamente al cliente")
                        else:
                            print(f"Ocurrió un error al enviar el archivo {file_name} al cliente")

                    except ConnectionResetError:
                        print("Se perdió la conexión con el cliente.")
                else:
                    # El archivo no existe en este nodo, se intenta recuperar de otro nodo
                    available_nodes = discover_nodes()
                    available_nodes.remove(HOST)

                    if len(available_nodes) > 0:
                        for node_address in available_nodes:
                            node_host = node_address

                            # Crea un socket TCP
                            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
                                try:
                                    # Conecta el socket al nodo destino
                                    client_socket.connect((node_host, 8100))

                                    # Envía el comando al nodo
                                    command = "DESCARGAR"
                                    client_socket.sendall(command.encode())

                                    # Envía el nombre de archivo al nodo
                                    client_socket.sendall(file_name.encode())

                                    # Recibe la respuesta del nodo
                                    response = client_socket.recv(1024).decode()

                                    if response:
                                        client_socket.sendall("READY".encode())
                                        file_data = client_socket.recv(1024).decode()
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
                                                chunk = client_socket.recv(1024)
                                                file.write(chunk)
                                                remaining_bytes -= len(chunk)
                                        
                                        client_socket.sendall("SUCCESS".encode())
                                        print(f"Archivo {file_name} recibido y almacenado en {file_path}")

                                        connection.sendall("EXISTE".encode())

                                        try:
                                            # Notifica al cliente que el nodo está listo para enviar el archivo
                                            ready_message = "READY"
                                            connection.sendall(ready_message.encode())

                                            # Envía los datos del archivo (nombre y tamaño)
                                            file_size = os.path.getsize(file_path)
                                            file_data = f"{file_name},{file_size}"
                                            connection.sendall(file_data.encode())

                                            # Envía el contenido del archivo en bloques
                                            with open(file_path, 'rb') as file:
                                                for chunk in iter(lambda: file.read(1024), b''):
                                                    connection.sendall(chunk)

                                            response = connection.recv(1024).decode()
                                            if response == "SUCCESS":
                                                print(f"Archivo {file_name} enviado correctamente al cliente")
                                                return
                                            else:
                                                print(f"Ocurrió un error al enviar el archivo {file_name} al cliente")

                                        except ConnectionResetError:
                                            print("Se perdió la conexión con el cliente.")

                                except ConnectionRefusedError:
                                    print(f"No se pudo conectar al nodo {node_address}. Intentando con otro nodo...")
                                except ConnectionResetError:
                                    print(f"Hubo un error de conexión...")

                        connection.sendall("".encode())

                    else:
                        # No hay otros nodos disponibles en el sistema
                        error_message = "No se encontraron nodos disponibles en el sistema o todos los nodos estaban inaccesibles."
                        connection.sendall(error_message.encode())
                        print(error_message)

            elif command == 'REPLICAR':
                # El cliente quiere enviar una réplica de archivo
                receive_replica(connection)

            # Cierra la conexión con el cliente
            connection.close()

def send_file_to_client(connection, file_path):
    try:
        # Abre el archivo y envía su contenido en bloques
        with open(file_path, 'rb') as file:
            # Notifica al cliente que el nodo está listo para enviar
            ready_message = "READY"
            connection.sendall(ready_message.encode())

            # Obtiene el tamaño del archivo
            file_size = os.path.getsize(file_path)

            # Envía los datos del archivo (nombre y tamaño)
            file_data = f"{os.path.basename(file_path)},{file_size}"
            connection.sendall(file_data.encode())

            # Lee y envía el contenido del archivo en bloques
            for chunk in iter(lambda: file.read(1024), b''):
                connection.sendall(chunk)

            # Espera la confirmación de éxito desde el cliente
            response = connection.recv(1024).decode()
            if response == "SUCCESS":
                print(f"Archivo {os.path.basename(file_path)} enviado correctamente al cliente")
            else:
                print(f"Error al enviar archivo {os.path.basename(file_path)} al cliente")

    except IOError:
        print(f"Error al abrir el archivo {os.path.basename(file_path)}")

if __name__ == '__main__':
    start_node()
