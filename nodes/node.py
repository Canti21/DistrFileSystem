import os
import socket
import threading

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

def receive_file(connection):
    # Recibe los datos del archivo (nombre, peso, etc.)
    file_data = connection.recv(1024).decode()
    file_name, file_size = file_data.split(',')

    # Crea el directorio "data" si no existe
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)

    # Ruta completa del archivo en el directorio "data"
    file_path = os.path.join(DATA_FOLDER, file_name)

    # Notifica al cliente que el nodo esta listo para recibir
    ready_message = "READY"
    connection.sendall(ready_message.encode())

    # Recibe y almacena el archivo en la carpeta "data"
    with open(file_path, 'wb') as file:
        remaining_bytes = int(file_size)
        while remaining_bytes > 0:
            chunk = connection.recv(1024)
            file.write(chunk)
            remaining_bytes -= len(chunk)

    print(f"Archivo {file_name} recibido y almacenado en {file_path}")

    # Replica el archivo en otro nodo
    replicate_file(file_name)

    # Actualiza la información de archivos en este nodo
    with mutex:
        archivos_nodos[file_name] = str(socket.gethostbyname(socket.gethostname()))

def replicate_file(file_name):
    # Descubre otro nodo disponible para replicar el archivo
    with mutex:
        available_nodes = list(nodos_disponibles)

    if len(available_nodes) > 1:
        # Elimina el nodo actual de la lista de nodos disponibles
        available_nodes.remove(str(socket.gethostbyname(socket.gethostname())))

        # Elige un nodo al azar para replicar el archivo
        node_address = available_nodes[0]
        node_host, node_port = node_address.split(':')

        # Crea un socket TCP para conectarse al nodo de replicación
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as node_socket:
            try:
                # Conecta el socket al nodo de replicación
                node_socket.connect((node_host, int(node_port)))

                # Envía el mensaje de registro al nodo de replicación (opcional)
                node_socket.sendall("REGISTRO".encode())

                # Lee el contenido del archivo
                file_path = os.path.join(DATA_FOLDER, file_name)
                with open(file_path, 'rb') as file:
                    file_data = file.read()

                # Envía el archivo al nodo de replicación
                node_socket.sendall(file_data)

                print(f"Archivo {file_name} replicado en nodo {node_address}")
            except ConnectionRefusedError:
                print(f"No se pudo replicar el archivo {file_name} en nodo {node_address}")
            finally:
                # Cierra la conexión con el nodo de replicación
                node_socket.close()
    else:
        print(f"No hay nodos disponibles para replicar el archivo {file_name}")

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

def start_node():
    # Anunciando al servidor de nodos que estamos en línea
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as serv:
        try:
            serv.connect((SERV_HOST, SERV_PORT))
            print(f"Anunciando a central en {SERV_HOST}: {SERV_PORT}")

            mensaje = "REGISTRO"
            serv.send(mensaje.encode())

            respuesta = serv.recv(1024).decode()
            print(f"Respuesta del servidor: {respuesta}")
        except ConnectionRefusedError:
            print("No se pudo establecer la conexión...")
        finally:
            serv.close()

    # Crea un socket TCP
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        # Enlace del socket a la dirección IP y puerto especificados
        server_socket.bind((HOST, PORT))

        # Escucha las conexiones entrantes
        server_socket.listen()

        print("El nodo está listo y esperando conexiones...")

        while True:
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

            # Cierra la conexión con el cliente
            connection.close()

if __name__ == '__main__':
    start_node()
