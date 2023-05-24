import os
import socket

# Dirección IP y puerto en el que el nodo escuchará las conexiones
HOST = '192.168.1.74'
PORT = 8100

# Direccion y puerto del servidor de nodos
SERV_HOST = '192.168.1.72'
SERV_PORT = 8000

# Carpeta donde se almacenarán los archivos recibidos
DATA_FOLDER = 'data'

def receive_file(connection):
    # Recibe los datos del archivo (nombre, peso, etc.)
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

    print(f"Archivo {file_name} recibido y almacenado en {file_path}")

def start_node():
    # Crea un socket TCP
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        # Anunciando al servidor de nodos que estamos en linea:

        try:
            server_socket.connect((SERV_HOST, SERV_PORT))
            print(f"Anunciando a central en {SERV_HOST}: {SERV_PORT}")

            mensaje = "REGISTRO"
            server_socket.send(mensaje.encode())

            respuesta = server_socket.recv(1024).decode()
            print(f"Respuesta del servidor: {respuesta}")
        except ConnectionRefusedError:
            print("No se pudo establecer la conexion...")
        finally:
            server_socket.close()

        # Enlace del socket a la dirección IP y puerto especificados
        server_socket.bind((HOST, PORT))

        # Escucha las conexiones entrantes
        server_socket.listen()

        print("El nodo está listo y esperando conexiones...")

        while True:
            # Acepta la conexión entrante del cliente
            connection, address = server_socket.accept()

            print(f"Conexión establecida desde {address}")

            # Recibe el archivo
            receive_file(connection)

            # Cierra la conexión con el cliente
            connection.close()

if __name__ == '__main__':
    start_node()
