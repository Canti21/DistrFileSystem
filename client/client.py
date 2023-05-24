import os
import socket

# Dirección IP y puerto del servidor de nodos
REGISTRATION_SERVER_HOST = '192.168.1.72'
REGISTRATION_SERVER_PORT = 8000

# Carpeta donde se almacenarán los archivos recibidos
DATA_FOLDER = 'data_client'

def discover_nodes():
    # Crea un socket TCP para conectarse al servidor de registro y descubrimiento
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        # Conecta el socket al servidor
        server_socket.connect((REGISTRATION_SERVER_HOST, REGISTRATION_SERVER_PORT))

        # Envía el mensaje de descubrimiento al servidor
        server_socket.sendall("DESCUBRIR".encode())

        # Recibe la respuesta del servidor
        respuesta = server_socket.recv(1024).decode()

        # Cierra la conexión con el servidor
        server_socket.close()

    # Retorna la lista de nodos disponibles
    return respuesta.split(',')

def receive_file(file_name, file_size, connection):
    # Crea el directorio "data_client" si no existe
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)

    # Ruta completa del archivo en la carpeta "data_client"
    file_path = os.path.join(DATA_FOLDER, file_name)

    # Recibe y almacena el archivo en la carpeta "data_client"
    with open(file_path, 'wb') as file:
        remaining_bytes = int(file_size)
        while remaining_bytes > 0:
            chunk = connection.recv(1024)
            file.write(chunk)
            remaining_bytes -= len(chunk)

    print(f"Archivo {file_name} recibido y almacenado en {file_path}")

def send_file(file_path):
    available_nodes = discover_nodes()

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
                            print(f"Archivo {file_name} enviado correctamente al nodo {node_address}")
                        else:
                            send_file(file_path)
                        return

                except ConnectionRefusedError:
                    print(f"No se pudo conectar al nodo {node_address}. Intentando con otro nodo...")

    print("No se encontraron nodos disponibles en el sistema o todos los nodos estaban inaccesibles.")

def start_client():
    while True:
        # Solicitar operación al usuario (SUBIR o DESCARGAR o RECIBIR)
        #operation = input("Ingresa la operación que deseas realizar (SUBIR, DESCARGAR o RECIBIR): ")
        operation = "SUBIR"

        if operation == "SUBIR":
            # Solicitar ruta del archivo al usuario
            file_path = input("Ingresa la ruta completa del archivo que deseas subir: ")

            if os.path.exists(file_path):
                send_file(file_path)
            else:
                print("El archivo especificado no existe.")

        elif operation == "DESCARGAR":
            # Solicitar nombre del archivo al usuario
            file_name = input("Ingresa el nombre del archivo que deseas descargar: ")

            # TODO: Agregar lógica para descargar el archivo del nodo correspondiente

        elif operation == "RECIBIR":
            # Solicitar nombre del archivo al usuario
            file_name = input("Ingresa el nombre del archivo que deseas recibir: ")

            available_nodes = discover_nodes()

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

                            # Envía el nombre del archivo al nodo
                            client_socket.sendall(file_name.encode())

                            # Recibe la respuesta del nodo
                            response = client_socket.recv(1024).decode()

                            if response == "EXISTE":
                                # Recibe el archivo del nodo
                                file_size = client_socket.recv(1024).decode()
                                receive_file(file_name, file_size, client_socket)

                                print(f"Archivo {file_name} recibido correctamente.")
                                return
                            elif response == "NO_EXISTE":
                                print(f"El archivo {file_name} no existe en el sistema.")
                                break

                        except ConnectionRefusedError:
                            print(f"No se pudo conectar al nodo {node_address}. Intentando con otro nodo...")

            print("No se encontraron nodos disponibles en el sistema o todos los nodos estaban inaccesibles.")

        else:
            print("Operación no válida. Inténtalo nuevamente.")

if __name__ == '__main__':
    start_client()
