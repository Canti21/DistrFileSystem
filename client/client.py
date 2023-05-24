import os
import socket

# Direccion IP y puerto del servidor de nodos
REGISTRATION_SERVER_HOST = '192.168.1.72'
REGISTRATION_SERVER_PORT = 8000

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

def send_file(file_path):
    available_nodes = discover_nodes()

    if len(available_nodes) > 0:
        node_address = available_nodes[0]
        node_host, node_port = node_address.split(':')

        # Obtiene el nombre y el tamaño del archivo
        file_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)

        # Crea un socket TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
            # Conecta el socket al nodo destino
            client_socket.connect((node_host, 8100))

            # Envía los datos del archivo (nombre y tamaño)
            file_data = f"{file_name},{file_size}"
            client_socket.sendall(file_data.encode())

            # Lee y envía el contenido del archivo en bloques
            with open(file_path, 'rb') as file:
                for chunk in iter(lambda: file.read(1024), b''):
                    client_socket.sendall(chunk)
    else:
        print("No se encontraron nodos disponibles en el sistema.")

    print(f"Archivo {file_name} enviado correctamente al nodo.")

if __name__ == '__main__':
    file_path = 'bear.png'  # Reemplaza con la ruta y nombre del archivo que deseas enviar
    send_file(file_path)
