import os
import socket
from tkinter import Tk, Button, filedialog, Entry, Toplevel
from tkinter.messagebox import showinfo

# Dirección IP y puerto del servidor de nodos
REGISTRATION_SERVER_HOST = '192.168.1.72'
REGISTRATION_SERVER_PORT = 8000

# Carpeta donde se almacenarán los archivos recibidos
DATA_FOLDER = 'data_client'

# Ruta global
PATH = None

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
    i = 0
    while i < 3:
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
                                file_upload_success()
                            return

                    except ConnectionRefusedError:
                        print(f"No se pudo conectar al nodo {node_address}. Intentando con otro nodo...")
                    except ConnectionResetError:
                        print(f"Hubo un error de conexion...")

        print("No se encontraron nodos disponibles en el sistema o todos los nodos estaban inaccesibles.")
        i = i + 1

    file_upload_error()

def receive_file(file_name):
    available_nodes = discover_nodes()
    for node in available_nodes:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
            try:
                # Conecta el socket al nodo destino
                client_socket.connect((node, 8100))

                # Envía el comando al nodo
                command = "DESCARGAR"
                client_socket.sendall(command.encode())

                client_socket.sendall(file_name.encode())

                existe = client_socket.recv(1024).decode()
                print(f"HELOOOOO: {existe}")

                if existe == "EXISTE":
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
                else:
                    file_404()

            except ConnectionRefusedError:
                print(f"No se pudo conectar al nodo {node}. Intentando con otro nodo...")
            except ConnectionResetError:
                print(f"Hubo un error de conexion...")

def file_upload_success():
    pop = Tk()
    pop.withdraw()

    showinfo("Éxito", "Archivo subido con éxito")

    pop.destroy()

def file_upload_error():
    pop = Tk()
    pop.withdraw()

    showinfo("Error", "Hubo un problema al subir el archivo")

    pop.destroy()

def file_404():
    pop = Tk()
    pop.withdraw()

    showinfo("Error", "El archivo no existe en el sistema")

    pop.destroy()

def file_download_error():
    pop = Tk()
    pop.withdraw()

    showinfo("Error", "Hubo un problema al descargar el archivo")

    pop.destroy()


def execute(operation, file_path):
    if operation == "SUBIR":
        if os.path.exists(file_path):
            send_file(file_path)
        else:
            print("El archivo especificado no existe.")

    elif operation == "DESCARGAR":
        
        receive_file(file_path)

    else:
        print("Operación no válida. Inténtalo nuevamente.")

def select_file():
    global PATH
    file = filedialog.askopenfilename()
    PATH = file

def verify_path():
    global PATH
    if PATH is not None:
        print("Ejecutando")
        execute("SUBIR", PATH)

def get_content():
    content = textbox.get()
    if content is not None:
        execute("DESCARGAR", content)

if __name__ == '__main__':
    # Crea la ventana principal
    ventana = Tk()

    # Crea el botón de selección de archivo
    fileButton = Button(ventana, text="Seleccionar archivo", command=select_file)
    fileButton.pack()

    uploadButton = Button(ventana, text="Subir", command=verify_path)
    uploadButton.pack()

    textbox = Entry(ventana)
    textbox.pack()

    fetchButton = Button(ventana, text="Obtener", command=get_content)
    fetchButton.pack()

    # Ejecuta el bucle principal de la ventana
    ventana.mainloop()