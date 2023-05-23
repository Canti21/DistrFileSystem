import socket

# Lista de nodos disponibles en el sistema
nodos_disponibles = []

def procesar_mensaje(mensaje, cliente):
    global nodos_disponibles

    if mensaje == "REGISTRO":
        # Registrar nuevo nodo
        nodos_disponibles.append(cliente)
        print(f"Nodo registrado: {cliente}")
    elif mensaje == "DESCUBRIR":
        # Enviar lista de nodos disponibles al cliente
        respuesta = ",".join(nodos_disponibles)
        cliente.send(respuesta.encode())
    else:
        # Otra lógica de procesamiento de mensajes según tu caso de uso
        pass

def main():
    # Configuración del servidor
    host = '192.168.1.75'
    port = 8000

    # Crear socket del servidor
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.bind((host, port))
    servidor.listen(10)

    print(f"Servidor central escuchando en {host}:{port}")

    while True:
        # Esperar conexiones entrantes
        cliente, direccion = servidor.accept()
        print(f"Conexión establecida desde {direccion[0]}:{direccion[1]}")

        # Recibir mensaje del cliente
        mensaje = cliente.recv(1024).decode()
        print(f"Mensaje recibido: {mensaje}")

        # Procesar el mensaje recibido
        procesar_mensaje(mensaje, cliente)

        # Cerrar la conexión con el cliente
        cliente.close()

if __name__ == '__main__':
    main()
