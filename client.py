import socket
import threading
import uuid
import json

# Konstanten Definition
BROADCAST_IP = '192.168.178.255'
BROADCAST_PORT = 10010
NEW_LEADER_PORT = 10020
SERVER_ELECTION_PORT = 10030
HEARTBEAT_SEND_PORT = 10040
HEARTBEAT_LISTEN_PORT = 10050

# Identifier für Broadcast-Nachrichten
BROADCAST_SEND_ID = '8c2d6619-6b05-4567-aca9-9ddd4ee76876'
BROADCAST_RESPONSE_ID = 'df147dc4-f2b0-4df7-84c8-967f46d4377c'

BUFFER_SIZE = 4096

# Anzahl der Versuche, die ein Server beim Start unternimmt, um andere Server zu entdecken
SERVER_DISCOVERY_ATTEMPTS = 5
# Anzahl der Herzschlag-Versuche, bevor ein Server eine Wahl startet
HEARTBEAT_ATTEMPTS = 3
HEARTBEAT_PRINT_INTERVAL = 10
HEARTBEAT_MISSES_MAX = 3

# Hilfsfunktionen
def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP

def create_broadcast_socket(timeout=None):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    s.bind((get_local_ip(), 0))
    if timeout:
        s.settimeout(timeout)
    return s

def create_heartbeat_send_socket(timeout=None):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((get_local_ip(), 0))
    if timeout:
        s.settimeout(timeout)
    return s

def create_heartbeat_listen_socket(port, timeout=None):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('', port))
    if timeout:
        s.settimeout(timeout)
    return s

def create_tcp_socket():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((get_local_ip(), 0))
    s.listen()
    return s

def create_tcp_socket_with_port(port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((get_local_ip(), port))
    s.listen()
    return s

def send_tcp_message(address, message):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(1)
    s.connect(address)
    s.send(message)
    s.close()

def encode_message(node_type, sender_address, content):
    message_dict = {'node_type': node_type, 'sender': sender_address, 'content': content}
    return json.dumps(message_dict).encode()

def encode_heartbeat_message(node_type, sender_address, content, server_list, client_list):
    message_dict = {'node_type': node_type, 'sender': sender_address, 'content': content, 'server_list': server_list, 'client_list': client_list}
    return json.dumps(message_dict).encode()

def decode_message(message):
    return json.loads(message.decode())

# Variablen zur Speicherung der Serveradresse und des Client-Namens
server_address = None

# Variable, um dem Client zu ermöglichen, den Chatraum zu verlassen
is_active = True

# Variable, um die Suche nach einem Server zu stoppen
is_searching = True

# Erstelle TCP-Sockets
client_socket = create_tcp_socket()
client_address = client_socket.getsockname()

# Definiere Client-ID und -Informationen
client_id = str(uuid.uuid4())
client_info = {'client_id': client_id, 'client_address': client_address}

def main():
    discover_server()
    threading.Thread(target=chat).start()
    threading.Thread(target=tcp_listener).start()

# Broadcast für dynamische Entdeckung
def discover_server():
    broadcast_socket = create_broadcast_socket(timeout=2)

    while is_searching:
        message = {'sending_id': BROADCAST_SEND_ID, 'node_type': 'client', 'sender': client_info}
        broadcast_socket.sendto(json.dumps(message).encode(), (BROADCAST_IP, BROADCAST_PORT))
        print("Versuche, eine Verbindung zum Server herzustellen...")
        try:
            data, address = broadcast_socket.recvfrom(BUFFER_SIZE)
        except TimeoutError:
            pass
        else:
            response = decode_message(data)
            if 'response_id' in response and response['response_id'] == BROADCAST_RESPONSE_ID:
                set_server = response
                set_server_address(response.get('server'))
                print(f'Server gefunden bei {server_address["server_address"][0]}:{server_address["server_address"][1]}')
                break

    broadcast_socket.close()
    try:
        send_message_to_server('#Joining#')
    except (ConnectionRefusedError, TimeoutError):
        print('Fehler beim Beitreten zum Chat! \r Erneuter Versuch...')
    else:
        print('Chat beigetreten!')

# Funktion zum Empfangen von TCP-Nachrichten vom Server
def tcp_listener():
    client_socket.settimeout(2)
    while is_active:
        try:
            client, address = client_socket.accept()
            if client:
                message = decode_message(client.recv(BUFFER_SIZE))
                sender = tuple(message.get('content').get('sender').get('client_address'))
                content = message.get('content').get('content')
                print(f'\r{sender[0]}: {content}')
        except TimeoutError:
            continue

    client_socket.close()

# Setzt die Serveradresse, an die Nachrichten gesendet werden
def set_server_address(address: tuple):
    global server_address
    server_address = address

# Chat-Nachrichten an den Server senden
def chat():
    while is_active:
        message_content = input('\r')

        print("\033[A\033[A")

        if len(message_content) > BUFFER_SIZE / 10:
            print('Nachricht ist zu lang')
        elif len(message_content) == 0:
            continue
        elif '/leave' in message_content:
            set_is_active(False)
            send_message_to_server('#Leaving#')
        else:
            send_message_to_server(message_content)

def send_message_to_server(message_content):
    try:
        send_tcp_message(tuple(server_address['server_address']), encode_message('client', client_info, message_content))
    except (ConnectionRefusedError, TimeoutError):
        print('\rFehler beim Senden der Nachricht, erneuter Verbindungsversuch zum Server...')
        discover_server()

def set_is_active(active):
    global is_active
    is_active = active

if __name__ == '__main__':
    main()
