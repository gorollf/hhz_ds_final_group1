import socket
import uuid
import threading
import json
import time

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

# Erstelle TCP-Kommunikationssockets
server_socket = create_tcp_socket()
server_address = server_socket.getsockname()

# Erstelle Wahl-Sockets
election_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
election_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
election_socket.bind((get_local_ip(), SERVER_ELECTION_PORT))

# Variablen für Systemübersicht
server_id = str(uuid.uuid4())
server_info = {'server_id': server_id, 'server_address': server_address}
clients = []
servers = []

# Flag, um das Stoppen des Servers zu ermöglichen (um "while True" zu vermeiden)
is_running = True

# Globale Führungsvariablen
is_leader = False
leader_info = None
is_participant = False

# Hauptfunktion zum Starten mehrerer Threads
def main():
    start_thread(broadcast_listener, True)
    start_thread(tcp_listener, False)
    start_thread(leader_election, True)

    discover_servers()

    start_thread(heartbeat_listener, True)
    start_thread(heartbeat_sender, True)

def start_thread(target, daemon):
    t = threading.Thread(target=target)
    t.daemon = daemon
    t.start()

def discover_servers():
    broadcast_socket = create_broadcast_socket(timeout=1)
    got_response = False

    for _ in range(SERVER_DISCOVERY_ATTEMPTS):
        message = {'sending_id': BROADCAST_SEND_ID, 'node_type': 'server', 'sender': server_info}
        broadcast_socket.sendto(json.dumps(message).encode(), (BROADCAST_IP, BROADCAST_PORT))
        print("Suche nach anderen Servern...")
        try:
            data, address = broadcast_socket.recvfrom(BUFFER_SIZE)
            response = decode_message(data)
        except TimeoutError:
            pass
        else:
            if 'response_id' in response and address[0] != server_address[0] and response['response_id'] == BROADCAST_RESPONSE_ID:
                print(f'Server gefunden bei {response["server"]["server_address"][0]}:{response["server"]["server_address"][1]}')
                reset_server_list(response['server_list'])
                print(f'Aktualisierte Serverliste: {servers}')
                try:
                    send_message_to_server('#Joining#', (response['server']['server_address'][0], response['server']['server_address'][1]))
                except (ConnectionRefusedError, TimeoutError):
                    print('Fehler beim Beitreten zu den Servern! \r')
                else:
                    print('Servern beigetreten!')
                    neighbour = get_neighbour(servers, server_info, 'left')
                    print('Neue Führungswahl gestartet')
                    start_leader_election(server_info, neighbour)
                    got_response = True
                    break
    broadcast_socket.close()
    if not got_response:
        print('Keine anderen Server gefunden, setze mich als Führer')
        set_as_leader(True)
        set_leader_info(server_info)
        append_to_server_list(server_info)

def broadcast_listener():
    print(f'Server läuft bei {server_address} und hört auf Port {BROADCAST_PORT}')

    listener_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    listener_socket.bind(('', BROADCAST_PORT))
    listener_socket.settimeout(2)

    while is_running:
        try:
            data, address = listener_socket.recvfrom(BUFFER_SIZE)
        except TimeoutError:
            pass
        else:
            message = decode_message(data)
            if is_leader and 'sending_id' in message and address != server_address and message['sending_id'] == BROADCAST_SEND_ID:
                print(f'Broadcast von {address[0]} erhalten, antworte mit Antwort-ID, IP und Port')
                if message['node_type'] == 'server':
                    append_to_server_list(message['sender'])
                response = {'response_id': BROADCAST_RESPONSE_ID, 'server': server_info, 'server_list': servers}
                listener_socket.sendto(json.dumps(response).encode(), address)
    print('Broadcast Listener schließt')
    listener_socket.close()

def heartbeat_sender():
    i = 0
    while is_running:
        heartbeat_send_socket = create_heartbeat_send_socket()
        if is_leader:
            message = encode_heartbeat_message('server', server_info, 'for heartbeat', servers, clients)
            heartbeat_target = (BROADCAST_IP, HEARTBEAT_LISTEN_PORT)
            if i % HEARTBEAT_PRINT_INTERVAL == 0:
                print(f'Heartbeat gesendet an {heartbeat_target} {i} Mal')
            time.sleep(1)
            heartbeat_send_socket.sendto(message, heartbeat_target)
            i += 1
        heartbeat_send_socket.close()

def heartbeat_listener():
    f = 0
    while is_running:
        heartbeat_listen_socket = create_heartbeat_listen_socket(HEARTBEAT_LISTEN_PORT, timeout=3)
        if not is_leader:
            missed_heartbeats = 0
            for _ in range(HEARTBEAT_ATTEMPTS):
                try:
                    data, address = heartbeat_listen_socket.recvfrom(BUFFER_SIZE)
                    if data:
                        f += 1
                        missed_heartbeats = 0
                        message = decode_message(data)
                        reset_server_list(message['server_list'])
                        reset_client_list(message['client_list'])
                        set_leader_info(message['sender'])
                        if f % HEARTBEAT_PRINT_INTERVAL == 0:
                            print(f'Heartbeat empfangen {f} Mal')
                except TimeoutError:
                    missed_heartbeats += 1
                    if missed_heartbeats == HEARTBEAT_MISSES_MAX:
                        print(f'{missed_heartbeats} Heartbeats vom Führer verpasst! Starte Wahl!')
                        try:
                            remove_from_server_list(leader_info)
                        except:
                            pass
                        neighbour = get_neighbour(servers, server_info, 'left')
                        start_leader_election(server_info, neighbour)
                        time.sleep(3)
                        break
        heartbeat_listen_socket.close()

def send_message_to_server(message_content, target_address):
    try:
        send_tcp_message(target_address, encode_message('server', server_info, message_content))
    except (ConnectionRefusedError, TimeoutError):
        print('\rFehler beim Senden der Nachricht, Server ist nicht erreichbar')

def leader_election():
    while True:
        data, address = election_socket.recvfrom(BUFFER_SIZE)
        if data:
            neighbour = get_neighbour(servers, server_info, 'left')
            neighbour = (neighbour['server_address'][0], SERVER_ELECTION_PORT)
            election_message = json.loads(data.decode())
            mid = election_message['mid'].get('server_id')

            if mid > server_id:
                election_socket.sendto(json.dumps(election_message).encode(), neighbour)
            elif mid == server_id and is_participant:
                new_election_message = {"mid": server_info, "isLeader": True}
                election_socket.sendto(json.dumps(new_election_message).encode(), neighbour)
            elif mid < server_id and not is_participant:
                new_election_message = {"mid": server_info, "isLeader": False}
                election_socket.sendto(json.dumps(new_election_message).encode(), neighbour)
            set_participant(True)

            if election_message['isLeader']:
                set_leader_info(election_message['mid'])
                if server_id != mid:
                    set_as_leader(False)
                    election_socket.sendto(json.dumps(election_message).encode(), neighbour)
                else:
                    set_as_leader(True)
                set_participant(False)

def get_neighbour(ring, current_node, direction):
    current_index = next((index for (index, d) in enumerate(ring) if d["server_id"] == current_node["server_id"]), None)
    if current_index != -1:
        if direction == 'left':
            if current_index + 1 == len(ring):
                return ring[0]
            else:
                return ring[current_index + 1]
        else:
            if current_index == 0:
                return ring[len(ring) - 1]
            else:
                return ring[current_index - 1]
    else:
        return None

def start_leader_election(server_info, neighbour_info):
    election_message = {"mid": server_info, "isLeader": False}
    neighbour = (neighbour_info['server_address'][0], SERVER_ELECTION_PORT)
    election_socket.sendto(json.dumps(election_message).encode(), neighbour)
    set_participant(True)
    print(str(server_info) + " ist der Führer")

def message_all_servers(message):
    print(f'Server: {servers}')
    for server in servers:
        try:
            send_tcp_message(tuple(server['server_address']), message)
        except (ConnectionRefusedError, TimeoutError):
            print(f'Kann nicht an {server} senden')
            clients.remove(server)

def reset_server_list(server_list):
    global servers
    servers = server_list

def set_participant(p):
    global is_participant
    is_participant = p

def remove_from_server_list(server_info):
    global servers
    servers.remove(server_info)

def reset_client_list(client_list):
    global clients
    clients = client_list

def append_to_server_list(server_info):
    global servers
    servers.append(server_info)

def append_to_client_list(client_info):
    global clients
    clients.append(client_info)

def get_server_ip():
    global server_info
    return server_info['server_address'][0]

def tcp_listener():
    server_socket.settimeout(2)
    while is_running:
        try:
            client, address = server_socket.accept()
            message = decode_message(client.recv(BUFFER_SIZE))
            print(message)
            if message['content'] == '#Joining#':
                match message['node_type']:
                    case 'client':
                        if not next((index for (index, d) in enumerate(clients) if d == message['sender']), False):
                            append_to_client_list(message['sender'])
                        message_all_clients(encode_message('server', server_info, message))
                        print('Aktualisierte Client-Liste: \r')
                        print(clients)
                    case 'server':
                        pass
                        servers.append(message['sender'])
                        print('Aktualisierte Server-Liste\r')
                        print(servers)
            elif message['content'] == '#Leaving#':
                message_all_clients(encode_message('server', server_info, message))
            else:
                message_all_clients(encode_message('server', server_info, message))
        except TimeoutError:
            continue

    print('TCP Listener schließt')
    server_socket.close()

def message_all_clients(message):
    print(f'Clients: {clients}')
    for client in clients:
        try:
            send_tcp_message(tuple(client['client_address']), message)
        except (ConnectionRefusedError, TimeoutError):
            print(f'Kann nicht an {client} senden')
            clients.remove(client)

def set_as_leader(is_leader_flag):
    global is_leader
    is_leader = is_leader_flag

def set_leader_info(leader_info_param):
    global leader_info
    leader_info = leader_info_param

if __name__ == '__main__':
    main()
