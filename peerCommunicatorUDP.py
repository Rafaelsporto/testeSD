from socket  import *
from constMP import * #-
import threading
import random
import time
import pickle
from requests import get
import heapq

# Contador para handshakes
handShakeCount = 0

PEERS = []
N = 0 # Número de peers
N_MSGS = 0 # Número de mensagens por peer

# --- Variáveis para Ordenação Total com Relógio de Lamport ---
lamportClock = 0
messageQueue = []  # Fila de prioridade para mensagens (timestamp, origin_id, payload)
logList = [] # Log final de mensagens entregues

# Sockets
sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

lock = threading.Lock()

def get_public_ip():
  ipAddr = get('https://api.ipify.org').content.decode('utf8')
  print(f'Meu IP público é: {ipAddr}')
  return ipAddr

def registerWithGroupManager():
  clientSock = socket(AF_INET, SOCK_STREAM)
  clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  ipAddr = get_public_ip()
  req = {"op":"register", "ipaddr":ipAddr, "port":PEER_UDP_PORT}
  msg = pickle.dumps(req)
  clientSock.send(msg)
  clientSock.close()

def getListOfPeers():
  global N
  clientSock = socket(AF_INET, SOCK_STREAM)
  clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  req = {"op":"list"}
  msg = pickle.dumps(req)
  clientSock.send(msg)
  msg = clientSock.recv(2048)
  peers = pickle.loads(msg)
  clientSock.close()
  N = len(peers)
  return peers

class MsgHandler(threading.Thread):
  def __init__(self, sock, myself_id, num_peers):
    threading.Thread.__init__(self)
    self.sock = sock
    self.myself = myself_id
    self.num_peers = num_peers
    self.last_seen_clocks = {i: -1 for i in range(num_peers)}
    # NOVO: Conjunto para rastrear peers que já enviaram 'STOP'
    self.stopped_peers = set()

  def deliver_messages(self):
    """
    Verifica a fila e entrega mensagens que são estáveis.
    A lógica de entrega foi aprimorada para evitar deadlocks no final.
    """
    global messageQueue, logList
    
    while messageQueue:
      messageQueue.sort()
      
      head_timestamp, head_origin_id, head_payload = messageQueue[0]
      
      can_deliver = True
      for i in range(self.num_peers):
        # LÓGICA CORRIGIDA: Se um peer já parou, não precisamos mais
        # esperar por uma mensagem dele para liberar a mensagem atual.
        if i in self.stopped_peers:
          continue
        
        if self.last_seen_clocks[i] < head_timestamp:
          can_deliver = False
          break
      
      if can_deliver:
        msg_to_deliver = heapq.heappop(messageQueue)
        original_sender = msg_to_deliver[2][0]
        msg_number = msg_to_deliver[2][1]
        
        print(f'ENTREGUE: Mensagem {msg_number} de {original_sender} com timestamp {msg_to_deliver[0]}')
        logList.append(msg_to_deliver[2])
      else:
        break

  def run(self):
    print('Handler está pronto. Aguardando mensagens...')
    
    global lamportClock, handShakeCount, messageQueue
    
    stop_signals_received = 0
    
    # Loop principal para receber mensagens
    while stop_signals_received < self.num_peers:
      try:
        msgPack, addr = self.sock.recvfrom(4096)
        if not msgPack: continue
          
        msg = pickle.loads(msgPack)

        lock.acquire()
        
        received_clock = msg.get('clock', -1)
        lamportClock = max(lamportClock, received_clock) + 1
        
        sender_id = msg.get('sender_id')
        if sender_id is not None:
          self.last_seen_clocks[sender_id] = max(self.last_seen_clocks.get(sender_id, -1), received_clock)

        msg_type = msg.get('type')

        if msg_type == 'HANDSHAKE':
          handShakeCount += 1
          print(f'--- Handshake recebido de {msg["sender_id"]}. Total: {handShakeCount}')
        
        elif msg_type == 'DATA':
          item = (msg['clock'], msg['origin_id'], msg['payload'])
          heapq.heappush(messageQueue, item)
          #print(f'Na fila: Mensagem {msg["payload"][1]} de {msg["origin_id"]} com clock {msg["clock"]}')

        elif msg_type == 'STOP':
          if sender_id not in self.stopped_peers:
            self.stopped_peers.add(sender_id)
            stop_signals_received += 1
            print(f'Sinal de STOP recebido de {sender_id}. Total: {stop_signals_received}/{self.num_peers}')
        
        self.deliver_messages()

        lock.release()

      except Exception as e:
        print(f"Erro no handler: {e}")
        lock.release()
        break

    print("Recebi todos os sinais de STOP. Processando mensagens restantes na fila...")

    # LÓGICA DE FINALIZAÇÃO: Garante que a fila seja esvaziada
    expected_total_msgs = self.num_peers * N_MSGS
    while len(logList) < expected_total_msgs:
        lock.acquire()
        self.deliver_messages()
        lock.release()
        time.sleep(0.05) # Evita busy-waiting

    print("Todas as mensagens foram entregues. Encerrando o handler.")
    
    logFile = open(f'logfile{self.myself}.log', 'w')
    logFile.writelines(str(logList))
    logFile.close()
    
    print('Enviando a lista de mensagens para o servidor para comparação...')
    clientSock = socket(AF_INET, SOCK_STREAM)
    clientSock.connect((SERVER_ADDR, SERVER_PORT))
    msgPack = pickle.dumps(logList)
    clientSock.send(msgPack)
    clientSock.close()
    
    exit(0)

def waitToStart():
  global N_MSGS
  (conn, addr) = serverSock.accept()
  msgPack = conn.recv(1024)
  msg = pickle.loads(msgPack)
  myself_id, N_MSGS = msg[0], msg[1]
  conn.send(pickle.dumps(f'Processo peer {myself_id} iniciado.'))
  conn.close()
  return (myself_id, N_MSGS)

# --- Início da Execução ---
registerWithGroupManager()
while 1:
  print('Aguardando sinal para iniciar...')
  (myself, N_MSGS_local) = waitToStart()
  N_MSGS = N_MSGS_local
  print(f'Estou ativo, meu ID é: {myself}')

  if N_MSGS == 0:
    print('Terminando.')
    exit(0)
  
  PEERS = getListOfPeers()
  
  msgHandler = MsgHandler(recvSocket, myself, N)
  msgHandler.start()
  
  time.sleep(2)

  lock.acquire()
  lamportClock += 1
  msg = {'type': 'HANDSHAKE', 'sender_id': myself, 'clock': lamportClock}
  msgPack = pickle.dumps(msg)
  lock.release()
  for addrToSend in PEERS:
    sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
    
  while (handShakeCount < N):
    time.sleep(0.5)

  print("Todos os peers estão prontos. Começando o envio de mensagens.")

  for msgNumber in range(0, N_MSGS):
    time.sleep(random.randrange(10,100)/1000)
    
    lock.acquire()
    lamportClock += 1
    msg = {
        'type': 'DATA',
        'payload': (myself, msgNumber),
        'clock': lamportClock,
        'origin_id': myself,
        'sender_id': myself
    }
    msgPack = pickle.dumps(msg)
    lock.release()
    
    for addrToSend in PEERS:
      sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
    print(f'Enviei mensagem {msgNumber} com clock {msg["clock"]}')

  print("Terminei de enviar. Enviando sinal de parada.")
  lock.acquire()
  lamportClock += 1
  msg = {'type': 'STOP', 'sender_id': myself, 'clock': lamportClock}
  msgPack = pickle.dumps(msg)
  lock.release()
  
  for addrToSend in PEERS:
    sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
