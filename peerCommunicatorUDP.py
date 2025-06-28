from socket  import *
from constMP import * #-
import threading
import random
import time
import pickle
from requests import get
import heapq

# Counter to make sure we have received handshakes from all other processes
handShakeCount = 0

PEERS = []
N = 0 # Number of peers, will be updated.

# --- Variáveis para Ordenação Total com Relógio de Lamport ---
lamportClock = 0
messageQueue = []  # Fila de prioridade para mensagens recebidas (timestamp, origin_id, payload)
logList = [] # Log final de mensagens entregues na ordem correta

# Sockets UDP
sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

# Socket TCP para sinal do servidor
serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

# Lock para proteger o acesso concorrente às variáveis compartilhadas (clock, fila)
lock = threading.Lock()

def get_public_ip():
  ipAddr = get('https://api.ipify.org').content.decode('utf8')
  print('Meu IP público é: {}'.format(ipAddr))
  return ipAddr

def registerWithGroupManager():
  clientSock = socket(AF_INET, SOCK_STREAM)
  print ('Conectando ao group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  ipAddr = get_public_ip()
  req = {"op":"register", "ipaddr":ipAddr, "port":PEER_UDP_PORT}
  msg = pickle.dumps(req)
  print ('Registrando no group manager: ', req)
  clientSock.send(msg)
  clientSock.close()

def getListOfPeers():
  global N
  clientSock = socket(AF_INET, SOCK_STREAM)
  print ('Conectando ao group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  req = {"op":"list"}
  msg = pickle.dumps(req)
  print ('Obtendo lista de peers do group manager: ', req)
  clientSock.send(msg)
  msg = clientSock.recv(2048)
  peers = pickle.loads(msg)
  print ('Lista de peers obtida: ', peers)
  clientSock.close()
  N = len(peers) # Atualiza o número total de peers
  return peers

class MsgHandler(threading.Thread):
  def __init__(self, sock, myself_id, num_peers):
    threading.Thread.__init__(self)
    self.sock = sock
    self.myself = myself_id
    self.num_peers = num_peers
    # Dicionário para rastrear o timestamp da última mensagem vista de cada peer
    self.last_seen_clocks = {i: -1 for i in range(num_peers)}

  def deliver_messages(self):
    """Verifica a fila e entrega mensagens que são estáveis."""
    global messageQueue, logList
    
    while messageQueue:
      # Ordena a fila por timestamp, desempatando com o ID do processo
      messageQueue.sort()
      
      # Pega a mensagem com menor timestamp
      head_timestamp, head_origin_id, head_payload = messageQueue[0]
      
      # Verifica se a mensagem no topo da fila é entregável.
      # Uma mensagem é entregável se já recebemos uma mensagem de TODOS os outros
      # processos com um timestamp maior que o da mensagem no topo.
      can_deliver = True
      for i in range(self.num_peers):
        if self.last_seen_clocks[i] < head_timestamp:
          can_deliver = False
          break
      
      if can_deliver:
        # Remove a mensagem da fila e a adiciona ao log
        msg_to_deliver = heapq.heappop(messageQueue)
        
        # O payload original está dentro da tupla
        original_sender = msg_to_deliver[2][0]
        msg_number = msg_to_deliver[2][1]
        
        print(f'ENTREGUE: Mensagem {msg_number} do processo {original_sender} com timestamp {msg_to_deliver[0]}')
        logList.append(msg_to_deliver[2])
      else:
        # Se a mensagem do topo não pode ser entregue, nenhuma outra pode.
        break

  def run(self):
    print('Handler está pronto. Aguardando mensagens...')
    
    global lamportClock, handShakeCount, messageQueue
    
    stopCount=0 
    while True:                
      msgPack = self.sock.recv(2048)
      if not msgPack:
        continue
        
      msg = pickle.loads(msgPack)

      lock.acquire()
      
      # Atualiza o relógio de Lamport
      received_clock = msg.get('clock', -1)
      lamportClock = max(lamportClock, received_clock) + 1
      
      # Atualiza o último timestamp visto para o remetente da mensagem
      sender_id = msg.get('sender_id')
      if sender_id is not None:
          self.last_seen_clocks[sender_id] = max(self.last_seen_clocks.get(sender_id, -1), received_clock)

      msg_type = msg.get('type')

      if msg_type == 'HANDSHAKE':
        handShakeCount += 1
        print(f'--- Handshake recebido de {msg["sender_id"]}. Total: {handShakeCount}')
      
      elif msg_type == 'DATA':
        # Adiciona a mensagem à fila de prioridade
        # Formato na fila: (timestamp, origin_id, payload)
        item = (msg['clock'], msg['origin_id'], msg['payload'])
        heapq.heappush(messageQueue, item)
        print(f'Na fila: Mensagem {msg["payload"][1]} de {msg["origin_id"]} com clock {msg["clock"]}')

      elif msg_type == 'STOP':
        stopCount += 1
        if stopCount == self.num_peers:
          break
      
      # Tenta entregar mensagens da fila
      self.deliver_messages()

      lock.release()

    # Finalização: garantir que todas as mensagens na fila sejam entregues
    while len(logList) < self.num_peers * N_MSGS:
        lock.acquire()
        self.deliver_messages()
        lock.release()
        time.sleep(0.1)

    # Escrever arquivo de log
    logFile = open('logfile'+str(self.myself)+'.log', 'w')
    logFile.writelines(str(logList))
    logFile.close()
    
    # Enviar a lista de mensagens para o servidor de comparação
    print('Enviando a lista de mensagens para o servidor para comparação...')
    clientSock = socket(AF_INET, SOCK_STREAM)
    clientSock.connect((SERVER_ADDR, SERVER_PORT))
    msgPack = pickle.dumps(logList)
    clientSock.send(msgPack)
    clientSock.close()
    
    exit(0)

def waitToStart():
  (conn, addr) = serverSock.accept()
  msgPack = conn.recv(1024)
  msg = pickle.loads(msgPack)
  myself_id = msg[0]
  nMsgs = msg[1]
  conn.send(pickle.dumps('Processo peer '+str(myself_id)+' iniciado.'))
  conn.close()
  return (myself_id, nMsgs)

# --- Início da Execução ---
registerWithGroupManager()
while 1:
  print('Aguardando sinal para iniciar...')
  (myself, N_MSGS) = waitToStart()
  print('Estou ativo, meu ID é: ', str(myself))

  if N_MSGS == 0:
    print('Terminando.')
    exit(0)
  
  PEERS = getListOfPeers()
  
  # Cria e inicia o handler de mensagens
  msgHandler = MsgHandler(recvSocket, myself, N)
  msgHandler.start()
  print('Handler iniciado')

  time.sleep(2) # Pausa para garantir que todos os handlers estejam prontos

  # Envia handshakes
  lock.acquire()
  lamportClock += 1
  msg = {'type': 'HANDSHAKE', 'sender_id': myself, 'clock': lamportClock}
  msgPack = pickle.dumps(msg)
  for addrToSend in PEERS:
    print('Enviando handshake para ', addrToSend)
    sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
  lock.release()

  # Espera por todos os handshakes
  print('Thread Principal: Enviei todos os handshakes.')
  while (handShakeCount < N):
    time.sleep(0.5)

  print("Todos os peers estão prontos. Começando o envio de mensagens de dados.")

  # Envia uma sequência de mensagens de dados para todos os outros processos
  for msgNumber in range(0, N_MSGS):
    time.sleep(random.randrange(10,100)/1000)
    
    lock.acquire()
    lamportClock += 1
    # Mensagem com tipo, payload, clock e ID do remetente original
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

  # Avisa a todos os processos que não tem mais mensagens para enviar
  print("Terminei de enviar mensagens. Enviando sinal de parada.")
  lock.acquire()
  lamportClock += 1
  msg = {'type': 'STOP', 'sender_id': myself, 'clock': lamportClock}
  msgPack = pickle.dumps(msg)
  lock.release()
  
  for addrToSend in PEERS:
    sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
