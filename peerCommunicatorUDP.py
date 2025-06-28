from socket  import *
from constMP import * #-
import threading
import random
import time
import pickle
from requests import get
import heapq

# --- Variáveis Globais ---
handShakeCount = 0
PEERS = []
N = 0 
N_MSGS = 0 
lock = threading.Lock()

# --- Variáveis para Ordenação Total com Relógio de Lamport ---
lamportClock = 0
messageQueue = []  # Fila de prioridade para mensagens (timestamp, origin_id, payload)
logList = [] # Log final de mensagens entregues para verificação

# --- Sockets ---
sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))
serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

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
    self.stopped_peers = set()

  def deliver_messages(self):
    """
    Verifica a fila e entrega mensagens estáveis, agora com a lógica de chat integrada.
    """
    global messageQueue, logList
    
    while messageQueue:
      messageQueue.sort()
      head_timestamp, head_origin_id, head_payload = messageQueue[0]
      
      can_deliver = True
      for i in range(self.num_peers):
        if i in self.stopped_peers:
          continue
        if self.last_seen_clocks[i] < head_timestamp:
          can_deliver = False
          break
      
      if can_deliver:
        msg_to_deliver = heapq.heappop(messageQueue)
        
        # Extrai os dados da mensagem para o log e para o chat
        timestamp, origin_id, payload = msg_to_deliver
        peer_id = payload[0]
        msg_topic_id = payload[1]

        # --- LÓGICA DE CHAT (RESPOSTA) ---
        # A resposta é gerada no momento da ENTREGA, garantindo a ordem.
        conversation_replies = {
            0: ["Olá, Peer {peer_id}! Tudo bem por aqui.", "E aí, {peer_id}! Recebido. A começar os trabalhos."],
            1: ["Hmm, que pergunta interessante, {peer_id}. Vou pensar a respeito.", "A propósito, {peer_id}, viste as últimas notícias?"],
            2: ["Hahaha, essa foi boa, {peer_id}!", "{peer_id}, sempre com as melhores piadas."]
        }
        default_replies = ["Interessante o que dizes, {peer_id}.", "Entendido, {peer_id}. Próximo!"]
        replies_list = conversation_replies.get(msg_topic_id, default_replies)
        selected_reply = random.choice(replies_list)
        formatted_reply = selected_reply.format(peer_id=peer_id)
        
        # Imprime a "conversa" ordenada
        print(f"[Peer {self.myself}] ouviu do Peer {peer_id} (Entregue): \"{formatted_reply}\" (Tópico #{msg_topic_id}, Clock: {timestamp})")
        
        # Adiciona a mensagem original ao log para verificação do servidor
        logList.append(payload)
      else:
        break

  def run(self):
    print('Handler está pronto. Aguardando mensagens...')
    global lamportClock, handShakeCount
    stop_signals_received = 0
    
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
        elif msg_type == 'DATA':
          item = (msg['clock'], msg['origin_id'], msg['payload'])
          heapq.heappush(messageQueue, item)
        elif msg_type == 'STOP':
          if sender_id not in self.stopped_peers:
            self.stopped_peers.add(sender_id)
            stop_signals_received += 1
        
        self.deliver_messages()
        lock.release()
      except Exception:
        lock.release()
        break

    print("Recebi todos os sinais de STOP. Processando mensagens restantes na fila...")
    expected_total_msgs = self.num_peers * N_MSGS
    while len(logList) < expected_total_msgs:
        lock.acquire()
        self.deliver_messages()
        lock.release()
        time.sleep(0.05)

    print("Todas as mensagens foram entregues. Encerrando o handler.")
    logFile = open(f'logfile{self.myself}.log', 'w')
    logFile.writelines(str(logList))
    logFile.close()
    
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

# --- Bloco Principal de Execução ---
registerWithGroupManager()
while 1:
  print('Aguardando sinal para iniciar...')
  (myself, N_MSGS_local) = waitToStart()
  N_MSGS = N_MSGS_local
  if N_MSGS == 0:
    print('Terminando.')
    break
  
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

  print("--- Todos os peers estão prontos. Começando a simulação de chat. ---")

  # NOVA FUNCIONALIDADE: Frases para iniciar a conversa
  conversation_starters = [
    "Olá a todos! Alguém na escuta?",
    "Vou lançar uma pergunta no ar para reflexão.",
    "Alguém aí conhece uma boa piada?",
    "Que tal falarmos sobre o tempo?",
    "A iniciar uma nova ronda de discussões."
  ]

  for msgNumber in range(0, N_MSGS):
    time.sleep(random.randrange(10,100)/1000)
    lock.acquire()
    lamportClock += 1
    msg = {'type': 'DATA', 'payload': (myself, msgNumber), 'clock': lamportClock, 'origin_id': myself, 'sender_id': myself}
    msgPack = pickle.dumps(msg)
    lock.release()
    
    # LÓGICA DE CHAT (ENVIO)
    starter_index = msgNumber % len(conversation_starters)
    starter_phrase = conversation_starters[starter_index]

    for addrToSend in PEERS:
      sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
    print(f"[Peer {myself}] enviou: \"{starter_phrase}\" (Tópico #{msgNumber}) com clock {msg['clock']}")

  lock.acquire()
  lamportClock += 1
  msg = {'type': 'STOP', 'sender_id': myself, 'clock': lamportClock}
  msgPack = pickle.dumps(msg)
  lock.release()
  for addrToSend in PEERS:
    sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
