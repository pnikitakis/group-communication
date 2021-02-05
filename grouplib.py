#2h ergasia. Nikhtakhs Panagioths(1717), Rantou Kalliopi(2004)
import socket
import threading
import struct
import queue
import time
import os

dirIP = ''		#arxikopoihshs timwn
dirPort = 0

grpName = []		#pinakas apothiekyshs onomatos omadas
multicastIP = []	#pinakas apothikeushs multicast ip
multicastPort = []	#pinakas apothikeushs multicast port
myID = []			#pinakas apothikeyshs id-appname kathe efarmoghs
grpMembers = []		#pinakas apothikeshs melwn omadas
sequencerArray = []	#pinakas apothikeushs timwn sequencer ths kathe efarmoghs
qMes = []			#oura apothikeushs mhnumatwn tou sender
qSeq = []			#oura apothikeyshs mhnumatwn tou sequencer
seqNo = []		#id paketou apo ton sender
messageBuf = []		#pinakas apothikeyshs mhnumatwn panw apo to diktyo UDP kai TCP
typeBuffer = []		#pinakas apothikeyshs typou mhnumatos(mhnuma apo directory D, mhnuma apo sender S)
flagArray = []		#pinakas gia flags
eventArray = []
packetNo = []	#number pou dinei o sequencer

condition = threading.Condition() 		#condition gia sugxronismo kata th diarkeia pou mia efarmogh kalei th leave


#sunarthsh orismou directory ip  kai directory port
def grp_setdir(ipaddr, port):
	global dirIP
	global dirPort
	
	dirIP = str(ipaddr)
	dirPort = int(port)
	
	return

#orismos threads
class dirThread(threading.Thread):		#directory thread
	def __init__(self, port, index):
		threading.Thread.__init__(self)
		self.port = port
		self.index = index
	def run(self):
		dir_Thread(self.port, self.index)


class rcvThread(threading.Thread):		#receiver thread
	def __init__(self, index):
		threading.Thread.__init__(self)
		self.index = index
	def run(self):
		RM_deliver(self.index)
		

class sndThread(threading.Thread):		#sender thread
	def __init__(self, index):
		threading.Thread.__init__(self)
		self.index = index
	def run(self):
		sender_thread(self.index)

class seqThread(threading.Thread):		#sequencer thread
	def __init__(self, index):
		threading.Thread.__init__(self)
		self.index = index
	def run(self):
		seq_thread(self.index)


#sunarthsh gia thn oaralabh mhnymatwn apo to directory
def dir_Thread(port, index):
	global grpMembers
	global myID
	global eventArray
	global multicastIP
	global multicastPort
	global sequencerArray
	global messageBuf
	global typeBuffer
	
	index = int(index)

	eventArray[index].clear()
	
	tmp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	tmp.connect(("8.8.8.8", 80))
	ipAddr = tmp.getsockname()[0]
	tmp.close()
	
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)		#TCP connection
	s.bind((ipAddr, port))
	
	s.listen(1)	#kathe fora akoume 1 thread-sundesh
	
	while True:
		connection, clientAddr = s.accept()
		try:
			data = connection.recv(256) 
			instr, data = data.decode("utf-8").split(',',1)		#instr = Joined or Left kai omada pou prostithetai h apoxwrei
			
			if instr == "Joined":
				grpMembers[index].append(data)
				message = ('Changed group members after Joined, ' + data)#type mhnumatos gia directory
				messageBuf.append(message)
				typeBuffer.append('D')
			elif instr == "Left":
				data, flagSequencer = data.split(',', 1)
				sequencerArray[index] = int(flagSequencer)		#enhmerwsh tou sequencer gia na dhmiourghthei o kainourios
				grpMembers[index].remove(data)			#apomakrunsh apo omada
				
				message = ('Changed group members after Left, ' + data)#type mhnumatos gia directory
				messageBuf.append(message)
				typeBuffer.append('D')
				if data == myID[index]:		 #to sugkekrimenothread kanei leave ara UNLOCK to event
					connection.close()	
					s.close()
					
					print('Directory Thread returns after left: ', myID[index])
					eventArray[index].set()			#unlock to event wste na termatisei to thread
					return
			print('Changed group members after Joined or Left: ', grpMembers[index])

		finally:
			pass
	
	
#Udp receiver: epibebaiwsh me acks ston sender kai apothikeysh paketwn ston buffer ths antistoixhs diergasias
def RM_deliver(gsocket):
	global myID
	global messageBuf
	global typeBuffer
	global mulIP
	global mulPort
	global qSeq
	global packetNo
	
	index = int(gsocket)
	
	mulIP = multicastIP[index]		#multicast ip, port
	mulPort = multicastPort[index]
	currID = myID[index]			#id-appname ths kathe efarmoghs
	
	tmp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	tmp.connect(("8.8.8.8", 80))
	ipAddr = tmp.getsockname()[0]
	tmp.close()
	
	sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)		#dhmioyrgia sundeshs UDP
	sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	
	sock.bind(('',mulPort))				#bind me to diko mas ip kathe fora
	
		#!!!!!!!!!!!!!!!!!!!!little enidian
	mreq = struct.pack("4sL", socket.inet_aton(mulIP), socket.INADDR_ANY)
	sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
	sock.settimeout(2)

	while True:
		numOfOccur = 0			#arxikopiihsh
		if flagArray[index] == True:		#flag gia ton termatismo apo to thread (allagh timhs apo thn leave)
			return
		
		try:
			packet, senderAddress = sock.recvfrom(1024)				#paralabh paketou apo sender h sequencer
		except socket.timeout:
			continue

		info, restPacket = packet.split(b",",1)		#info = msg or SEQUENCER, restPacket = (ID, PacketNo)
		
		print('Received packet in RM_deliver:', packet,'\n')
		
		if info == b"msg":
			numOfOccur = restPacket.count(b',')
			
			if numOfOccur == 3:				#prokeitai gia qrxeio(perilambanei kai fielname)
				packetId, temp = restPacket.split(b",",1)		#split kai apokwdikopoihsh tou paketou
				packetSeq, temp = temp.split(b",",1)
				dTemp,data = temp.split(b",",1)			#anagnwristiko otiakolouthoun data
			elif numOfOccur == 4:			#prokeitai gia aplo buffer den einai arxeio.
				packetId, temp = restPacket.split(b",",1)		#split kai apokwdikopoihsh tou paketou
				packetSeq, temp = temp.split(b",",1)
				dTemp,temp = temp.split(b",",1)			#anagnwristiko otiakolouthoun data
				fileName, data =  temp.split(b",",1)
				
				dTemp = dTemp.decode("utf-8")
				fileName = fileName.decode("utf-8")
			
			
			packetId = packetId.decode("utf-8")
			packetSeq = packetSeq.decode("utf-8")
			packetSeq = int(packetSeq)
			
			
			#print('(MSG) Sending acknowledgement to: ', senderAddress) 	#apanthsh proswpikh ston sender me ack gia paralabh paketou
			ackMess = ("ACKM" + ',' + currID).encode("utf-8")	#ACKM + currID==appname efarmoghs
			sock.sendto(ackMess, senderAddress)
			
			condition.acquire()	
			#elegxos gia diplotypa
			currPos = 0
			flagFind = False
			for x in messageBuf:
				if x[0] == packetId and x[1] == packetSeq:	#sugkrish sugkekrimenwn timwn ths pleiadas me to upoloipo paketo
					flagFind = True
					break
				currPos += 1
			
			if flagFind == False:		#h pleiada den uparxei hdh ston buffer(den exei mpei apo ton sequencer)
				buf = []
				buf.append(packetId)
				buf.append(packetSeq)
				buf.append(dTemp)
				if(numOfOccur == 4):			#an prokteitai gia arxeio
					buf.append(fileName)
				buf.append(data)
				
				messageBuf.append(buf)#proswrinh apothikeysh mhnumatos, sth synexeia tha baloume kai to id apo ton
				typeBuffer.append('S')
				#afou hrthe neo mhnuma o sequencer tha steilei ena id gia to sugkekimeno mhnuma se olous(kai ston euato tou)
				if(sequencerArray[index] == 1):
					packetNo[index] +=1  			#seq number pou dinei o sequencer sto paketo pou erxetai
					#temp = str(packetNo[index])
					qSeq[index].put([packetId, packetSeq, packetNo[index]]) #prosthiki mhnumatos sthn oura tou sequencer
					
			else:
				if len(messageBuf[currPos]) == 3:		#uparxei hdh h  pleiada, prosthetw apla ta data
					messageBuf[currPos].append(dTemp)
					messageBuf[currPos].append(data)
			
			condition.release()	
		
		elif info == b"SEQUENCER" :			#mhnuma apo ton sequencer
			#print('(SEQ) Sending acknowledgement to: ', senderAddress) 	#apostolh ACK gia epibebaiwsh
			ackMess = ("ACKS" + ',' + currID).encode("utf-8")
			sock.sendto(ackMess, senderAddress)		#bind me to dio m ip gia proswpikh sundesh
			
			restPacket = restPacket.decode("utf-8")
			
			packetId,temp = restPacket.split(',',1)	
			packetSeq, temp = temp.split(',',1)	
			
			packetNo[index] = int(temp)
			currPacketNo = packetNo[index]
			packetSeq = int(packetSeq)
			
			#elegxos gia diplotypa
			condition.acquire()	
			currPos = 0
			flagFind = False
			for x in messageBuf:
				if x[0] == packetId and x[1] == packetSeq:
					flagFind = True
					break
				currPos += 1
			
			if(flagFind == True):			#an uparxei hdh to mhnuma tha kanw append apla to currPacketNo
				if currPacketNo not in messageBuf[currPos]:
					messageBuf[currPos].insert(2,currPacketNo)		#insert e sugkekirmnh thesh
			else:			#diaforetika tha kanw append kai to upoloipo paketo(id,seqno,packetNo[index])
				buf = []
				buf.append(packetId)
				buf.append(packetSeq)
				buf.append(currPacketNo)
				
				messageBuf.append(buf)		#leipoun mono ta data
				typeBuffer.append('S')
			
			condition.release()	
		print("-------------------------")
		print('Message buffer after receiving: ', messageBuf)
		print('Type buffer after receiving: ', typeBuffer)
		print("-------------------------\n")

#sunarthsh pou thn kaloun oi efarmoges pou theloun na kanoun join se omada(gName)
def grp_join(gName, mIP, mPort, mID):
	global dirIP
	global dirPort
	global grpName
	global multicastIP
	global multicastPort
	global myID
	global grpMembers 
	global flagsArray
	global sequencerArray
	global packetNo
	
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)	#dhmiourgia sundeshs TCP
	s.connect((dirIP, dirPort))
	port = s.getsockname()[1]

	try:		#apostolh mhnumatos sto directory
		message = 'Join ' + gName + ' ' + mIP + ' ' + mPort + ' ' + mID
		message = message.encode('utf-8')
		s.sendall(message)
		
		# Look for the response
		data = s.recv(256) 		#increase for more team members - data = OK, totalIDs, flagSequencer
		data = data.decode('utf-8')
	finally:
		s.close()		#closing socket
		
	if data[0:6] == "ERROR":		#elegxos an erthei error
		return -1
	
	resp, group = data.split(',', 1) 	#split dedomenwn gia na ananewsoume tous pinakes
	group, flagSequencer = group.rsplit(',',1)
	
	flagClose = False		#arxikopiihsh se False
	
	qMes.append(queue.Queue())
	qSeq.append(queue.Queue())
	grpName.append(gName)
	multicastIP.append(mIP)
	multicastPort.append(int(mPort))
	myID.append(mID)
	grpMembers.append([ x.strip() for x in group.split(',') ])
	sequencerArray.append(int(flagSequencer))		#apothiekysh sequencer diaergasias ths antistoixhs omadas
	seqNo.append(-1)
	flagArray.append(flagClose)
	eventArray.append(threading.Event())
	packetNo.append(0)
	
	#print("Initial group member: ", grpMembers)
	
	index = len(grpName) - 1
	
	th1 = dirThread(port, index)
	th1.start()
	
	th2 = rcvThread(index)
	th2.start()
	
	th3 = sndThread(index)
	th3.start()
	
	th4 = seqThread(index)
	th4.start()
	
	return index
	
	
#sunarthsh pou thn kaloun oi efarmoges gia na apoxwrhsoun apo omada
def grp_leave(gsocket):
	global dirIP
	global dirPort
	global grpName
	global multicastIP
	global multicastPort
	global myID
	global grpMembers 
	global eventArray
	global sequencerArray
	global qMes
	global qSeq
	global messageBuf
	global typeBuffer
	global eventRecv
	
	print("****")
	print("BEFORE LEAVING GROUP: ", messageBuf)
	print("****")
	
	index = int(gsocket)
	
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)		#dhmioyrgia sundeshs TCP
	s.connect((dirIP, dirPort))
	
	try:#aposolh mhnumatos sto directory
		message = 'Leave ' + grpName[index] + ' ' + multicastIP[index] + ' ' + str(multicastPort[index]) + ' ' + myID[index]
		message = message.encode('utf-8')
		s.sendall(message)
		
		# Look for the response
		data = s.recv(32) 	#increase for more team members
		data = data.decode('utf-8')	
	finally:
		s.close()		#closing socket
	
	if data != "OK":		#an den erthei epibebaiwsh
		return -1
	
	eventArray[index].wait()	#perimenoume na teleiwsei to thread tou directory

	flagArray[index] = True			#close to thread receiver
	
	qMes[index].put(["", -1, 0])	#close to thread tou sender
	qSeq[index].put(["", -1, 0])	#close to thread tou sequencer
	
	time.sleep(5)		#anamonh 5 deuterolepta
	
	del grpName[index]
	del multicastIP[index]
	del multicastPort[index]
	del myID[index]
	del grpMembers[index]
	del sequencerArray[index]
	del qMes[index]
	del qSeq[index]
	del eventArray[index]
	del messageBuf
	del typeBuffer
	
	return 0

#thread tou sender: dhmiourgei sundesh UDP sto multicast. pairnei to epomeno kathe fora mhnyma apo thn oura kai to stelnei mesw
#reliable multicast (RM).
def sender_thread(index):
	global myIDpacket
	global qMes
	
	index = int(index)
	
	sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)		#dhmioyrgia syndeshs UDP
	ttl = struct.pack('b', 1)
	sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
	
	
	while True:
		msg, length, seqno = qMes[index].get()		#paralabh mhnymatos apo oyra
		
		if length == -1:		#if length==-1: return + close socket
			sock.close()
			print('CLOSE: sender thread!')
			return
		msg=str(msg)
		try:
			f = open(msg, 'rb')		#an mhnyma einai arxeio open
			file_size = os.stat(msg)
			
			if(length > file_size):
				print("Given length is bigger than file size!Read only length: ", length)
				data = f.read(file_size)
			else:
				data = f.read(length)
				
			f.close()	
			#dhmiourgia paketwn gia apostolh ston receiver
			packet = bytes("msg,", "utf-8") + (myID[index] + ','+ str(seqno) +  ',' + 'D'+ ',' + msg+ ',').encode("utf-8")+ data
			print('Packet to send from sender thread:', packet)
		except:						#diaforetika kwdikopoihsh
			data = bytes(msg[:length],"utf-8")#dhmiourgia paketwn gia apostolh ston receiver
			packet = bytes("msg,", "utf-8") + (myID[index] + ','+ str(seqno) +  ',' + 'D'+ ',').encode("utf-8")+ data
			print('Packet to send from sender thread:', packet)

		RM_send(packet, sock, index) 	#blocking mexri na bebaiw8ei to sygkekrimeno paketo apo olous


#thread tou sequencer: dhmiourgei sundesh UDP sto multicast. pairnei to epomeno kathe fora mhnyma apo thn oura tou sequencer
## kai to stelnei mesw reliable multicast (RM).
def seq_thread(index):
	global myIDpacket
	global qSeq
	
	index = int(index)

	sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)		#dhmioyrgia syndeshs UDP
	ttl = struct.pack('b', 1)
	sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
	
	while True:
		packId, packSeq, packNo = qSeq[index].get()
		
		if packSeq == -1:			#if packSeq == -1: return + close socket
			print('CLOSE: sequencer thread!')
			sock.close()
			return
		
		#dhmiourgia paketwn gia apostolh ston receiver
		seqMessage = ("SEQUENCER" + ',' + str(packId) + ','+ str(packSeq) + ',' + str(packNo)).encode("utf-8")
		print('Packet to send from sequencer thread:', seqMessage)
		
		RM_send(seqMessage, sock, index) #blocking mexri na bebai8ei to sygkekrimeno paketo apo olous


#Reliable multicast. Apostolh paketwn mesw udp sundeshs kai anamonh mexri na paralhfthoun ola ta acks.An teleiwsei to timeout 
#epanapostolh tou paketou.
def RM_send(packet, sock, index):
	global grpMembers
	global multicastIP
	global multicastPort

	index = int(index)
	mulIP = multicastIP[index]
	mulPort = multicastPort[index]
	
	sock.settimeout(0.3)
	sock.sendto(packet, (mulIP, mulPort))
	#edw mporoume na metrisoume round time
	acks = []
	while True:
		try:
			data, ackAddr = sock.recvfrom(16)		#ack, member address
			ack, user = data.split(b',', 1)

			if ack == b'ACKM' or ack == b'ACKS':
				if user not in acks:
					acks.append(user)
					if(ack == b'ACKM'):
						print('IN RM_send, acksM =', acks)
					elif( ack == b'ACKS'):
						print('IN RM_send, acksS =', acks)
					
					if len(acks) == len(grpMembers[index]): #ftasane ola ta acks
						print("---GOT ALL ACKS!---")
						break
		except socket.timeout:
			print('RESEND PACKET from RM_send:', packet)
			sock.sendto(packet, (mulIP, mulPort)) #perase to timeout ara dn hr8an ola ta acks

	return

#sunarthsh gia apostolh mhnymatwn
def grp_send(gsocket, msg, length):
	global seqNo
	global qMes
	
	index = int(gsocket)
	length = int(length)
	
	seqNo[index] += 1
	
	qMes[index].put([msg, length, seqNo[index]])		#prosthiki mhnymatos sthn oura
    
	return

	
def grp_recv(gsocket, mtype, msg, length):
	index = int(gsocket)
	length = int(length)
	
	msg = checkbuffer(index, mtype, msg, length)		#anamonh gia na epistrepsei h checkbuffer

	return msg


#Message type: 1) mhnuma apo directory gia anasunthesh ths omadas, 2) mhnuma apo sender ths omadas
def checkbuffer(gsocket, mtype, msg, length):
	global messageBuf
	global typeBuffer
	
	index = int(gsocket)
	length = int(length)
	mtype = str(mtype)
	
	tempPos = 0
	tempBuf=[]		#pinakas gia proswrinh apothikeush pleiadwn
	flagFound = False
	
	condition.acquire()		#giati ginontai allagew sotn messageBuf ai typeBuffer
	
	if(mtype == 'S'):			#paralabh mhnymatos apo sender
		for x in typeBuffer:
			if x[0] == 'S':			#S for sender messages
				flagFound = True
				break
		
		if(flagFound == False):			#den brethike mhnuma me ton sugkekrimeno type
			msg = "No message with type: " + mtype
			condition.release()
			return msg
		else:
			for x in typeBuffer:				#proswrinh apothikeysh olwn twn pleiadwn tou sender
				if x[0] == 'S':			#S for sender messages
					tempBuf.append(messageBuf[tempPos])
				tempPos += 1
			
			#aanzhthsh ou mikroterou sequencernumber
			tempseq = tempBuf[0][2]

			tempPos = 0
			for x in tempBuf:				
				if x[2] <= tempseq:
					tempseq = x[2]
					tempTuple = x
					tempPos = messageBuf.index(x)
			
			msg = messageBuf[tempPos]		#apothikeush pleiads
			print(msg)
			del messageBuf[tempPos]		#apostolh mhnumatos sthn efarmogh, ara diagrafh apo ton pinaka
			del typeBuffer[tempPos]
	elif(mtype == 'D'):			#paralabh mhnymatos apo directory
		for x in typeBuffer:
			if x[0] == 'D':			#D for directory messages
				flagFound = True
				break
			tempPos += 1
		
		if(flagFound == False):
			msg = "No message with type: " + mtype
			condition.release()
			return msg
		else:
			msg = messageBuf[tempPos]
			print(msg)
			
			del messageBuf[tempPos]		#apostolh mhnumatos sthn efarmogh, ara diagrafh apo ton pinaka
			del typeBuffer[tempPos]
	condition.release()

	return msg
	
	