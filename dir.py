#Run python3 dir.py wste na paroume server ip kai port
import socket
import struct

groups = {}

tmp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
tmp.connect(("8.8.8.8", 80))
ipAddr = tmp.getsockname()[0]
tmp.close()

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)		#dhmiourgia sundeshs TCP
s.bind((ipAddr, 0))
port = s.getsockname()[1]
print("Server -- IP: " + str(ipAddr) + " port: " + str(port))

s.listen(100)

while True:
	connection, clientAddr = s.accept()		#apodoxh sundesewn
	try:
		data = connection.recv(256) #more if huge incoming msg
		data = data.decode("utf-8")
		data = [ x.strip() for x in data.split(' ') ]
		
		if len(data) != 5:		#elegxos gia request
			print("Not valid syntax for the request")
			
			errorMess = 'ERROR:Not valid syntax for the request '
			errorMess = bytes(errorMess,"utf-8")
			
			connection.send(errorMess)	#send OK or error to sender
			connection.close()
			continue
		
		gName = data[1]		#apothikeush dedomenwn 
		gIP = data[2]
		gPort = data[3]
		appName = data[4]
		
		flagSequencer = 0
		if data[0] == "Join":		#an Join  elegxos gia to mhnuma
			if gName not in groups: 	#nea omada -> create
				groups[gName] = [gIP, gPort, []]
				flagSequencer = 1
				nameOfSequencer = appName		#arxikopihsh sequencer
				
			if groups[gName][0] !=  gIP or groups[gName][1] != gPort:
				print("Requested join with invalid miltucast address or port!")
				
				errorMess = 'ERROR:invalid miltucast address or port ' +','+ gIP
				errorMess = bytes(errorMess,"utf-8")
				connection.send(errorMess)
				connection.close()
				continue
			
			for x in groups[gName][2]:
				if x[0] == appName:
					print("Current app already exists in the group!")
					errorMess = 'ERROR:Current app already exist in the group ' +','+ gIP
					errorMess = bytes(errorMess,"utf-8")
					connection.send(errorMess)
					connection.close()
					continue
			
			#update others
			updateMess = 'Joined' + ',' + appName
			updateMess = bytes(updateMess,"utf-8")
				
			for x in groups[gName][2]:
				sTemp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				sTemp.connect(x[1])
				
				sTemp.sendall(updateMess)
				sTemp.close()
			
			groups[gName][2].append([appName, clientAddr]) #add sth omada
			
			totalIDs = ""	
			for x in groups[gName][2]:
				totalIDs += x[0] + ','
			totalIDs = totalIDs[:-1]	#petagma teleutaio koma
			totalIDs = bytes(totalIDs,"utf-8")
			
				#send OK to sender+totalIDs twn melwn ths omadas
			confMess = b'OK,' + totalIDs + (bytes(',' + (str(flagSequencer)), "utf-8"))
			connection.send(confMess)	#send OK to sender
		
		elif data[0] == "Leave":		#an mhnyma leave
			if gName not in groups:				#den uparxei omada me to sugkekrimeno onoma
				print('Error. There is no such group leave')#reply error?
				errorMess = 'ERROR:There is no such group leave ' +','+ gIP
				errorMess = bytes(errorMess,"utf-8")
				
				connection.send(errorMess)
				connection.close()
				continue
			
			flagFind = False
			for x in  groups[gName][2]:			#psaximo mesa sto dictionary gia anazhthsh ou sugkekrimenou onomatos efarmoghs
				if appName == x[0]:
					appIndex = groups[gName][2].index(x)
					flagFind = True
					break
			
			if flagFind != True:
				print('Error. There is no app in the group')
				errorMess = 'ERROR: There is no app in the group ' +','+ gIP
				errorMess = bytes(errorMess,"utf-8")
				
				connection.send(errorMess)
				connection.close()
				continue
			
			#update mhnuma 
			updateMess = 'Left' + ',' + appName + ',0' 
			updateMess = bytes(updateMess,"utf-8")
			
			i = 0
			for x in groups[gName][2]:
				sTemp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)#dhmiourgia sundeshs gia apostolh enhmerwshs
				sTemp.connect(x[1])
				if appName == nameOfSequencer and i == 0 and appName != x[0]:	#an feygei o sequencer
					nameOfSequencer = x[0]										#prepei na dhmioughthei allos me tyxaia epilogh
					i =+ 1
					sTemp.sendall(bytes('Left' + ',' + appName + ',1',"utf-8"))
					sTemp.close()
					continue
				sTemp.sendall(updateMess)
				sTemp.close()
			
			confMess = b'OK'			#send OK to sender (confirm message)
			connection.send(confMess)
			
			del groups[gName][2][appIndex]
			
			if len(groups[gName][2]) == 0: #adeia omada -> diagrafh ths
				del groups[gName]
		else:
			print("Error at incoming connection. Unknown request")
			errorMess = 'ERROR at incoming connection. Unknown request ' +','+ gIP
			errorMess = bytes(errorMess,"utf-8")
			
			connection.send(errorMess)
			connection.close()
			continue
	finally:
		connection.close()
		
	print("Groups in directory: " , groups)
	