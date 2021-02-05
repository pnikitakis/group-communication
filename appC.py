import grouplib as gl
import time
import os

ipaddr = '192.168.2.9'
#ipaddr, port = input('App C started\nGive IP & port of directory (with whitespaces in between): \n').split()
port = input('App C started\nGive port of directory (with whitespaces in between): \n')
gl.grp_setdir(ipaddr, port)

gName, multicastIP, multicastPort, myID = input('App C: Give group name, multicastIP, multicastPort and your ID (with whitespaces in between): \n').split()

gsocket = gl.grp_join(gName, multicastIP, multicastPort, myID)

if gsocket == -1:
	print('ERROR: App C joined group!')

time.sleep(30)

messType = 'S'		#orismos type mhnumatos
msg=''
msg = gl.grp_recv(gsocket, messType, msg, 0 )


if messType == 'S':
	if len(msg) == 6:           #prokeitai gia arxeio ara dhmiourgia arxeiou
		sender = msg[0]
		fileName = msg[4]
		data = msg[5]

		os.makedirs(os.path.dirname('recieved_files/'), exist_ok=True)
		fileName = 'recieved_files/' + fileName

		f = open(fileName, "ab")
		f.write(data)

		print("----")
		print("AppC: received message from: ",msg[0],"sender:", fileName, "file!")
		print("----")
	elif len(msg) == 5:         #aplh ektypwsh
		sender = msg[0]
		data = msg[4].decode("utf-8")
		
		print("----")
		print("AppC: received message from: ",msg[0],"sender:", data, " !")
		print("----")
elif messType == 'D':
	print("----")
	print("AppC: received message: ", msg)
	print("----")

time.sleep(15)

print('App C is leaving group...')
ret = gl.grp_leave(gsocket)
if ret == -1:
	print('ERROR: App C left group!')
	
	