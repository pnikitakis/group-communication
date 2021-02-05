#Sender B
import grouplib as gl
import time
import os

ipaddr = '192.168.2.9'
#ipaddr, port = input(' Sender B started\nGive IP & port of directory (with whitespaces in between): \n').split()
port = input(' Sender B started\nGive port of directory (with whitespaces in between): \n')
gl.grp_setdir(ipaddr, port)

gName, multicastIP, multicastPort, myID = input(' Sender B: Give group name, multicastIP, multicastPort and your ID (with whitespaces in between): \n').split()

gsocket = gl.grp_join(gName, multicastIP, multicastPort, myID)

if gsocket == -1:
	print('ERROR: Sender B joined group!')

time.sleep(15)


gl.grp_send(gsocket, "hi", 4)

time.sleep(30)

print(' Sender B is leaving group...')
ret = gl.grp_leave(gsocket)
if ret == -1:
	print('ERROR:  Sender B left group!')
	
	
