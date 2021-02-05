import grouplib as gl
import time
import os

ipaddr = '192.168.2.9'
#ipaddr, port = input('Sender A started\nGive IP & port of directory (with whitespaces in between): \n').split()
port = input('Sender A started\nGive port of directory (with whitespaces in between): \n')
gl.grp_setdir(ipaddr, port)

gName, multicastIP, multicastPort, myID = input('Sender A: Give group name, multicastIP, multicastPort and your ID (with whitespaces in between): \n').split()

gsocket = gl.grp_join(gName, multicastIP, multicastPort, myID)

if gsocket == -1:
	print('ERROR: Sender A joined group!')

time.sleep(15)


gl.grp_send(gsocket, "test.txt", 4)

time.sleep(30)

print('Sender A is leaving group...')
ret = gl.grp_leave(gsocket)
if ret == -1:
	print('ERROR: Sender A left group!')
	
	
