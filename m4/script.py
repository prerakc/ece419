from subprocess import Popen
import threading

starting_port = 8082
ecsPort = 2181
addr = '127.0.0.1'
n = 5
# threads = 
cmds = []

for i in range(n):
    cmds.append('java -jar m2-server.jar ' + addr +' '+ str(starting_port+i) + ' '+ '10000' + ' i ' + '127.0.0.1 ' + str(ecsPort))

procs = [ Popen(i,shell=True) for i in cmds ]
for p in procs:
   p.wait()