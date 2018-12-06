import sys, os
import hashlib
from paramiko import SSHClient
from scp.scp import SCPClient


try:
    sys.argv[1]
except IndexError:
    print("usage: python scphash.py [hostname] [username] [password] [filename] [hash algorism]")
    print("[[hash algorism number]] 1: MD5, 2: SHA-1, 3: SHA-256, 4: SHA-512")
    sys.exit(1)

ssh = SSHClient()
ssh.load_system_host_keys()
# ssh.connect('192.168.7.71',username='root',password='lwa123*')
# print(sys.argv[1], type(sys.argv[1]))
ssh.connect(sys.argv[1], username=sys.argv[2], password=sys.argv[3])

# SCPCLient takes a paramiko transport as an argument
scp = SCPClient(ssh.get_transport())

# Uploading the 'test' directory with its content in the
# '/home/user/dump' remote directory
scp.put(sys.argv[4], recursive=True, remote_path='/home/lsiem/')

# send hash value file 
BLOCKSIZE = 65536

if sys.argv[5] == '1':
    hasher = hashlib.md5()
elif sys.argv[5] == '2':
    hasher = hashlib.sha1()
elif sys.argv[5] == '3':
    hasher = hashlib.sha256()
elif sys.argv[5] == '4':
    hasher = hashlib.sha512()
else:
    print("not defined hash algorism number error")

with open(sys.argv[4], 'rb') as afile:
    buf = afile.read(BLOCKSIZE)
    while len(buf) > 0:
        hasher.update(buf)
        buf = afile.read(BLOCKSIZE)

print(hasher.hexdigest())
with open('hash.txt', 'a+') as f:
    f.write(hasher.hexdigest())

scp.put('hash.txt', recursive=True, remote_path='/home/lsiem/')

os.remove('hash.txt')

scp.close()


