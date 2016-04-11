
# coding: utf-8

# In[ ]:

import pexpect

print("Start sync data from webserver to compserver")

# Read my password for ssh key
with open("/media/raid0/zeshi/AR_db/passphrase.txt") as f:
    my_pass = f.readline()
    
# Rsync everything
child = pexpect.spawn("rsync -avz -e ssh ziran@glaser.berkeley.edu:~/AR2014/data /media/raid0/zeshi/AR_db/tmp")
try:
    child.expect("Enter passphrase for key '/home/zeshi/.ssh/id_rsa':")
    child.sendline(my_pass)
except pexpect.TIMEOUT as timeout:
    print(timeout)
child.interact()