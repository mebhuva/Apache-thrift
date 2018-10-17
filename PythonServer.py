#!/usr/bin/env python

import glob
import sys
import socket
sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages')[0])
from hashlib import sha256
from chord import FileStore
from chord.ttypes import *
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
CurrentNode = NodeID(sha256(socket.gethostbyname(socket.gethostname())+":"+sys.argv[1]).hexdigest(), socket.gethostbyname(socket.gethostname()),int(sys.argv[1]))

class FileStoreHandler:
    serverdict = {}	
    FingerTable = []
    def FindRange(self, key, startPoint, endPoint):
	logger.debug("if key exists in this range")
        if key > startPoint and key < endPoint:
		logger.debug("Yes it exists")        	
		return True
	elif startPoint > endPoint:
		if key < startPoint and key < endPoint:
			logger.debug("Yes it exists")
			return True	
		else :
			logger.debug("Not exists")
			return False		
	logger.debug("Not exists")
        return False

    def setFingertable(self, node_list):
	logger.debug("inside setfinger table")
        if not node_list: 
        	raise SystemException("Finger table can not be set because Node list is empty")
        	return
        self.FingerTable= node_list
        logger.debug("setfinger table done")

    def findPred(self, key):
	logger.debug("Statrted looking for node predecessor")
        if not self.FingerTable:
        	logger.debug("FingerTable does not exist")
        	raise SystemException("FingerTable does not exist")
		return
	inBet = self.FindRange(key, CurrentNode.id, self.FingerTable[0].id);
	if(inBet):
		return CurrentNode
	else:
		highestNodeforCurrentNode = self.FingerTable[-1]
		for currentListNode in reversed(self.FingerTable):
                	if(self.FindRange(currentListNode.id, CurrentNode.id, key)):
		        	return highestNodeforCurrentNode
	return CurrentNode

    def getNodeSucc(self):
	logger.debug("Started looking for successor node")
        if(not self.FingerTable):
        	logger.debug("FingerTable does not exist")
        	raise SystemException("FingerTable does not exist")
		return
        else:
        	succnode = self.FingerTable[0]
        	logger.debug("Ending finding successor node")        
        	return succnode 
        return

    def findSucc(self, key):
	logger.debug("Statrted looking for succesor")
        if(not self.FingerTable):
        	logger.debug("FingerTable does not exist")
        	raise SystemException("FingerTable does not exist")
		return
        Node = self.findPred(key)
	if Node == CurrentNode:
		newnode = self.getNodeSucc()
        	logger.debug("End finding node succesor")  
                return newnode

    def writeFile(self, rFile):
	if(not self.FingerTable):
        	logger.debug("FingerTable does not exist")
        	raise SystemException("FingerTable does not exist")
		return
	key = sha256(rFile.meta.filename).hexdigest()
        Node = self.findSucc(key)
        if CurrentNode != Node:
        	logger.debug("Can't write the file beacuse server does not own the file")
        	raise SystemException("Can't write the file beacuse server does not own the file")
        	return
        objRFile = RFile(RFileMetadata(),"")
	if(not rFile.meta.filename in self.serverdict.keys()):
		logger.debug("Creting New File started")   
                objRFile = rFile
                objRFile.meta.version = 0
                objRFile.meta.contentHash = sha256(rFile.content).hexdigest()
		objRFile.content = rFile.content
                objRFile.meta.filename = rFile.meta.filename
		logger.debug("File object with all metainformation created")                   
        self.serverdict[rFile.meta.filename] = objRFile
        logger.debug("File " + rFile.meta.filename + " writing Complete" )
	return



    def readFile(self, filename):
	if(not self.FingerTable):
        	logger.debug("FingerTable does not exist")
        	raise SystemException("FingerTable does not exist")
		return
	key = sha256(filename).hexdigest()
        Node = self.findSucc(key)
	logger.debug("found Node")
        if CurrentNode != Node:
        	logger.debug("Can't read the file beacuse server does not own the file")
        	raise SystemException("Can't read the file beacuse server does not own the file")
        	return
        
        if(not filename in self.serverdict.keys()):
		logger.debug("Server don't have the file")
        	raise SystemException("Server don't have have the file")
		return             
        else:            
                rFile = self.serverdict[filename]
		logger.debug("File read complete :"+ filename)
		return rFile
        return


if __name__ == '__main__':
    localhost = socket.gethostbyname(socket.gethostname())
    print(localhost+":"+sys.argv[1])
    handler = FileStoreHandler()
    processor = FileStore.Processor(handler)
    transport = TSocket.TServerSocket(port=int(sys.argv[1]))
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

    # You could do one of these for a multithreaded server
    # server = TServer.TThreadedServer(
    #     processor, transport, tfactory, pfactory)
    # server = TServer.TThreadPoolServer(
    #     processor, transport, tfactory, pfactory)

    print('Starting the server...')
    server.serve()
    print('done.')