import socket
import struct
import threading
import time
import traceback


def debug(msg):
    """ Prints a messsage to the screen with the name of the current thread """
    print(f"[{str(threading.currentThread().getName())}]", msg)


class Peer:
    """
    Implements the core functionality that might be used by a peer in a
    P2P network.
    """

    def __init__(self, maxPeers, serverPort, myId=None, serverHost=None):
        """
        Initializes a peer servent (sic.) with the ability to catalog
        information for up to maxPeers number of peers (maxPeers may
        be set to 0 to allow unlimited number of peers), listening on
        a given server port , with a given canonical peer name (id)
        and host address. If not supplied, the host address
        (serverHost) will be determined by attempting to connect to an
        Internet host like Google.
        """
        self.debug = 0

        self.maxPeers = int(maxPeers)
        self.serverPort = int(serverPort)
        if serverHost:
            self.serverHost = serverHost
        else:
            self.__initServerHost()

        if myId:
            self.myId = myId
        else:
            self.myId = f"{self.serverHost}:{self.serverPort}"

        self.peerLock = threading.Lock()  # ensure proper access to
        # peers list (maybe better to use
        # threading.RLock (reentrant))
        self.peers = {}        # peerid ==> (host, port) mapping
        self.shutdown = False  # used to stop the main loop

        self.handlers = {}
        self.router = None

    def __initServerHost(self):
        """
        Attempt to connect to an Internet host in order to determine the
        local machine's IP address.
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("www.google.com", 80))
        self.serverHost = s.getsockname()[0]
        s.close()

    def __debug(self, msg):
        if self.debug:
            debug(msg)

    def __handlePeer(self, clientSock):
        """
        handlePeer( new socket connection ) -> ()

        Dispatches messages from the socket connection
        """

        self.__debug('New child ' + str(threading.currentThread().getName()))
        self.__debug('Connected ' + str(clientSock.getpeername()))

        host, port = clientSock.getpeername()
        peerConn = PeerConnection(None, host, port, clientSock, debug=False)

        try:
            msgtype, msgdata = peerConn.recvData()
            if msgtype:
                msgtype = msgtype.upper()
            if msgtype not in self.handlers:
                self.__debug(f"Not handled: {msgtype}: {msgdata}")
            else:
                self.__debug(f"Handling peer msg: {msgtype}: {msgdata}")
                self.handlers[msgtype](peerConn, msgdata)
        except KeyboardInterrupt:
            raise
        except:
            if self.debug:
                traceback.print_exc()

        self.__debug('Disconnecting ' + str(clientSock.getpeername()))
        peerConn.close()

    # end handlePeer method

    def __runStabilizer(self, stabilizer, delay):

        while not self.shutdown:
            stabilizer()
            time.sleep(delay)

    def setMyId(self, myId):

        self.myId = myId

    def startStabilizer(self, stabilizer, delay):
        """
        Registers and starts a stabilizer function with this peer. 
        The function will be activated every <delay> seconds. 
        """
        t = threading.Thread(target=self.__runStabilizer,
                             args=[stabilizer, delay])
        t.start()

    def addHandler(self, msgtype, handler):
        """ Registers the handler for the given message type with this peer """
        assert len(msgtype) == 4
        self.handlers[msgtype] = handler

    def addRouter(self, router):
        """
        Registers a routing function with this peer. The setup of routing
        is as follows: This peer maintains a list of other known peers
        (in self.peers). The routing function should take the name of
        a peer (which may not necessarily be present in self.peers)
        and decide which of the known peers a message should be routed
        to next in order to (hopefully) reach the desired peer. The router
        function should return a tuple of three values: (next-peer-id, host,
        port). If the message cannot be routed, the next-peer-id should be
        None.
        """
        self.router = router

    def addPeer(self, peerid, host, port):
        """
        Adds a peer name and host:port mapping to the known list of peers.
        """
        if peerid not in self.peers and (self.maxPeers == 0 or
                                         len(self.peers) < self.maxPeers):
            self.peers[peerid] = (host, int(port))
            return True
        else:
            return False

    def getPeer(self, peerid):
        """ Returns the (host, port) tuple for the given peer name """
        assert peerid in self.peers    # maybe make this just a return NULL?
        return self.peers[peerid]

    def removePeer(self, peerid):
        """ Removes peer information from the known list of peers. """
        if peerid in self.peers:
            del self.peers[peerid]

    def addPeerAt(self, loc, peerid, host, port):
        """
        Inserts a peer's information at a specific position in the 
        list of peers. The functions addPeerAt, getPeerAt, and removePeerAt
        should not be used concurrently with addPeer, getPeer, and/or 
        removePeer.
        """
        self.peers[loc] = (peerid, host, int(port))

    def getPeerAt(self, loc):

        if loc not in self.peers:
            return None
        return self.peers[loc]

    def removePeerAt(self, loc):

        self.removePeer(self, loc)

    def getPeerIds(self):
        """ Return a list of all known peer id's. """
        return self.peers.keys()

    def numberOfPeers(self):
        """ Return the number of known peer's. """
        return len(self.peers)

    def maxPeersReached(self):
        """
        Returns whether the maximum limit of names has been added to the
        list of known peers. Always returns True if maxPeers is set to
        0.
        """
        assert self.maxPeers == 0 or len(self.peers) <= self.maxPeers
        return self.maxPeers > 0 and len(self.peers) == self.maxPeers

    def makeServerSocket(self, port, backlog=5):
        """
        Constructs and prepares a server socket listening on the given 
        port.
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('', port))
        s.listen(backlog)
        return s

    def sendToPeer(self, peerid, msgtype, msgdata, waitreply=True):
        """
        sendToPeer( peer id, message type, message data, wait for a reply )
         -> [ ( reply type, reply data ), ... ] 

        Send a message to the identified peer. In order to decide how to
        send the message, the router handler for this peer will be called.
        If no router function has been registered, it will not work. The
        router function should provide the next immediate peer to whom the 
        message should be forwarded. The peer's reply, if it is expected, 
        will be returned.

        Returns None if the message could not be routed.
        """

        if self.router:
            nextpid, host, port = self.router(peerid)
        if not self.router or not nextpid:
            self.__debug(f"Unable to route {msgtype} to {peerid}")
            return None
        #host,port = self.peers[nextpid]
        return self.connectAndSend(host, port, msgtype, msgdata,
                                   pid=nextpid,
                                   waitreply=waitreply)

    def connectAndSend(self, host, port, msgtype, msgdata,
                       pid=None, waitreply=True):
        """
        connectAndSend( host, port, message type, message data, peer id,
        wait for a reply ) -> [ ( reply type, reply data ), ... ]

        Connects and sends a message to the specified host:port. The host's
        reply, if expected, will be returned as a list of tuples.
        """
        msgreply = []
        try:
            peerConn = PeerConnection(pid, host, port, debug=self.debug)
            peerConn.sendData(msgtype, msgdata)
            self.__debug(f"Sent {pid}: {msgtype}")

            if waitreply:
                onereply = peerConn.recvData()
                while (onereply != (None, None)):
                    msgreply.append(onereply)
                    self.__debug(f"Got reply {pid}: {str(msgreply)}")
                    onereply = peerConn.recvData()
            peerConn.close()
        except KeyboardInterrupt:
            raise
        except:
            if self.debug:
                traceback.print_exc()

        return msgreply

    # end connectsend method

    def checkLivePeers(self):
        """
        Attempts to ping all currently known peers in order to ensure that
        they are still active. Removes any from the peer list that do
        not reply. This function can be used as a simple stabilizer.
        """
        toDelete = []
        for pid in self.peers:
            isConnected = False
            try:
                self.__debug(f"Check live {pid}")
                host, port = self.peers[pid]
                peerConn = PeerConnection(pid, host, port, debug=self.debug)
                peerConn.sendData('PING', '')
                isConnected = True
            except:
                toDelete.append(pid)
            if isConnected:
                peerConn.close()

        self.peerLock.acquire()
        try:
            for pid in toDelete:
                if pid in self.peers:
                    del self.peers[pid]
        finally:
            self.peerLock.release()
    # end checkLivePeers method

    def mainLoop(self):

        s = self.makeServerSocket(self.serverPort)
        s.settimeout(2)
        self.__debug(
            f"Server started: {self.myId} ({self.serverHost}:{self.serverPort})")

        while not self.shutdown:
            try:
                self.__debug('Listening for connections...')
                clientSock, clientaddr = s.accept()
                clientSock.settimeout(None)

                t = threading.Thread(target=self.__handlePeer,
                                     args=[clientSock])
                t.start()
            except KeyboardInterrupt:
                print('KeyboardInterrupt: stopping mainLoop')
                self.shutdown = True
                continue
            except:
                if self.debug:
                    traceback.print_exc()
                    continue

        # end while loop
        self.__debug('Main loop exiting')

        s.close()

    # end mainLoop method

# end Peer class


class PeerConnection:

    def __init__(self, peerid, host, port, sock=None, debug=False):
        # any exceptions thrown upwards

        self.id = peerid
        self.debug = debug

        if not sock:
            self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.s.connect((host, int(port)))
        else:
            self.s = sock

        self.sd = self.s.makefile('rwb', 0)

    def __makemsg(self, msgtype, msgdata):
        msglen = len(msgdata)
        msg = struct.pack(f"!4sL{msglen}s",
                          msgtype.encode(), msglen, msgdata.encode())
        return msg

    def __debug(self, msg):
        if self.debug:
            debug(msg)

    def sendData(self, msgtype, msgdata):
        """
        sendData( message type, message data ) -> boolean status

        Send a message through a peer connection. Returns True on success
        or False if there was an error.
        """

        try:
            msg = self.__makemsg(msgtype, msgdata)
            self.sd.write(msg)
            self.sd.flush()
        except KeyboardInterrupt:
            raise
        except:
            if self.debug:
                traceback.print_exc()
            return False
        return True

    def recvData(self):
        """
        recvData() -> (msgtype, msgdata)

        Receive a message from a peer connection. Returns (None, None)
        if there was any error.
        """

        try:
            msgtype = self.sd.read(4)
            msgtype = msgtype.decode()
            if not msgtype:
                return (None, None)

            lenstr = self.sd.read(4)
            msglen = int(struct.unpack("!L", lenstr)[0])
            msg = ""

            while len(msg) != msglen:
                data = self.sd.read(min(2048, msglen - len(msg)))
                data = data.decode()
                if not len(data):
                    break
                msg += data

            if len(msg) != msglen:
                return (None, None)

        except KeyboardInterrupt:
            raise
        except:
            if self.debug:
                traceback.print_exc()
            return (None, None)

        return (msgtype, msg)

    # end recvData method

    def close(self):
        """
        Close the peer connection. The send and recv methods will not work
        after this call.
        """

        self.s.close()
        self.s = None
        self.sd = None

    def __str__(self):
        return f"|{self.id}|"
