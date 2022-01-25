from peer import *

PEERNAME = "NAME"
LISTPEERS = "LIST"
INSERTPEER = "JOIN"
QUERY = "QUER"
QRESPONSE = "RESP"
FILEGET = "FGET"
PEERQUIT = "QUIT"

REPLY = "REPL"
ERROR = "ERRO"


class MyPeer(Peer):
    """
    Implements a file-sharing peer-to-peer entity
    """

    def __init__(self, maxPeers, serverPort):
        """
        Initializes the peer to support connections up to maxPeers number
        of peers, with its server listening on the specified port. Also sets
        the dictionary of local files to empty and adds handlers to the 
        Peer framework.
        """
        Peer.__init__(self, maxPeers, serverPort)

        self.files = {}  # available files: name --> peerid mapping

        self.addRouter(self.__router)

        handlers = {
            LISTPEERS: self.__handleListPeers,
            INSERTPEER: self.__handleInsertPeer,
            PEERNAME: self.__handlePeerName,
            QUERY: self.__handleQuery,
            QRESPONSE: self.__handleQResponse,
            FILEGET: self.__handleFileGet,
            PEERQUIT: self.__handleQuit
        }
        for mt in handlers:
            self.addHandler(mt, handlers[mt])

    # end MyPeer constructor

    def __debug(self, msg):
        if self.debug:
            debug(msg)

    def __router(self, peerid):
        if peerid not in self.getPeerIds():
            return (None, None, None)
        else:
            rt = [peerid]
            rt.extend(self.peers[peerid])
            return rt

    def __handleInsertPeer(self, peerConn, data):
        """
        Handles the INSERTPEER (join) message type. The message data
        should be a string of the form, "peerid  host  port", where peer-id
        is the canonical name of the peer that desires to be added to this
        peer's list of peers, host and port are the necessary data to connect
        to the peer.
        """
        self.peerLock.acquire()
        try:
            try:
                peerid, host, port = data.split()

                if self.maxPeersReached():
                    self.__debug(
                        f"maxPeers {self.maxPeers} reached: connection terminating")
                    peerConn.sendData(ERROR, 'Join: too many peers')
                    return

                # peerid = '%s:%s' % (host,port)
                if peerid not in self.getPeerIds() and peerid != self.myId:
                    self.addPeer(peerid, host, port)
                    self.__debug(f"added peer: {peerid}")
                    peerConn.sendData(REPLY, f"Join: peer added: {peerid}")
                else:
                    peerConn.sendData(
                        ERROR, f"Join: peer already inserted {peerid}")
            except:
                self.__debug(f"invalid insert {str(peerConn)}: {data}")
                peerConn.sendData(ERROR, 'Join: incorrect arguments')
        finally:
            self.peerLock.release()

    # end handle_insertpeer method

    def __handleListPeers(self, peerConn, data):
        """ Handles the LISTPEERS message type. Message data is not used. """
        self.peerLock.acquire()
        try:
            self.__debug(f"Listing peers {self.numberOfPeers()}")
            peerConn.sendData(REPLY, f"{self.numberOfPeers()}")
            for pid in self.getPeerIds():
                host, port = self.getPeer(pid)
                peerConn.sendData(REPLY, f"{pid} {host} {port}")
        finally:
            self.peerLock.release()

    def __handlePeerName(self, peerConn, data):
        """ Handles the NAME message type. Message data is not used. """
        peerConn.sendData(REPLY, self.myId)

    def __handleQuery(self, peerConn, data):
        """
        Handles the QUERY message type. The message data should be in the
        format of a string, "return-peer-id  key  ttl", where return-peer-id
        is the name of the peer that initiated the query, key is the (portion
        of the) file name being searched for, and ttl is how many further 
        levels of peers this query should be propagated on.
        """
        # self.peerLock.acquire()
        try:
            peerid, key, ttl = data.split()
            peerConn.sendData(REPLY, f"Query ACK: {key}")
        except:
            self.__debug(f"invalid query {str(peerConn)}: {data}")
            peerConn.sendData(ERROR, 'Query: incorrect arguments')
        # self.peerLock.release()

        t = threading.Thread(target=self.__processQuery,
                             args=[peerid, key, int(ttl)])
        t.start()

    #

    def __processQuery(self, peerid, key, ttl):
        """
        Handles the processing of a query message after it has been 
        received and acknowledged, by either replying with a QRESPONSE message
        if the file is found in the local list of files, or propagating the
        message onto all immediate neighbors.
        """
        for fname in self.files.keys():
            if key in fname:
                fpeerid = self.files[fname]
                if not fpeerid:   # local files mapped to None
                    fpeerid = self.myId
                host, port = peerid.split(':')
                # can't use sendToPeer here because peerid is not necessarily
                # an immediate neighbor
                self.connectAndSend(host, int(port), QRESPONSE,
                                    f"{fname} {fpeerid}", pid=peerid)
                return
        # will only reach here if key not found... in which case
        # propagate query to neighbors
        if ttl > 0:
            msgdata = f"{peerid} {key} {ttl - 1}"
            for nextpid in self.getPeerIds():
                self.sendToPeer(nextpid, QUERY, msgdata)

    def __handleQResponse(self, peerConn, data):
        """
        Handles the QRESPONSE message type. The message data should be
        in the format of a string, "file-name  peer-id", where file-name is
        the file that was queried about and peer-id is the name of the peer
        that has a copy of the file.
        """
        try:
            fname, fpeerid = data.split()
            if fname in self.files:
                self.__debug(f"Can't add duplicate file {fname} {fpeerid}")
            else:
                self.files[fname] = fpeerid
        except:
            if self.debug:
                traceback.print_exc()

    def __handleFileGet(self, peerConn, data):
        """
        Handles the FILEGET message type. The message data should be in
        the format of a string, "file-name", where file-name is the name
        of the file to be fetched.
        """
        fname = data
        if fname not in self.files:
            self.__debug(f"File not found {fname}")
            peerConn.sendData(ERROR, 'File not found')
            return
        try:
            fd = open(fname, 'r')
            filedata = ''
            while True:
                data = fd.read(2048)
                if not len(data):
                    break
                filedata += data
            fd.close()
        except:
            self.__debug(f"Error reading file {fname}")
            peerConn.sendData(ERROR, 'Error reading file')
            return

        peerConn.sendData(REPLY, filedata)

    def __handleQuit(self, peerConn, data):
        """
        Handles the QUIT message type. The message data should be in the
        format of a string, "peer-id", where peer-id is the canonical
        name of the peer that wishes to be unregistered from this
        peer's directory.
        """
        self.peerLock.acquire()
        try:
            peerid = data.lstrip().rstrip()
            if peerid in self.getPeerIds():
                msg = f"Quit: peer removed: {peerid}"
                self.__debug(msg)
                peerConn.sendData(REPLY, msg)
                self.removePeer(peerid)
            else:
                msg = f"Quit: peer not found: {peerid}"
                self.__debug(msg)
                peerConn.sendData(ERROR, msg)
        finally:
            self.peerLock.release()

    def buildPeers(self, host, port, hops=1):
        """
        buildPeers(host, port, hops) 

        Attempt to build the local peer list up to the limit stored by
        self.maxPeers, using a simple depth-first search given an
        initial host and port as starting point. The depth of the
        search is limited by the hops parameter.
        """
        if self.maxPeersReached() or not hops:
            return

        peerid = None

        self.__debug(f"Building peers from ({host},{port})")

        try:
            _, peerid = self.connectAndSend(host, port, PEERNAME, '')[0]

            self.__debug("contacted " + peerid)
            resp = self.connectAndSend(
                host, port, INSERTPEER, f"{self.myId} {self.serverHost} {self.serverPort}")[0]
            self.__debug(str(resp))
            if (resp[0] != REPLY) or (peerid in self.getPeerIds()):
                return

            self.addPeer(peerid, host, port)

            # do recursive depth first search to add more peers
            resp = self.connectAndSend(host, port, LISTPEERS, '',
                                       pid=peerid)
            if len(resp) > 1:
                resp.reverse()
                resp.pop()    # get rid of header count reply
                while len(resp):
                    nextpid, host, port = resp.pop()[1].split()
                    if nextpid != self.myId:
                        self.buildPeers(host, port, hops - 1)
        except:
            if self.debug:
                traceback.print_exc()
            self.removePeer(peerid)

    def addLocalFile(self, filename):
        """ Registers a locally-stored file with the peer. """
        self.files[filename] = None
        self.__debug(f"Added local file {filename}")
