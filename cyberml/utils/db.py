import pyorient
import time
import logging


class DBConnection(object):
    def __init__(self, batch=False, batch_every=10000):
        self.batch = batch
        self.batch_every = batch_every
        self.commands = []
        self.client = None
        self.logger = logging.getLogger(self.__class__.__name__)

    def __enter__(self):
        if self.client is None:
            self.client = pyorient.OrientDB(host="localhost", port=2424)
        self.client.db_open("cyber", "root", "root")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            if len(self.commands) > 0:
                self._check_and_batch(force_run=True)
            self.client.close()

    def _check_and_batch(self, force_run=False):
        if len(self.commands) >= self.batch_every or force_run:
            s = time.time()
            res = self.client.batch( ";".join(self.commands) )
            self.logger.debug("Batched {} commands. Took {} seconds".format(len(self.commands), time.time()-s))
            self.commands = []
            return res

    def add_node(self, ip, timestamp):
        cmd = "UPDATE Node SET ip='{ip}', timestamp={timestamp} UPSERT WHERE ip='{ip}'".format(ip=ip, timestamp=timestamp)
        if self.batch:
            self.commands.append(cmd)
            return self._check_and_batch()
        return self.client.command(cmd)

    def add_object(self, hash, timestamp, malware):
        cmd = ("UPDATE Object SET hash='{hash}', timestamp={timestamp}, malware={malware} "
               "UPSERT WHERE hash='{hash}'".format(hash=hash, timestamp=timestamp, malware=malware))
        if self.batch:
            self.commands.append(cmd)
            return self._check_and_batch()
        return self.client.command(cmd)


    def add_flow(self, source_ip, source_port, destination_ip, destination_port, object_hash, timestamp, protocol="", malware=True):
        cmd = ("LET $records=(SELECT FROM Object WHERE hash='{object_hash}'); "  # for some reason can't do subquery and need this silly batch query
               "CREATE EDGE Flow "
               "FROM(SELECT FROM Node WHERE ip='{source_ip}') "
               "TO(SELECT FROM Node WHERE ip='{destination_ip}') "
               "SET object = $records[0], protocol='{protocol}', "
               "sourcePort='{source_port}', destinationPort='{destination_port}', "
               "timestamp={timestamp}".format(source_ip=source_ip, destination_ip=destination_ip,
                                              source_port=source_port, destination_port=destination_port,
                                              object_hash=object_hash, timestamp=timestamp, protocol=protocol))
        self.add_node(source_ip, timestamp)
        self.add_node(destination_ip, timestamp)
        self.add_object(object_hash, timestamp, malware)
        if self.batch:
            self.commands.append(cmd)
            return self._check_and_batch()
        return self.client.batch(cmd)


