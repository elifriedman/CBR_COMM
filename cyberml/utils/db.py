import pyorient
import time
import logging


class DBConnection(object):
    def __init__(self, batch=False, batch_every=10000, db="cyber", user="root", password="root", host="localhost", port=2424):
        self.batch = batch
        self.batch_every = batch_every
        self.commands = []
        self.client = None
        self.config = {"db": db,
                       "user": user,
                       "password": password,
                       "host": host,
                       "port": port}
        self.logger = logging.getLogger(self.__class__.__name__)

    def __enter__(self):
        if self.client is None:
            self.client = pyorient.OrientDB(host=self.config["host"], port=self.config["port"])
        self.client.db_open(self.config["db"], self.config["user"], self.config["password"])
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

    def add_object(self, hash, timestamp, malware=None):
        if malware is None:
            params =  "hash='{hash}', timestamp={timestamp}"
        else:
            params = "hash='{hash}', timestamp={timestamp}, malware={malware}"
        cmd = ("UPDATE Object SET " + params +
               " UPSERT WHERE hash='{hash}'").format(hash=hash, timestamp=timestamp, malware=malware)
        if self.batch:
            self.commands.append(cmd)
            return self._check_and_batch()
        return self.client.command(cmd)


    def add_flow(self, source_ip, source_port, destination_ip, destination_port, object_hash, timestamp, protocol="", malware=True):
        cmd = ("LET $records=(SELECT FROM Object WHERE hash='{object_hash}'); "  # for some reason can't do subquery and need this silly batch query
               "LET $v1=(SELECT FROM Node WHERE ip='{source_ip}'); "
               "LET $v2=(SELECT FROM Node WHERE ip='{destination_ip}'); "
               "CREATE EDGE Flow "
               "FROM $v1 TO $v2 "
               "SET object = $records[0], protocol='{protocol}', "
               "sourcePort='{source_port}', destinationPort='{destination_port}', "
               "timestamp={timestamp}; "
               "UPDATE $records[0] ADD graphnodes = $v1, graphnodes = $v2; ").format(
                                              source_ip=source_ip, destination_ip=destination_ip,
                                              source_port=source_port, destination_port=destination_port,
                                              object_hash=object_hash, timestamp=timestamp, protocol=protocol
                                            )
        self.add_node(source_ip, timestamp)
        self.add_node(destination_ip, timestamp)
        self.add_object(object_hash, timestamp, malware)
        if self.batch:
            self.commands.append(cmd)
            return self._check_and_batch()
        return self.client.batch(cmd)


