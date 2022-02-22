# -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
# vim: ts=8 sw=2 smarttab
import sys
import logging
import socket
import ctypes
from threading import Thread

from commondefs import WEBSERVER_LISTEN_PORT

def init_logging():
    logging.basicConfig(level = logging.INFO, format = '%(asctime)s [%(tag)s] %(message)s')
    logger = logging.getLogger('MAIN')
    logger.setLevel(logging.INFO)
    return logger

g_logger = None
log_debug = None
log_info = None
log_warn = None
log_err = None

def log(func, tag, message):
    for line in message.strip().split('\n'):
        func(line, extra={'tag': tag})

def to_int(data: bytes) -> int:
    return int.from_bytes(data, byteorder='little')

class ServingThread(Thread):
    def __init__(self, predictor, clientsockettuple):
        Thread.__init__(self)
        self.predictor = predictor
        self.clientsocket, self.addr = clientsockettuple

    def recv(self):
        header = self.clientsocket.recv(8)
        if not header:    return None
        #print(f'Header: {header}')
        row, col = to_int(header[:4]), to_int(header[4:])
        #sys.stdout.write(f'{self.addr}: matrix {row} * {col}\n')
        #sys.stdout.flush()
        log(log_info, 'RECV', f'{self.addr}: matrix {row} * {col}\n')
        load_mat = [ None ] * row
        for i in range(row):
            load_mat[i] = [ None ] * col
            cur_row = load_mat[i]
            for j in range(col):
                cur_row[j] = to_int(self.clientsocket.recv(4))
        #sys.stdout.write(f'Received: {load_mat}\n')
        #sys.stdout.flush()
        log(log_debug, 'RECV', f'Received: {load_mat}\n')
        return load_mat

    def send(self, pred_load):
        buf = (ctypes.c_double * len(pred_load))()
        buf[:] = pred_load
        self.clientsocket.send(bytes(buf))

    def cleanup(self):
        self.clientsocket.close()

    def run(self):
        #sys.stdout.write(f'Client connected: {self.addr}\n')
        #sys.stdout.flush()
        try:
            while True:
                load_matrix = self.recv()
                if not load_matrix:    break
                pred_load = self.predictor.predict(load_matrix)
                #sys.stdout.write(f'Predicted: {pred_load}\n')
                #sys.stdout.flush()
                #log(log_debug, 'PRED', f'Predicted: {pred_load}\n')
                log(log_info, 'PRED', f'Predicted end.')
                self.send(pred_load)
        except ConnectionResetError:
            pass # Client closed
        except ConnectionError as e:
            #sys.stdout.write('Connection error: %s\n' % e.strerror)
            #sys.stdout.flush()
            log(log_warn, 'PRED', 'Connection error: %s\n' % e.strerror)
        finally:
            self.cleanup()

class Server(Thread):
    def __init__(self, predictor):
        Thread.__init__(self)
        self._thrdpool = []
        self._predictor = predictor

        global g_logger, log_debug, log_info, log_warn, log_err
        if not g_logger:
            g_logger = init_logging()
            log_debug = g_logger.debug
            log_info = g_logger.info
            log_warn = g_logger.warn
            log_err = g_logger.error

    def run(self):
        log(log_info, 'MAIN', 'Server thread launch!')
        serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #serversocket.bind((socket.gethostname(), WEBSERVER_LISTEN_PORT))
        serversocket.bind(('', WEBSERVER_LISTEN_PORT))
        serversocket.listen(5)
        try:
            while True:
                new_thrd = ServingThread(self._predictor, serversocket.accept())
                new_thrd.start()
                self._thrdpool.append(new_thrd)
        except KeyboardInterrupt:
            log(log_err, 'MAIN', 'Ctrl-C caught, exiting...')
    
    def __del__(self):
        for thrd in self._thrdpool:
            thrd.join()

