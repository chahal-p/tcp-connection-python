#!/usr/bin/env python3

import io
import socket
import threading

class TCPConnection():
  LENGTH_PREFIX = 4
  MAX_DATA_SIZE = 0xffffff

  _sock: socket.socket
  def __init__(self, sock: socket.socket):
    self._sock = sock
    self._send_lock = threading.Lock()
    self._recv_lock = threading.Lock()

  def _recv(self, size):
    with io.BytesIO() as buf:
      with self._recv_lock:
        while size > 0:
          data = self._sock.recv(size, socket.MSG_WAITALL)
          if data:
            buf.write(data)
            size -= len(data)
          else:
            raise ConnectionClosedError()
      return buf.getvalue()

  def recv(self):
    le = int.from_bytes(self._recv(self.LENGTH_PREFIX))
    if le == 0:
      return b''
    data = self._recv(le)
    if len(data) != le:
      raise ValueError('Complete data did not arrive, missing bytes %s' % (le - len(data)))
    return data
  
  def send(self, data: bytes):
    le = len(data)
    if le > self.MAX_DATA_SIZE:
      raise ValueError('Max allowed data size %d' % self.MAX_DATA_SIZE)
    try:
      with self._send_lock:
        self._sock.sendall(int.to_bytes(le, length=self.LENGTH_PREFIX) + data)
    except (BrokenPipeError, ConnectionResetError, TimeoutError) as e:
      raise ConnectionClosedError(e)

  def remote_address(self):
    return self._sock.getpeername()

  def close(self):
    self._sock.close()

class ConnectionClosedError(Exception): pass
