#!/usr/bin/env python3

import sys
import socket

def main():
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect(('localhost', 6787))
	row = 3
	col = 2
	data = 1
	s.send(row.to_bytes(4, byteorder='little'))
	s.send(col.to_bytes(4, byteorder='little'))
	for i in range(6):
		s.send(data.to_bytes(4, byteorder='little'))
		data += 1
	ret = s.recv(1024)
	print(ret)
	#print(float.from_bytes(ret, byteorder='little'))

if __name__ == '__main__':
	sys.exit(main())
