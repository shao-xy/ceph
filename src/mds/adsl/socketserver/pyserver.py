#!/usr/bin/env python3

# Required from lstnet_datautil.py
logger_name = "lstnet"

import sys
from Server import Server
from Predictor import Predictor

def main():
	Server(Predictor()).start()
	return 0

if __name__ == '__main__':
	sys.exit(main())
