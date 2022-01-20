import sys
sys.path.insert(0, '/home/ceph/LSTNet/')

from predict import LSTNetPredictor as LSP

class Predictor():
	def __init__(self):
		self._pred = LSP()
		#pass

	def predict(self, load_matrix):
		#return [ la[-1] for la in load_matrix ]
		return self._pred.predict(load_matrix)
