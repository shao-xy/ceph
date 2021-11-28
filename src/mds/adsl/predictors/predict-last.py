def predict(load_matrix):
	ret = [ row[-1] for row in load_matrix ]
	print(ret)
	return ret
