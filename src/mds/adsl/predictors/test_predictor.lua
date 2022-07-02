#!/usr/bin/env lua

load_matrix = {
	{5,2,0,0,0},
	{2,5,2,0,0},
	{0,2,5,2,0},
	{0,0,2,5,2},
	{0,0,0,2,5},
}

predicted = require("waterfall")
print(table.concat(predicted, ","))
