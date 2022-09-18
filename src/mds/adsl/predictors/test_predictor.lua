#!/usr/bin/env lua

--[=[
load_matrix = {
	{5,2,0,0,0},
	{2,5,2,0,0},
	{0,2,5,2,0},
	{0,0,2,5,2},
	{0,0,0,2,5},
}
--]=]

load_matrix = {
	{5,2,0,0},
	{2,5,2,0},
	{0,2,5,2},
	{0,0,2,5},
	{0,0,0,2},
}

DEBUG = true
--predicted = require("waterfall")
predicted = require("lunule")
print(table.concat(predicted, ","))
