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

--[=[
load_matrix = {
	{5,2,0,0},
	{2,5,2,0},
	{0,2,5,2},
	{0,0,2,5},
	{0,0,0,2},
}
--]=]

load_matrix = {
	{181,192,245,188,46,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,37556,28511},
	{0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},
	{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},
}


DEBUG = true
--predicted = require("waterfall")
predicted = require("lunule")
print(table.concat(predicted, ","))
