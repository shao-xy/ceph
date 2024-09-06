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

---[=[
load_matrix = {
	{5,2,0,0},
	{2,5,2,0},
	{0,2,5,2},
	{0,0,2,5},
	{0,0,0,2},
}
--]=]

--[=[
load_matrix = {
	{181,192,245,188,46,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,37556,28511},
	{0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},
	{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},
}
--]=]


DEBUG = os.getenv('DEBUG') or false
if DEBUG then
	print('DEBUG mode is on!')
end

decay_fac_str = os.getenv('DECAY_FAC')
DECAY_FAC = decay_fac_str and tonumber(decay_fac_str) or nil

function PRED_LOG (lvl, ...)
	io.write(lvl .. ' ')
	print(...)
end

function dout(...)
	if DEBUG then
		print(...)
	end
end
--predicted = require("waterfall")
--predicted = require("lunule")
--predicted = require("spatial-even")
--predicted = require("stream-match-1")
predicted = require("ceph")
print(table.concat(predicted, ","))
