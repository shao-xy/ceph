-- #!/usr/bin/env lua

--DO_STREAM_MATCH_LENGTH_OFFSET = 5
DO_STREAM_MATCH_LENGTH_OFFSET = 1
--DECAY_FAC = DECAY_FAC or (1/2)
DECAY_FAC = DECAY_FAC or (1/4)
-- dout = dout or function (...)
-- 	PRED_LOG(0, ...)
-- end
-- print('DECAY_FAC is set to ' .. DECAY_FAC)

function temporal_predict(load_list)
	if #load_list == 0 then return 0 end
	if #load_list == 1 then return load_list[1] * DECAY_FAC end
	local s = DECAY_FAC * load_list[1]
	local cur = 0
	local _1_DECAY_FAC = 1 - DECAY_FAC
	for i = 2, (#load_list - 1) do
		cur = load_list[i]
		s = DECAY_FAC * (_1_DECAY_FAC * cur + s)
	end
	s = (1 + DECAY_FAC) * load_list[#load_list] - s
	return s > 0 and s or 0.0
end

function fast_temporal_predict()
	prediction = {}
	for entry, entry_load_list in ipairs(load_matrix) do
		prediction[entry] = temporal_predict(entry_load_list)
	end
	return prediction
end

function distribute_float_vertex_load(next_vertex, next_val)
	local floor = math.floor(next_vertex)
	local targets = {}
	if floor == next_vertex then
		targets[floor] = next_val
		return targets
	end
	
	local ceil = floor + 1
	targets[floor] = next_val * (next_vertex - floor)
	targets[ceil] = next_val * (ceil - next_vertex)
	return targets
end

function predict()
	col_len = #load_matrix[1]
	-- Corner case?
	if col_len <= DO_STREAM_MATCH_LENGTH_OFFSET then
		return fast_temporal_predict()
	end

	-- Init
	streams_coll = {}
	streams_coll.streams = {}
	streams_coll.new_streams = {}
	--[===[
	setmetatable(streams_coll.streams, {
		__index = function (t, k)
			t[k] = {}
			return t[k]
		end
	})
	--]===]
	streams_coll.cutdown_streams = {}
	streams_coll.epoch = 0
	streams_coll.switch_epoch = function (epoch)
		-- dout('streams_coll.switch_epoch', epoch)
		if epoch <= streams_coll.epoch then return end

		place_cutdown_streams = function (streams, left_epoch)
			for entry, entry_streams in pairs(streams) do
				for _, entry_stream in ipairs(entry_streams) do
					entry_stream.epoch = left_epoch
					table.insert(streams_coll.cutdown_streams, entry_stream)
				end
			end
		end

		place_cutdown_streams(streams_coll.streams, streams_coll.epoch - 1)
		if epoch == streams_coll.epoch + 1 then
			streams_coll.streams = streams_coll.new_streams
		else
			place_cutdown_streams(streams_coll.new_streams, streams_coll.epoch)
			streams_coll.streams = {}
		end
		streams_coll.new_streams = {}
		streams_coll.epoch = epoch
		if DEBUG then streams_coll.show_streams() end
	end
	streams_coll.adjust = function (entry, epoch, volume)
		-- dout('streams_coll.adjust', entry, epoch, volume)
		if streams_coll.epoch < epoch then streams_coll.switch_epoch(epoch) end

		local streams = streams_coll.streams
		local new_streams = streams_coll.new_streams
		if volume <= 0 then return end
		left = volume

		if new_streams[entry] == nil then new_streams[entry] = {} end
		-- streams_coll.show_streams()

		find_and_match = function (idx)
			-- dout('find_and_match ' .. idx)
			try_match = function (sub_stream)
				if left == 0 then return true end
				-- assert(volume) > 0
				if sub_stream.val <= left then
					vertices = sub_stream.vertices
					table.insert(vertices, entry)
					new_sub_stream = {vertices=vertices, val=sub_stream.val}
					left = left - sub_stream.val
					keep_old_stream = false
				else
					new_vertices = {}
					for _k, _v in pairs(sub_stream.vertices) do
						new_vertices[_k] = _v
					end
					table.insert(new_vertices, entry)
					new_sub_stream = {vertices=new_vertices, val=left}
					sub_stream.val, left = sub_stream.val - left, 0
					keep_old_stream = true
				end
				table.insert(new_streams[entry], new_sub_stream)
				return keep_old_stream
			end

			sub_streams = streams[idx]
			j, n = 1, #sub_streams
			for i = 1, n do
				local sub_stream = sub_streams[i]
				if try_match(sub_stream, entry, volume) then
					if i ~= j then
						sub_streams[j] = sub_streams[i]
						sub_streams[i] = nil
					end
					j = j + 1
				else
					sub_streams[i] = nil
				end
			end
			if #sub_streams == 0 then
				streams[idx] = nil
			end
			return volume
		end

		if left > 0 and rawget(streams, entry - 1) then
			find_and_match(entry - 1)
		end
		if left > 0 and rawget(streams, entry) then
			find_and_match(entry)
		end

		-- dout('streams_coll.adjust flag 2', entry, epoch, volume, left)

		-- New stream
		if left > 0 then
			table.insert(new_streams[entry], {vertices={entry}, val=left})
		end
	end

	streams_coll.show_streams = function()
		io.write('streams={')
		for entry, sub_streams in pairs(streams_coll.streams) do
			io.write('[' .. entry .. ']={')
			for _, stream in pairs(sub_streams) do
				io.write('{vertices={' .. table.concat(stream.vertices, ',') .. '}, val=' .. stream.val.. '},')
			end
			io.write('},')
		end
		io.write('},new_streams={')
		for entry, sub_streams in pairs(streams_coll.new_streams) do
			io.write('[' .. entry.. ']={')
			for _, stream in pairs(sub_streams) do
				io.write('{vertices={' .. table.concat(stream.vertices, ',') .. '}, val=' .. stream.val.. '},')
			end
			io.write('},')
		end
		io.write('}\n')
	end

	-- Match
	for epoch = 1, col_len do
		for entry, entry_load_list in pairs(load_matrix) do
			streams_coll.adjust(entry, epoch, entry_load_list[epoch])
			if DEBUG then streams_coll.show_streams() end
		end
	end

	streams_coll.switch_epoch(col_len + 1)
	predicted = {}
	for _, sub_streams in pairs(streams_coll.streams) do
		for __, stream in pairs(sub_streams) do
			next_idx = temporal_predict(stream.vertices)
			-- dout('next_idx: ' .. next_idx .. ' value:' .. stream.val)
			for vert, val in pairs(distribute_float_vertex_load(next_idx, stream.val)) do
				if vert >= 1 and vert <= #load_matrix then
					predicted[vert] = predicted[vert] and (predicted[vert] + val) or val
				end
			end
		end
	end
	for i = 1, #load_matrix do
		if predicted[i] == nil then predicted[i] = 0 end
	end
	return predicted
end

return predict()
