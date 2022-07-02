-- #!/usr/bin/env lua

STREAM_MATCH_OFFSET = (not STREAM_MATCH_OFFSET) and 5 or STREAM_MATCH_OFFSET

DECAY_FAC = DECAY_FAC or (1/2)

function new_matrix()
	t = {}
	setmetatable(t, {
		-- Visiting non-existing keys calls this function
		__index = function (t, a)
			t[a] = {}
			return t[a]
		end
	})
	return t
end

function calc_diff_matrix()
	diff = new_matrix()
	for entry, entry_load_list in ipairs(load_matrix) do
		for i = 1,(#entry_load_list-1) do
			diff[entry][i] = load_matrix[entry][i+1] - load_matrix[entry][i]
		end
	end
	return diff
end

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

function table_value_table_append(t, k, e)
	if t[k] == nil then
		t[k] = {e}
	else
		table.insert(t[k], e)
	end
end

Stream = {}

-- Keeps the original stream cut at cutting point, and creates a new short stream
function Stream.cut(orig_stream, cutting_point)
	local orig_vlist = orig_stream.vlist
	local last_start = orig_vlist[#orig_vlist-1]
	local last_end = orig_vlist[#orig_vlist]
	local new_stream = nil
	if cutting_point > last_start and cutting_point < last_end then
		new_stream = {values={orig_stream.values[#orig_stream.values]}, epoch=orig_stream.epoch, vlist={cutting_point, last_end}}
		orig_vlist[#orig_vlist] = cutting_point
	end
	return new_stream
end

-- Always generates new streams
function Stream.split(orig_stream, substream_vols)
	local s = 0
	local last = orig_stream.values[#orig_stream.values]
	for _, v in pairs(substream_vols) do
		s = s + v
	end

	if s > last then
		substream_vols = {last}
	elseif s < last then
		substream_vols[#substream_vols+1] = (last - s)
	end
	local new_streams = {}
	for _, vol in pairs(substream_vols) do
		fac = vol / last
		local new_stream = {values={}, epoch=orig_stream.epoch, vlist={}}
		for i, v in ipairs(orig_stream.values) do
			new_stream.values[i] = math.floor(v * fac)
		end
		for i, v in ipairs(orig_stream.vlist) do
			new_stream.vlist[i] = v
		end
		new_streams[#new_streams+1] = new_stream
	end
	return new_streams
end

function Stream.append(stream, flow)
	if stream.values[#stream.values] == flow.from then
		table.insert(stream.values, flow.val)
		stream.epoch = flow.epoch
		table.insert(stream.vlist, flow.to)
	end
	return stream
end

function match_delta_to_flows(diff_matrix, epoch)
	-- Flows
	flows = new_matrix()
	-- external: void -> dir
	ext_flows = {}
	flows_cnt = 0
	debts = {}
	for entry = 1, #diff_matrix do
		delta = diff_matrix[entry][epoch]
		if delta > 0 then
			for debt_entry, debt in pairs(debts) do
				if delta > debt then
					-- if not flows[debt_entry] then flows[debt_entry] = {} end
					flows[debt_entry][entry] = debt
					flows_cnt = flows_cnt + 1
					debts[debt_entry] = nil
					delta = delta - debt
				elseif delta <= debt then
					-- if not flows[debt_entry] then flows[debt_entry] = {} end
					flows[debt_entry][entry] = delta
					flows_cnt = flows_cnt + 1
					debts[debt_entry] = debt - delta
					if debts[debt_entry] == 0 then debts[debt_entry] = nil end
					delta = 0
					break
				end
			end
			if delta > 0 then
				ext_flows[entry] = delta
			end
		elseif delta < 0 then
			debts[entry] = -delta
		end
	end

	-- Unmatched debts?
	for debt_entry, debt in pairs(debts) do
		ext_flows[debt_entry] = -debt
	end

	return flows, ext_flows, flows_cnt
end

function try_match_stream_to_flow(flow, waterfalls, streams, new_waterfalls, new_streams)
	for last_stream_start=(flow.from-STREAM_MATCH_OFFSET),(flow.from-1) do
		-- if found?
		if streams[last_stream_start] ~= nil then
			for last_stream_end, last_streams in pairs(streams[last_stream_start]) do
				if last_stream_end > flow.from then
					for last_streams_idx, last_stream in pairs(last_streams) do
						last_stream_value = last_stream.values[#last_stream.values]
						if last_stream_value <= flow.val then
							-- Split to 2 streams
							table_value_table_append(waterfalls[last_stream_end], flow.from, Stream.cut(last_stream, flow.from))
							table.remove(last_streams, last_streams_idx)
							-- if waterfalls[flow.from] == nil then waterfalls[flow.from] = {} end
							table_value_table_append(new_waterfalls[flow.to], flow.from, Stream.append(last_stream, flow))
							flow.val = flow.val - last_stream_value
						else
							-- Split to 3 streams
							local new_streams = Stream.split(last_stream, {flow.val})
							last_streams[last_streams_idx] = new_streams[2]
							local truncated_stream = new_streams[1]
							table_value_table_append(waterfalls[last_stream_end], flow.from, Stream.cut(truncated_stream, flow.from))
							table_value_table_append(new_waterfalls[flow.to], flow.from, Stream.append(truncated_stream, flow))
							flow.val = 0
						end
						if flow.val == 0 then break end
					end
				end
				if flow.val == 0 then break end
			end
		end
		if flow.val == 0 then break end
	end
	return matched
end

function match_streams_to_flows(epoch, flows, waterfalls, streams, new_waterfalls, new_streams)
	for flow_from, flow_sub_list in pairs(flows) do
		for flow_to, flow_val in pairs(flow_sub_list) do
			try_match_stream_to_flow({from=flow_from, to=flow_to, val=flow_val, epoch=epoch}, waterfalls, streams, new_waterfalls, new_streams)
		end
	end
	for flow_from, flow_sub_list in pairs(flows) do
		for flow_to, flow_val in pairs(flow_sub_list) do
			if flow_val ~= 0 then
				-- if new_waterfalls[flow_to] == nil then new_waterfalls[flow_to] = {} end
				if new_waterfalls[flow_to][flow_from] == nil then
					new_waterfalls[flow_to][flow_from]={ {values={flow_val}, epoch=epoch, vlist={flow_from, flow_to}} }
				else
					local streams = new_waterfalls[flow_to][flow_from]
					local total, stream_vals = 0, nil
					for _, stream in pairs(streams) do
						total = total + stream.values[#stream.values]
					end
					for _, stream in pairs(streams) do
						stream_vals = stream.values
						stream_vals[#stream_vals] = stream_vals[#stream_vals] + stream_vals[#stream_vals] * flow_val / total
					end
				end
				-- if new_streams[flow_from] == nil then new_streams[flow_from] = {} end
				new_streams[flow_from][flow_to] = new_waterfalls[flow_to][flow_from]
			end
		end
	end
end

function predict()
	local diff_matrix = calc_diff_matrix()
	-- 2-D array for all streams collection
	-- This table collects all flow lists. Suppose we have streams like this:
	--     A =30=> B =20=> C
	-- waterfalls contains 2-D array element with key (C, B) and value (also a table)
	--     {values={30,20}, epoch=2, vlist={A,B,C}}
	-- As the flow grows, suppose we have:
	--     A =30=> B =20=> C =25=> D
	-- New waterfalls drops key (C, B) and replace it with new key (D, C) and value
	--     {values={30,20,25}, epoch=3, vlist={A,B,C,D}}
	-- In case multiple streams might exist with the same starting and ending index, this
	-- design has to be modified to a list containing such structures
	local waterfalls = {}
	-- Stream pointers at epochs
	-- This table contains references to table "waterfalls" with dimensions swapped in keys.
	--     streams[A][B] = waterfalls[B][A]
	-- This design is used to quickly match "A->B" to "B->C" since it costs much to search
	-- in table "waterfalls" in a reverse way.
	-- Both these two tables are updated by replacing old with new EACH line.
	local streams = {}
	local ext_stream_val = 0
	for epoch = 1, #diff_matrix[1] do
		repeat
			-- New streams collection
			local new_waterfalls = new_matrix()
			-- New stream pointer
			local new_streams = new_matrix()
			local flows, ext_flows, flows_cnt = match_delta_to_flows(diff_matrix, epoch)

			-- External stream?
			local ext_stream_val = 0
			for _, flowval in pairs(ext_flows) do
				ext_stream_val = ext_stream_val + flowval
			end

			---- Match flows!
			-- No flows?
			if flows_cnt == 0 then break end
			
			-- Match!
			match_streams_to_flows(epoch, flows, waterfalls, streams, new_waterfalls, new_streams)

			-- Unset metatable:
			-- Variables "waterfalls" and "streams" will refer to these two tables later
			-- We should keep values nil when visiting non-existing keys in loops.
			setmetatable(new_waterfalls, nil)
			setmetatable(new_streams, nil)
			waterfalls = new_waterfalls
			streams = new_streams
		until true

		-- Always decay to half
		ext_stream_val = ext_stream_val * DECAY_FAC
	end

	local predicted_diff = {}
	-- External streams first: distribute to all nodes
	local splice = ext_stream_val / #diff_matrix
	for i = 1, #diff_matrix do predicted_diff[i] = splice end

	-- For each stream in waterfalls, predict the next target and load value
	for rev_start, streams_list in pairs(waterfalls) do
		for rev_direction, streams in pairs(streams_list) do
			for stream_idx, stream in pairs(streams) do
				local next_vertex = temporal_predict(stream.vlist)
				local next_val = temporal_predict(stream.values)
				for vert, val in pairs(distribute_float_vertex_load(next_vertex, next_val)) do
					predicted_diff[vert] = predicted_diff[vert] + val
				end
			end
		end
	end

	-- Generate prediction array
	local prediction = {}
	for entry, entry_load_list in pairs(load_matrix) do
		local predicted_load = entry_load_list[#entry_load_list] + predicted_diff[entry]
		-- Fix loads: clear negative values
		prediction[entry] = predicted_load > 0 and predicted_load or 0
	end

	return prediction
end

return predict()
