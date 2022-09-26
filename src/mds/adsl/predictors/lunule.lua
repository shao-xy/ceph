DECAY_FAC = DECAY_FAC or (1/2)

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
	last = load_list[#load_list]
	d = DECAY_FAC * last - s
	s = last + d
	s = s > 0 and s or 0.0
	d = d > (-last) and d or (-last)
	return s, d
end

function predict()
	prediction = {}

	local subtree_size = #load_matrix
	if subtree_size == 0 then return {} end

	-- Corner case: no data?
	if #load_matrix[1] == 0 then
		for entry = 1, #load_matrix do
			prediction[entry] = 0
		end
		return prediction
	end

	-- Calculate alpha, beta first
	local new_hits = 0
	local old_hits = 0
	local num_of_old_entries = 0
	for entry, entry_load_list in ipairs(load_matrix) do
		local current = entry_load_list[#entry_load_list]
		local oldest_epoch = #entry_load_list - 5
		if oldest_epoch < 1 then oldest_epoch = 1 end
		local is_new = true
		for i = #entry_load_list - 1, oldest_epoch, -1 do
			if entry_load_list[i] > 0 then
				is_new = false
				break
			end
		end
		if is_new then
			new_hits = new_hits + current
		else
			old_hits = old_hits + current
			num_of_old_entries = num_of_old_entries + 1
		end
	end
	local all_hits = new_hits + old_hits
	local alpha = (all_hits > 0) and (old_hits / all_hits) or 0.1
	local beta = (subtree_size > 0) and ((subtree_size - num_of_old_entries) / subtree_size) or 0.1
	print(alpha, beta)
	if alpha < 0.1 then alpha = 0.1 end
	if beta < 0.1 then beta = 0.1 end

	-- Sum up all delta load (uniformly distribution)
	local last_epoch_delta_total = 0
	for entry, entry_load_list in ipairs(load_matrix) do
		local temporal_load, temporal_delta = temporal_predict(entry_load_list)
		last_epoch_delta_total = last_epoch_delta_total + temporal_delta
		prediction[entry] = temporal_load
	end
	local spatial_load_each = last_epoch_delta_total / subtree_size

	for entry, entry_load_list in ipairs(load_matrix) do
		local calculated = alpha * prediction[entry] + beta * spatial_load_each
		prediction[entry] = (calculated > 0) and calculated or 0
	end
	return prediction
end

return predict()
