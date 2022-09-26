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

	-- Sum up all delta load (uniformly distribution)
	local last_epoch_total = 0
	for entry, entry_load_list in ipairs(load_matrix) do
		last_epoch_total = last_epoch_total + entry_load_list[#entry_load_list]
	end

	local spatial_load_each = last_epoch_total / subtree_size

	for entry, entry_load_list in ipairs(load_matrix) do
		prediction[entry] = spatial_load_each
	end
	return prediction
end

return predict()
