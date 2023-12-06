PERCENTAGE_SPATIAL_THRESHOLD = 0.5
PERCENTAGE_TEMPORAL_THRESHOLD = 0.2

function calculate_diff_mat(input_matrix)
	local diff_matrix = {}
	for i = 1, #input_matrix do
		local last_row = input_matrix[i - 1]
		local row = input_matrix[i]
		local diff_row = {}
		for j = 1, #row do
			diff_row[j] = row[j] - last_row[j]
		end
		diff_matrix[i] = diff_row
	end
	return diff_matrix
end

function temporal_predict_vector(input_vector)
	if #input_vector == 0 then
		return 0
	end
	local _lambda = 1 / 2
	local _1_lambda = 1 - _lambda
	local s = _lambda * input_vector[1]
	for i = 2, #input_vector - 1 do
		local cur = input_vector[i]
		s = _lambda * (_1_lambda * cur + s)
	end
	s = (1 + _lambda) * input_vector[#input_vector] - s
	return s >= 0 and s or 0
end

function temporal_predict(input_matrix)
	local prediction = {}
	for entry, entry_load_list in ipairs(input_matrix) do
		prediction[entry] = temporal_predict_vector(entry_load_list)
	end
	return prediction
end

function spatial_predict(input_matrix, mode)
	mode = mode or 1
	local sum_vector = {}
	if #input_matrix == 0 then
		return sum_vector
	end
	if mode == 1 then
		for i = 1, #input_matrix[1] do
			local sum = 0
			for entry, entry_load_list in ipairs(input_matrix) do
				sum = sum + entry_load_list[i]
			end
			sum_vector[i] = sum
		end
		--[=[
		if DEBUG then
			PRED_LOG(0, table.concat(sum_vector, ","))
		end
		--]=]
		local col_size = #input_matrix
		avg = temporal_predict_vector(sum_vector) / col_size
	else
		local sum = 0
		for entry, entry_load_list in ipairs(input_matrix) do
			sum = sum + entry_load_list[#entry_load_list]
		end
		avg = sum / #input_matrix
	end
	prediction = {}
	for i = 1, #input_matrix do
		prediction[i] = avg
	end
	--[=[
	if DEBUG then
		PRED_LOG(0, table.concat(prediction, ","))
	end
	--]=]
	return prediction
end

function decouple_temporal_spatial_predict()
	local temporal_matrix = {}
	local spatial_matrix = {}
	for entry, entry_load_list in ipairs(load_matrix) do
		temporal_column = {}
		spatial_column = {}

		for epoch = 1, #entry_load_list-1 do
			local last = entry_load_list[epoch]
			local cur = entry_load_list[epoch+1]
			local diff = cur - last
			if math.abs(diff) > PERCENTAGE_SPATIAL_THRESHOLD * last then
				spatial_column[#spatial_column+1] = cur
				temporal_column[#temporal_column+1] = 0
			elseif math.abs(diff) < PERCENTAGE_TEMPORAL_THRESHOLD * last then
				spatial_column[#spatial_column+1] = 0
				temporal_column[#temporal_column+1] = cur
			elseif diff > 0 then
				spatial_column[#spatial_column+1] = diff
				temporal_column[#temporal_column+1] = last
			else
				spatial_column[#spatial_column+1] = 0
				temporal_column[#temporal_column+1] = cur
			end
		end

		temporal_matrix[entry] = temporal_column
		spatial_matrix[entry] = spatial_column
	end
	-- PRED_LOG(0, table.concat(temporal_matrix, ","))
	-- PRED_LOG(0, table.concat(spatial_matrix, ","))
	-- PRED_LOG(0, #temporal_matrix)
	-- PRED_LOG(0, #spatial_matrix)
	local temporal_predicted = temporal_predict(temporal_matrix)
	local spatial_predicted = spatial_predict(spatial_matrix)
	if DEBUG then
		dump_matrix = function(matrix, name)
			if name then
				print("Dumping matrix " .. name)
			end
			if type(matrix) ~= "table" then
				print(matrix)
			elseif type(matrix[1]) ~= "table" then
				print(table.concat(matrix, ","))
			else
				for entry, entry_load_list in ipairs(matrix) do
					print(table.concat(entry_load_list, ","))
				end
			end
		end
		dump_matrix(temporal_matrix, "temporal_matrix")
		dump_matrix(spatial_matrix, "spatial_matrix")
		dump_matrix(temporal_predicted, "temporal_predicted")
		dump_matrix(spatial_predicted, "spatial_predicted")
	end
	--[==[
	if DEBUG then
		PRED_LOG(0, table.concat(temporal_predicted, ","))
		PRED_LOG(0, table.concat(spatial_predicted, ","))
	end
	--]==]
	local predictions = {}
	local temporal_predicted_sum = 0
	local spatial_predicted_sum = 0
	for i = 1, #temporal_predicted do
		temporal_predicted_sum = temporal_predicted_sum + temporal_predicted[i]
		spatial_predicted_sum = spatial_predicted_sum + spatial_predicted[i]
		predictions[#predictions+1] = temporal_predicted[i] + spatial_predicted[i]
	end
	total = temporal_predicted_sum + spatial_predicted_sum
	if total == 0 then
		PRED_LOG(0, " TEMPORAL_SPATIAL_RATIO " .. temporal_predicted_sum .. " " .. spatial_predicted_sum .. " 0 0")
	else
		PRED_LOG(0, " TEMPORAL_SPATIAL_RATIO " .. temporal_predicted_sum .. " " .. spatial_predicted_sum .. " " .. (temporal_predicted_sum / total) .. " " .. (spatial_predicted_sum / total))
	end
	return predictions
end

return decouple_temporal_spatial_predict()
