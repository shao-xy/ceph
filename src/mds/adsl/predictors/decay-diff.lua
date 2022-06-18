#!/usr/bin/env lua

function predict()
	prediction = {}
	lambda = 1/2
	_1_lambda = 1 - lambda
	-- PRED_LOG(0, "1 - lambda = " .. _1_lambda)
	for entry, entry_load_list in ipairs(load_matrix) do
		-- PRED_LOG(0, "entry = " .. entry)
		-- PRED_LOG(0, "entry_load_list = " .. table.concat(entry_load_list, ","))
		repeat
			if #entry_load_list == 0 then
				prediction[entry] = 0
				break
			elseif #entry_load_list == 1 then
				prediction[entry] = lambda * entry_load_list[#entry_load_list]
				break
			end

			-- PRED_LOG(0, " start.")
			s = lambda * entry_load_list[1]
			-- PRED_LOG(0, " s = " .. s)
			for i = 2,(#entry_load_list-1) do
				cur = entry_load_list[i]
				s = lambda * (_1_lambda * cur + s)
				-- PRED_LOG(0, " s = " .. s)
			end
			s = (1 + lambda) * entry_load_list[#entry_load_list] - s
			prediction[entry] = s >= 0 and s or 0.0
			-- PRED_LOG(0, "prediction[entry] = " .. prediction[entry])
		until true
	end
	-- PRED_LOG(0, "Prediction: " .. table.concat(prediction, ","))
	return prediction
end

return predict()
