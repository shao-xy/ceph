#!/usr/bin/env lua

function predict()
	prediction = {}
	lambda = 1/2
	for entry, entry_load_list in ipairs(load_matrix) do
		repeat
			if #entry_load_list == 0 then
				prediction[entry] = 0
				break
			elseif #entry_load_list == 1 then
				prediction[entry] = lambda * entry_load_list[#entry_load_list]
				break
			end

			s = entry_load_list[1]
			for i = 1, #entry_load_list do
				s = s >= 0.1 and s or 0
				s = lambda * (s + entry_load_list[i])
			end
			prediction[entry] = s >= 0.1 and s or 0.0
		until true
	end
	return prediction
end

return predict()
