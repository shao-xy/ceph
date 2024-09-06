--DECAY_FAC = DECAY_FAC or (1/4)
DECAY_FAC = DECAY_FAC or 1/2

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

print(temporal_predict(arg))
