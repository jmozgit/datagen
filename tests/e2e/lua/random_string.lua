local math = math
local string = string

function random_array()
    local arr = {}
    local cnt = math.random(10)
    for i = 1, cnt do
        arr[i] = (33 + math.random(93))
    end
    return arr
end

return string.char(unpack(random_array()))