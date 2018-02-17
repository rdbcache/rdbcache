--- redis-cli --eval set-expire-key.lua key , trace_id expire_string
---
local res = redis.call('KEYS', KEYS[1] .. "::*")
local expValue = tonumber(ARGV[2])
if next(res) ~= nil then
    if expValue > 0 and string.sub(ARGV[2], 1, 1) ~= '+' then
        return 0
    end
    redis.call('DEL', unpack(res))
end
if expValue == 0 then
    return 0
end
if expValue < 0 then
    expValue = -expValue
end
redis.call('SETEX', KEYS[1] .. "::" .. ARGV[1], expValue, ARGV[2])
return 1