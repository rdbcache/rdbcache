--- redis-cli --eval expire-event-unlock.lua key , signature
---
if redis.call("GET",KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end