--- redis-cli --eval expire-event-lock.lua key , signature timeout
---
return redis.call("SET", KEYS[1], ARGV[1], "NX", "EX", ARGV[2])
