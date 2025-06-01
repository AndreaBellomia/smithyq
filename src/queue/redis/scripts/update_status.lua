-- Update task status atomically
local tasks_key = KEYS[1]
local task_id = ARGV[1]
local new_status = ARGV[2]
local now = tonumber(ARGV[3])

local task_data = redis.call('HGET', tasks_key, task_id)
if not task_data then
    return 0
end

local task = cjson.decode(task_data)
task.status = new_status
task.updated_at_timestamp = now
redis.call('HSET', tasks_key, task_id, cjson.encode(task))

return 1