-- Atomic dequeue operation
local pending_key = KEYS[1]
local running_key = KEYS[2]
local tasks_key = KEYS[3]
local visible_timestamp = tonumber(ARGV[1])
local now = tonumber(ARGV[2])

local task_id = redis.call('RPOP', pending_key)
if not task_id then
    return nil
end

local task_data = redis.call('HGET', tasks_key, task_id)
if not task_data then
    return nil
end

-- Set task as running with visibility timeout
redis.call('HSET', running_key, task_id, visible_timestamp)

-- Update task status
local task = cjson.decode(task_data)
task.status = 'Running'
task.updated_at_timestamp = now
local updated_task_data = cjson.encode(task)
redis.call('HSET', tasks_key, task_id, updated_task_data)

return updated_task_data