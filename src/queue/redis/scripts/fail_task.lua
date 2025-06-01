-- Atomic fail task operation
local running_key = KEYS[1]
local pending_key = KEYS[2]
local failed_key = KEYS[3]
local dead_key = KEYS[4]
local tasks_key = KEYS[5]
local task_id = ARGV[1]
local now = tonumber(ARGV[2])
local should_retry = ARGV[3] == 'true'

-- Remove from running
local was_running = redis.call('HDEL', running_key, task_id)
if was_running == 0 then
    return 0  -- Task was not running
end

local task_data = redis.call('HGET', tasks_key, task_id)
if not task_data then
    return 0
end

local task = cjson.decode(task_data)
task.retry_count = (task.retry_count or 0) + 1
task.updated_at_timestamp = now

if should_retry and task.retry_count <= task.max_retries then
    -- Retry the task
    task.status = 'Failed'
    redis.call('LPUSH', pending_key, task_id)
    redis.call('SADD', failed_key, task_id)
else
    -- Max retries exceeded
    task.status = 'Dead'
    redis.call('SADD', dead_key, task_id)
end

redis.call('HSET', tasks_key, task_id, cjson.encode(task))
return 1