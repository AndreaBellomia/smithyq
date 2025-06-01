-- Cleanup old tasks
local tasks_key = KEYS[1]
local completed_key = KEYS[2]
local dead_key = KEYS[3]
local cleanup_threshold = tonumber(ARGV[1])

local cleaned_count = 0

-- Clean completed tasks
local completed_tasks = redis.call('SMEMBERS', completed_key)
for i = 1, #completed_tasks do
    local task_id = completed_tasks[i]
    local task_data = redis.call('HGET', tasks_key, task_id)
    if task_data then
        local task = cjson.decode(task_data)
        if task.updated_at_timestamp and task.updated_at_timestamp < cleanup_threshold then
            redis.call('HDEL', tasks_key, task_id)
            redis.call('SREM', completed_key, task_id)
            cleaned_count = cleaned_count + 1
        end
    end
end

-- Clean dead tasks
local dead_tasks = redis.call('SMEMBERS', dead_key)
for i = 1, #dead_tasks do
    local task_id = dead_tasks[i]
    local task_data = redis.call('HGET', tasks_key, task_id)
    if task_data then
        local task = cjson.decode(task_data)
        if task.updated_at_timestamp and task.updated_at_timestamp < cleanup_threshold then
            redis.call('HDEL', tasks_key, task_id)
            redis.call('SREM', dead_key, task_id)
            cleaned_count = cleaned_count + 1
        end
    end
end

return cleaned_count