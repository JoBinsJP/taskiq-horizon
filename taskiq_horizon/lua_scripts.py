class LuaScripts:
    """
    Port of Laravel Horizon's LuaScripts.php for atomic Redis operations.
    """

    @staticmethod
    def update_metrics() -> str:
        """
        KEYS[1] - The name of the key being updated (e.g., jobs:MyTask)
        KEYS[2] - The name of the key of the metrics group (e.g., measured_jobs)
        ARGV[1] - The runtime in milliseconds of the current job
        """
        return """
            redis.call('hsetnx', KEYS[1], 'throughput', 0)

            redis.call('sadd', KEYS[2], KEYS[1])

            local hash = redis.call('hmget', KEYS[1], 'throughput', 'runtime')

            local throughput = hash[1] + 1
            local runtime = 0

            if hash[2] then
                runtime = ((hash[1] * tonumber(hash[2])) + tonumber(ARGV[1])) / throughput
            else
                runtime = tonumber(ARGV[1])
            end

            redis.call('hmset', KEYS[1], 'throughput', throughput, 'runtime', runtime)
        """

    @staticmethod
    def purge() -> str:
        """
        KEYS[1] - The name of the recent jobs sorted set
        KEYS[2] - The name of the pending jobs sorted set
        ARGV[1] - The prefix of the Horizon keys
        ARGV[2] - The name of the queue to purge
        ARGV[3] - cursor
        """
        return """
            local count = 0
            local cursor = ARGV[3]

            -- Iterate over the recent jobs sorted set
            local scanner = redis.call('zscan', KEYS[1], cursor)
            cursor = scanner[1]

            for i = 1, #scanner[2], 2 do
                local jobid = scanner[2][i]
                local hashkey = ARGV[1] .. jobid
                local job = redis.call('hmget', hashkey, 'status', 'queue')

                -- Delete the pending / reserved jobs in the given queue
                if((job[1] == 'reserved' or job[1] == 'pending') and job[2] == ARGV[2]) then
                    redis.call('zrem', KEYS[1], jobid)
                    redis.call('zrem', KEYS[2], jobid)
                    redis.call('del', hashkey)
                    count = count + 1
                end
            end

            return {count, cursor}
        """

    @staticmethod
    def trim() -> str:
        """
        KEYS[1] - The name of the sorted set to trim
        ARGV[1] - The maximum score value we want to retain (anything BELOW this is deleted)
        """
        return """
            -- 1. Gather the payload IDs that are being trimmed out.
            local ids = redis.call('zrangebyscore', KEYS[1], '-inf', '(-' .. ARGV[1])

            -- 2. Physically discard the backing Hashes so Redis Memory doesn't bloat.
            for i = 1, #ids do
                redis.call('del', ids[i])
            end

            -- 3. Slice the indices from the Sorted Set itself.
            redis.call('zremrangebyscore', KEYS[1], '-inf', '(-' .. ARGV[1])

            return #ids
        """
