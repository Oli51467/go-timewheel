package time_wheel

const (
	// LuaAddTasks 添加任务时，如果存在删除 key 的标识，则将其删除
	// 添加任务时，根据时间（所属的 min）决定数据从属于哪个分片{}
	LuaAddTasks = `
	   local zSetKey = KEYS[1]
	   local deleteSetKey = KEYS[2]
	   local score = ARGV[1]
	   local task = ARGV[2]
	   local taskKey = ARGV[3]
	   redis.call('srem',deleteSetKey,taskKey)
	   return redis.call('zadd',zSetKey,score,task)
	`

	// LuaDeleteTask 删除任务时，将删除 key 的标识置为 true
	LuaDeleteTask = `
	   local deleteSetKey = KEYS[1]
	   local taskKey = ARGV[1]
	   redis.call('sadd',deleteSetKey,taskKey)
	   local scnt = redis.call('scard',deleteSetKey)
       -- 如果是set中的首个元素 则对set设置120s的过期时间
	   if (tonumber(scnt) == 1)
	   then
	       redis.call('expire',deleteSetKey,120)
       end
	   return scnt
	`

	// LuaZRangeTasks 执行任务时，通过 zRange 操作取回所有不存在删除 key 标识的任务
	LuaZRangeTasks = `
	    -- 第一个 key 为存储定时任务的 zSet key
       local zSetKey = KEYS[1]
       -- 第二个 key 为已删除任务 set 的 key
       local deleteSetKey = KEYS[2]
       -- 第一个 arg 为 zRange 检索的 score 左边界
       local score1 = ARGV[1]
       -- 第二个 arg 为 zRange 检索的 score 右边界
       local score2 = ARGV[2]
       -- 获取到已删除任务的集合
       local deleteSet = redis.call('smembers',deleteSetKey)
       -- 根据秒级时间戳对 zSet 进行 zRange 检索，获取到满足时间条件的定时任务
       local targets = redis.call('zrange',zSetKey,score1,score2,'byscore')
       -- 检索到的定时任务直接从时间轮中移除，保证分布式场景下定时任务不被重复获取
       redis.call('zremrangebyscore',zSetKey,score1,score2)
       -- 返回的结果是一个 table
       local reply = {}
       -- table 的首个元素为已删除任务集合
       reply[1] = deleteSet
       -- 依次将检索到的定时任务追加到 table 中
       for i, v in ipairs(targets) do
           reply[#reply+1]=v
       end
       return reply
	`
)
