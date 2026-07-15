local clock = require('clock')
local msgpack = require('msgpack')

local requests = tonumber(os.getenv('TARANTOOL_REQUESTS') or '10000')
local keyspace = tonumber(os.getenv('TARANTOOL_KEYSPACE') or '10000')
local work_dir = os.getenv('TARANTOOL_WORK_DIR') or '.'
local memtx_memory = tonumber(os.getenv('TARANTOOL_MEMTX_MEMORY') or '268435456')

box.cfg({
	work_dir = work_dir,
	memtx_dir = work_dir,
	wal_dir = work_dir,
	vinyl_dir = work_dir,
	wal_mode = 'none',
	log = work_dir .. '/tarantool.log',
	log_level = 1,
	memtx_memory = memtx_memory,
})

local function create_space(name, parts)
	if box.space[name] ~= nil then
		box.space[name]:drop()
	end
	local space = box.schema.space.create(name)
	space:create_index('primary', {type = 'tree', parts = parts})
	return space
end

local kv = create_space('kv', {{1, 'unsigned'}})
local counters = create_space('counters', {{1, 'unsigned'}})
local ttl = create_space('ttl', {{1, 'unsigned'}})
ttl:create_index('expires_at', {type = 'tree', parts = {{3, 'unsigned'}}, unique = false})
local hash = create_space('hash', {{1, 'string'}, {2, 'string'}})
local queue = create_space('queue', {{1, 'unsigned'}})
local members = create_space('members', {{1, 'unsigned'}})
local pq = create_space('pq', {{1, 'unsigned'}})
pq:create_index('score', {type = 'tree', parts = {{2, 'unsigned'}, {1, 'unsigned'}}, unique = true})
local radix = create_space('radix', {{1, 'string'}})

local function key_for(i)
	return ((i - 1) % keyspace) + 1
end

for i = 1, keyspace do
	kv:replace({i, 'value'})
	counters:replace({i, 0})
	ttl:replace({i, 'value', 3600})
	members:replace({i})
	radix:replace({string.format('session:%08d', i), 'value'})
end
hash:replace({'map:key', 'field', 'value'})
queue:truncate()
pq:truncate()
box.snapshot()

local function seconds_for(fn)
	collectgarbage('collect')
	local start = clock.monotonic()
	fn()
	local elapsed = clock.monotonic() - start
	return elapsed * (10000 / requests)
end

local function format_seconds(seconds)
	return string.format('%.6f s', seconds)
end

local function print_row(feature, command, seconds)
	print(string.format('| %s | `%s` | %s |', feature, command, format_seconds(seconds)))
end

print(string.format('Tarantool benchmark: version=%s requests=%d keyspace=%d', box.info.version, requests, keyspace))
print('')
print('| Feature family | Tarantool operation | Seconds / 10k feature cycles |')
print('| --- | --- | ---: |')

print_row('String write', 'space:replace()', seconds_for(function()
	for i = 1, requests do
		kv:replace({key_for(i), 'value'})
	end
end))

print_row('String read', 'space.index.primary:get()', seconds_for(function()
	for i = 1, requests do
		kv.index.primary:get(key_for(i))
	end
end))

print_row('Integer counter', 'space:update({{"+", 2, 1}})', seconds_for(function()
	for i = 1, requests do
		counters:update(key_for(i), {{'+', 2, 1}})
	end
end))

print_row('TTL update', 'space:update({{"=", 3, expires_at}})', seconds_for(function()
	for i = 1, requests do
		ttl:update(key_for(i), {{'=', 3, 3600 + i}})
	end
end))

print_row('Map/hash write', 'space:replace({key, field, value})', seconds_for(function()
	for i = 1, requests do
		hash:replace({'map:key', 'field', 'value'})
	end
end))

print_row('Map/hash read', 'space.index.primary:get({key, field})', seconds_for(function()
	for i = 1, requests do
		hash.index.primary:get({'map:key', 'field'})
	end
end))

print_row('List/deque push+pop', 'space:replace() + space:delete()', seconds_for(function()
	for i = 1, requests do
		queue:replace({i, 'value'})
		queue:delete(i)
	end
end))

print_row('Set add+has', 'space:replace() + space.index.primary:get()', seconds_for(function()
	for i = 1, requests do
		local key = key_for(i)
		members:replace({key})
		members.index.primary:get(key)
	end
end))

print_row('Priority queue push+pop', 'tree index insert + index:min() + delete', seconds_for(function()
	for i = 1, requests do
		pq:replace({i, i, 'value'})
		local tuple = pq.index.score:min()
		if tuple ~= nil then
			pq:delete(tuple[1])
		end
	end
end))

print_row('Roaring bitmap add approximation', 'space:replace() membership index', seconds_for(function()
	for i = 1, requests do
		members:replace({65543})
	end
end))

print_row('Roaring bitmap lookup approximation', 'space.index.primary:get() membership index', seconds_for(function()
	for i = 1, requests do
		members.index.primary:get(65543)
	end
end))

print_row('Sparse bitset add approximation', 'space:replace() membership index', seconds_for(function()
	for i = 1, requests do
		members:replace({key_for(i)})
	end
end))

print_row('Sparse bitset lookup approximation', 'space.index.primary:get() membership index', seconds_for(function()
	for i = 1, requests do
		members.index.primary:get(key_for(i))
	end
end))

print_row('Radix-tree put approximation', 'space:replace() tree string key', seconds_for(function()
	for i = 1, requests do
		radix:replace({string.format('session:%08d', key_for(i)), 'value'})
	end
end))

print_row('Radix-tree prefix scan approximation', 'index:pairs(prefix, {iterator = "GE"})', seconds_for(function()
	for i = 1, requests do
		local seen = 0
		for _, tuple in radix.index.primary:pairs('session:', {iterator = 'GE'}) do
			if string.sub(tuple[1], 1, 8) ~= 'session:' then
				break
			end
			seen = seen + 1
			if seen >= 16 then
				break
			end
		end
	end
end))

print_row('Replication dump', 'msgpack.encode(tuple)', seconds_for(function()
	for i = 1, requests do
		msgpack.encode(kv.index.primary:get(key_for(i)))
	end
end))

local function proc_status_kib(name)
	local file = io.open('/proc/self/status', 'r')
	if file == nil then
		return nil
	end
	for line in file:lines() do
		local key, value = string.match(line, '^([^:]+):%s+(%d+)%s+kB$')
		if key == name then
			file:close()
			return tonumber(value)
		end
	end
	file:close()
	return nil
end

local function kib(value)
	value = tonumber(value) or 0
	return math.floor(value / 1024)
end

local slab = box.slab.info()
print('')
print('Memory summary:')
print('')
print('| Metric | Value |')
print('| --- | ---: |')
print(string.format('| Process RSS | %s KiB |', tostring(proc_status_kib('VmRSS') or 'unknown')))
print(string.format('| memtx_memory configured | %d KiB |', kib(memtx_memory)))
print(string.format('| slab quota used | %d KiB |', kib(slab.quota_used)))
print(string.format('| slab quota size | %d KiB |', kib(slab.quota_size)))
print(string.format('| slab arena used | %d KiB |', kib(slab.arena_used)))
print(string.format('| slab arena size | %d KiB |', kib(slab.arena_size)))
print(string.format('| slab items used | %d KiB |', kib(slab.items_used)))
print(string.format('| slab items size | %d KiB |', kib(slab.items_size)))

os.exit(0)
