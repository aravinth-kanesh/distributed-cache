package persistence

// mutatingCommands is the set of commands that modify state and must
// be logged to the AOF. I maintain this as a separate set rather than
// tagging each handler, keeping the persistence concern cleanly
// separated from command dispatch.
var mutatingCommands = map[string]bool{
	// Key management
	"DEL": true, "EXPIRE": true, "PEXPIRE": true, "PERSIST": true,
	"RENAME": true, "FLUSHDB": true, "FLUSHALL": true,

	// String commands
	"SET": true, "SETNX": true, "GETSET": true, "MSET": true,
	"INCR": true, "INCRBY": true, "DECR": true, "DECRBY": true,
	"INCRBYFLOAT": true, "APPEND": true, "SETRANGE": true,

	// List commands
	"LPUSH": true, "RPUSH": true, "LPOP": true, "RPOP": true,
	"LSET": true, "LTRIM": true, "LREM": true, "LINSERT": true,
	"LPUSHX": true, "RPUSHX": true, "LMOVE": true,

	// Hash commands
	"HSET": true, "HDEL": true, "HMSET": true, "HSETNX": true,
	"HINCRBY": true, "HINCRBYFLOAT": true,

	// Set commands
	"SADD": true, "SREM": true, "SPOP": true, "SMOVE": true,
	"SUNIONSTORE": true, "SINTERSTORE": true, "SDIFFSTORE": true,
}

// IsMutating returns true if the command modifies state and needs AOF logging.
func IsMutating(cmdName string) bool {
	return mutatingCommands[cmdName]
}
