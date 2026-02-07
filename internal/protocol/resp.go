package protocol

import "fmt"

// RESPType represents the type byte of a RESP value
type RESPType byte

const (
	SimpleString RESPType = '+'
	Error        RESPType = '-'
	Integer      RESPType = ':'
	BulkString   RESPType = '$'
	Array        RESPType = '*'
)

// Value represents a single RESP value
// I use a single struct with a type discriminator rather than an interface
// hierarchy to avoid heap allocations. At ~120 bytes, this stays on the stack.
type Value struct {
	Type   RESPType
	Str    string
	Num    int64
	Bulk   []byte
	Array  []Value
	IsNull bool
}

// Factory functions for creating RESP values

func SimpleStringVal(s string) Value {
	return Value{Type: SimpleString, Str: s}
}

func ErrorVal(msg string) Value {
	return Value{Type: Error, Str: msg}
}

func IntegerVal(n int64) Value {
	return Value{Type: Integer, Num: n}
}

func BulkStringVal(b []byte) Value {
	return Value{Type: BulkString, Bulk: b}
}

func NullBulkString() Value {
	return Value{Type: BulkString, IsNull: true}
}

func ArrayVal(vals []Value) Value {
	return Value{Type: Array, Array: vals}
}

func NullArray() Value {
	return Value{Type: Array, IsNull: true}
}

// Pre-allocated sentinel values for common responses
// These avoid allocations on the hottest paths
var (
	ValOK         = SimpleStringVal("OK")
	ValPong       = SimpleStringVal("PONG")
	ValNullBulk   = NullBulkString()
	ValZero       = IntegerVal(0)
	ValOne        = IntegerVal(1)
	ValNegOne     = IntegerVal(-1)
	ValNegTwo     = IntegerVal(-2)
	ValEmptyArray = ArrayVal([]Value{})
)

// Common error values
var (
	ErrWrongType = ErrorVal("WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrSyntax    = ErrorVal("ERR syntax error")
	ErrNotInteger = ErrorVal("ERR value is not an integer or out of range")
	ErrNotFloat  = ErrorVal("ERR value is not a valid float")
	ErrNoSuchKey = ErrorVal("ERR no such key")
)

// ErrWrongArgNum returns an error for wrong argument count
func ErrWrongArgNum(cmd string) Value {
	return ErrorVal(fmt.Sprintf("ERR wrong number of arguments for '%s' command", cmd))
}

// ErrUnknownCmd returns an error for unknown commands
func ErrUnknownCmd(cmd string) Value {
	return ErrorVal(fmt.Sprintf("ERR unknown command '%s'", cmd))
}
