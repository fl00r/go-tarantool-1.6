package tarantool

import (
	"fmt"
)

type Error struct {
	Code uint32
	Msg  string
}

func (tnterr Error) Error() (string) {
	return fmt.Sprintf("%s (0x%x)", tnterr.Msg, tnterr.Code)
}
