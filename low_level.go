package billyfs

import (
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

type TxSetter interface {
	Set(convertible fdb.KeyConvertible, value []byte)
}

//this interface is satisfied by fdb.Transaction
type TxGetter interface {
	Get(convertible fdb.KeyConvertible) fdb.FutureByteSlice
}

type NarrowGetter interface {
	Get(convertible fdb.KeyConvertible) FutureGetter
}

type NarrowGetterCast struct {
	tx TxGetter
}

func (g *NarrowGetterCast) Get(convertible fdb.KeyConvertible) FutureGetter {
	return g.tx.Get(convertible)
}

type FutureGetter interface {
	Get() ([]byte, error)
}

func WriteBlock(setter TxSetter, getter NarrowGetter, key fdb.Key, op writeOp) (ret int, err error) {
	partial := len(op.what) != op.pageSize
	if len(op.what)+op.offset > op.pageSize {
		return 0, fmt.Errorf("error_wrong_write_size Size:%v Want:%v", op.pageSize, len(op.what)+op.offset)
	}
	ret = 0
	if !partial {
		setter.Set(key, op.what)
		ret = len(op.what)
	} else {

		var data []byte
		data, err = getter.Get(key).Get()

		var buff []byte

		switch {
		//trim C
		case op.offset == 0:
			buff = op.what
			ret = len(op.what)
			break
		default: // A,B,D,E
			buff = make([]byte, len(op.what)+op.offset)
			copy(buff, data[0:op.offset])
			ret = copy(buff[op.offset:], op.what)
			break
		}

		setter.Set(key, buff)
	}

	return ret, err
}
