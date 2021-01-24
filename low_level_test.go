package billyfs

import (
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

type example struct {
	pageSize    int
	dataInDb    []byte
	offset      int
	dataToWrite []byte
	error       error

	wasRead            bool
	keyPresentedOnRead fdb.KeyConvertible
	w                  *write
}

func (e *example) Set(convertible fdb.KeyConvertible, value []byte) {
	e.w.Set(convertible, value)
}

func (example) Key() fdb.Key {
	return []byte{0xDE, 0xAD}
}

func (e example) Op() writeOp {
	return writeOp{e.dataToWrite, nil, e.offset, e.pageSize}
}

type write struct {
	key     fdb.KeyConvertible
	written []byte
}

func (w *write) Set(convertible fdb.KeyConvertible, value []byte) {
	w.key = convertible
	w.written = value
}

type exactGetter struct {
	data     []byte
	toReturn error
}

func (e exactGetter) Get() ([]byte, error) {
	return e.data, e.toReturn
}

func (e *example) Get(convertible fdb.KeyConvertible) FutureGetter {
	e.keyPresentedOnRead = convertible
	e.wasRead = true
	return &exactGetter{e.dataInDb, e.error}
}

func NewExample(pageSize int, dbSlice []byte, offset int, writeSlice []byte, errorToReturn error) example {
	return example{pageSize: pageSize, dataInDb: dbSlice, offset: offset, dataToWrite: writeSlice, keyPresentedOnRead: nil, error: errorToReturn, w: &write{}}
}

func ExampleWriteBlock() {
	//another option is to run 2 bit operations, 1 zeroing bits for writing,
	// 2nd setting bits to be written
	// partial write is funky!

	// expand & combine
	// A merge
	// ----- <- pageSize
	// --    <- data
	//  --   <- op.what

	// B merge
	// ----- <- pageSize
	// ----- <- data
	//  ---- <- op.what

	// C
	// ----- <- pageSize
	// ----- <- data
	// --    <- op.what --> trim?

	// D
	// ----- <- pageSize
	// ----- <- data
	//  --   <- op.what --> trim 2 & combine

	// E
	// ----- <- pageSize
	// ----  <- data
	//  --   <- op.what --> trim 2 & combine
	//  ^    op.offset

	examples := []example{
		NewExample(3, []byte{0x00, 0x01, 0x02}, 0, []byte{0x03, 0x04, 0x05}, nil),
		NewExample(3, []byte{0x00, 0x01, 0x02}, 0, []byte{0x04, 0x05}, nil),
		NewExample(3, []byte{0x00, 0x01, 0x02}, 1, []byte{0x04, 0x05}, nil),
		NewExample(3, []byte{0x00, 0x01, 0x02}, 1, []byte{0x05}, nil),
		NewExample(3, []byte{0x00, 0x01, 0x02}, 1, []byte{0x05, 0x06, 0x07}, nil),
	}

	for i := range examples {
		num, err := WriteBlock(&examples[i], &examples[i], examples[i].Key(), examples[i].Op())
		fmt.Printf("%v,%v,R:%v,%v,W:%v\n", num, err, examples[i].wasRead, examples[i].keyPresentedOnRead, examples[i].w)
	}

	// Output:
	// 3,<nil>,R:false,<nil>,W:&{[222 173] [3 4 5]}
	// 2,<nil>,R:true,\xde\xad,W:&{[222 173] [4 5]}
	// 2,<nil>,R:true,\xde\xad,W:&{[222 173] [0 4 5]}
	// 1,<nil>,R:true,\xde\xad,W:&{[222 173] [0 5]}
	// 0,error_wrong_write_size Size:3 Want:4,R:false,<nil>,W:&{<nil> []}

}
