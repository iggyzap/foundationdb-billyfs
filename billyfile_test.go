package billyfs

import "fmt"

type param struct {
	b         []byte
	off       int64
	writeSize int
}

func ExampleAsWriteOps() {
	var tests = []param{

		//exact page match
		{[]byte{0x00}, 0, 1},
		// two buckets exact match
		{[]byte{0x00, 0x01}, 0, 1},
		//  buckets, all filled
		{[]byte{0x00, 0x01, 0x02}, 0, 1},
		// 2 buckets, first full fill, second half-fill
		{[]byte{0x00, 0x01, 0x02}, 0, 2},
		// 2 buckets, first half-filled, second full-filled
		{[]byte{0x00, 0x01, 0x02}, 1, 2},
		// 3 buckets, first half-filled, second full-filled, 3rd half-filled
		{[]byte{0x00, 0x01, 0x02, 0x03}, 1, 2},
		{[]byte{0x00, 0x01, 0x02, 0x03}, 0, 3},
		{[]byte{0x00, 0x01, 0x02, 0x03}, 1, 3},
		{[]byte{0x00, 0x01, 0x02, 0x03, 0x04}, 2, 3},
	}

	for i := range tests {
		fmt.Printf("%v\n", AsWriteOps(tests[i].b, tests[i].off, tests[i].writeSize))
	}

	// Output:
	// [{[0] [253 0 0] 0 1}]
	// [{[0] [253 0 0] 0 1} {[1] [253 0 1] 0 1}]
	// [{[0] [253 0 0] 0 1} {[1] [253 0 1] 0 1} {[2] [253 0 2] 0 1}]
	// [{[0 1] [253 0 0] 0 2} {[2] [253 0 1] 0 2}]
	// [{[0] [253 0 0] 1 2} {[1 2] [253 0 1] 0 2}]
	// [{[0] [253 0 0] 1 2} {[1 2] [253 0 1] 0 2} {[3] [253 0 2] 0 2}]
	// [{[0 1 2] [253 0 0] 0 3} {[3] [253 0 1] 0 3}]
	// [{[0 1] [253 0 0] 1 3} {[2 3] [253 0 1] 0 3}]
	// [{[0] [253 0 0] 2 3} {[1 2 3] [253 0 1] 0 3} {[4] [253 0 2] 0 3}]

}
