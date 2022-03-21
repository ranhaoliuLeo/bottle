package bpage

type ID uint64

type IDs []ID

func (s IDs) Len() int           { return len(s) }
func (s IDs) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s IDs) Less(i, j int) bool { return s[i] < s[j] }

type TxID uint64

// page has 4 sorts.
const (
	BranchFlag   = 0x01
	LeafFlag     = 0x02
	MetaFlag     = 0x04
	FreelistFlag = 0x10
)

const magic uint32 = 0xED0CDBBD

// Data a page of data; It may presentation some data in the disk.
type Data struct {
	ID       ID
	Flags    uint16
	Count    uint16
	Overflow uint32
	Ptr      uintptr
}
