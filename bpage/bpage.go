package bpage

import (
	"hash/fnv"
	"unsafe"
)

type ID uint64
type TxID uint64

// page has 4 sorts.
const (
	BranchFlag   = 0x01
	LeafFlag     = 0x02
	MetaFlag     = 0x04
	FreelistFlag = 0x10
)

// Data a page of data; It may presentation some data in the disk.
type Data struct {
	ID       ID
	Flags    uint16
	Count    uint16
	Overflow uint32
	Ptr      uintptr
}

type Bucket struct {
	Root     ID     // page id of the bucket's root-level page
	Sequence uint64 // monotonically incrementing, used by NextSequence()
}

// if page is a mata page. It store the struct.And you can restore it from bytes.
type Meta struct {
	Magic    uint32
	Version  uint32
	PageSize int
	Flags    uint32
	Root     Bucket
	Freelist ID
	ID       ID
	TxID     TxID
	Checksum uint64
}

// getMeta if page is
func (d *Data) GetMeta() *Meta {
	return (*Meta)(unsafe.Pointer(&d.Ptr))
}

func (m *Meta) GenSum64() uint64 {
	var h = fnv.New64a()
	_, _ = h.Write((*[unsafe.Offsetof(Meta{}.Checksum)]byte)(unsafe.Pointer(m))[:])
	return h.Sum64()
}
