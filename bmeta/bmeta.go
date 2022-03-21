package bmeta

import (
	"errors"
	"hash/fnv"
	"unsafe"

	"github.com/ranhaoliuLeo/bottle/bpage"
)

// magic num indicate that file is bottle db file
const Magic uint32 = 0xED0CDBBD

var (
	ErrInvalid         = errors.New("invalid database")
	ErrVersionMismatch = errors.New("version mismatch")
	ErrChecksum        = errors.New("checksum error")
)

// if page is a mata page. It store the struct.And you can restore it from bytes.
type Data struct {
	Magic    uint32
	Version  uint32
	PageSize int
	Flags    uint32
	Root     Bucket
	Freelist bpage.ID
	ID       bpage.ID
	TxID     bpage.TxID
	Checksum uint64
}

type Bucket struct {
	Root     bpage.ID     // page id of the bucket's root-level page
	Sequence uint64 // monotonically incrementing, used by NextSequence()
}

// GenSum64 use sum to verify the 
func (m *Data) GenSum64() uint64 {
	var h = fnv.New64a()
	_, _ = h.Write((*[unsafe.Offsetof(Data{}.Checksum)]byte)(unsafe.Pointer(m))[:])
	return h.Sum64()
}

// Check: we have need to check db quality. A good way is to check the meta data. So we have this function.
func (m *Data) Check() error {
	if m.Magic != Magic {
		return ErrInvalid
	} else if m.Version != 2 {
		return ErrVersionMismatch
	} else if m.Checksum != 0 && m.Checksum != m.GenSum64() {
		return ErrChecksum
	}
	return nil
}

// getMeta if page is meta page
func Get(d *bpage.Data) *Data {
	return (*Data)(unsafe.Pointer(&d.Ptr))
}
