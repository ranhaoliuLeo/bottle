package bfreelist

import (
	"sort"
	"unsafe"

	"github.com/ranhaoliuLeo/bottle/bpage"
	"github.com/ranhaoliuLeo/bottle/constant"
)

type Data struct {
	ids     []bpage.ID                // all free and available free page ids.
	pending map[bpage.TxID][]bpage.ID // mapping of soon-to-be free page ids by tx.
	cache   map[bpage.ID]bool         // fast lookup of all free and pending page ids.
}

func New() *Data {
	return &Data{
		pending: make(map[bpage.TxID][]bpage.ID),
		cache:   make(map[bpage.ID]bool),
	}
}

// read initializes the freelist from a freelist page.
func (f *Data) Read(p *bpage.Data) {
	// If the page.count is at the max uint16 value (64k) then it's considered
	// an overflow and the size of the freelist is stored as the first element.
	idx, count := 0, int(p.Count)
	if count == 0xFFFF {
		idx = 1
		count = int(((*[constant.MaxAllocSize]bpage.ID)(unsafe.Pointer(&p.Ptr)))[0])
	}

	// Copy the list of page ids from the freelist.
	if count == 0 {
		f.ids = nil
	} else {
		ids := ((*[constant.MaxAllocSize]bpage.ID)(unsafe.Pointer(&p.Ptr)))[idx:count]
		f.ids = make([]bpage.ID, len(ids))
		copy(f.ids, ids)

		// Make sure they're sorted.
		sort.Sort(bpage.IDs(f.ids))
	}

	// Rebuild the page cache.
	f.reindex()
}

// reindex rebuilds the free cache based on available and pending free lists.
func (f *Data) reindex() {
	f.cache = make(map[bpage.ID]bool, len(f.ids))
	for _, id := range f.ids {
		f.cache[id] = true
	}
	for _, pendingIDs := range f.pending {
		for _, pendingID := range pendingIDs {
			f.cache[pendingID] = true
		}
	}
}