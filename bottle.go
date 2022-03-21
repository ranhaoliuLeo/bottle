package bottle

import (
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"syscall"
	"unsafe"

	"github.com/ranhaoliuLeo/bottle/bpage"
)

// magic num indicate that file is bottle db file
const magic uint32 = 0xED0CDBBD
const version = 1

// 256TB
const maxMapSize = 0xFFFFFFFFFFFF
const maxMmapStep = 1 << 30 // 1GB
// maxAllocSize is the size used when creating array pointers.
const maxAllocSize = 0x7FFFFFFF

type DB struct {
	pageSize  int
	file      *os.File
	pagePool  sync.Pool
	freelist  *freelist
	mmaplock  sync.RWMutex // Protects mmap access during remapping.
	dataref   []byte
	data      *[maxMapSize]byte
	datasz    int
	meta0     *bpage.Meta
	meta1     *bpage.Meta
	MmapFlags int
}

type freelist struct {
	ids     []bpage.ID                // all free and available free page ids.
	pending map[bpage.TxID][]bpage.ID // mapping of soon-to-be free page ids by tx.
	cache   map[bpage.ID]bool         // fast lookup of all free and pending page ids.
}

func newFreelist() *freelist {
	return &freelist{
		pending: make(map[bpage.TxID][]bpage.ID),
		cache:   make(map[bpage.ID]bool),
	}
}

func Open(path string, mode os.FileMode) (*DB, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, mode)
	if err != nil {
		return nil, err
	}
	dbIns := &DB{
		file: file,
	}
	fileDescInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if fileDescInfo.Size() == 0 {
		// means that file is a brand new file
		if err := dbIns.init(); err != nil {
			log.Fatalf("db init fail, err: %v", err)
			return nil, err
		}
	} else {
		// means that file is a old file,
		// so we should read the first files meta page.
		buf := make([]byte, 0x1000)
		if _, err := dbIns.file.ReadAt(buf[:], 0); err != nil {
			meta := dbIns.getPageFromBuffer(buf, 0).GetMeta()
			if err := meta.Check(); err != nil {
				log.Printf("Err Check db file, err: %v, maybe page size was wrong. plz try agin", err)
				dbIns.pageSize = os.Getpagesize()
			}
			dbIns.pageSize = int(meta.PageSize)
		}
	}

	dbIns.pagePool = sync.Pool{
		New: func() interface{} {
			return make([]byte, dbIns.pageSize)
		},
	}

	// Memory map the data file.
	if err := dbIns.mmap(0); err != nil {
		return nil, err
	}

	dbIns.freelist = newFreelist()
	dbIns.freelist.read(dbIns.page(dbIns.meta().Freelist))

	return dbIns, nil
}

// dbInit if file was the created, you should init the file.
func (db *DB) init() error {
	db.pageSize = os.Getpagesize()
	buf := make([]byte, db.pageSize*4)
	// the first page will contain the db meta info
	// the second page will contain the same data. that will
	// 0 is meta 1 is meta. so the freelist page will start at num 2.This is the cs's consensus.
	// so the 2 is the freelist page. and the third page will save the db data's root.And root is the begin of all data searching.
	// and data page will begin with 4.
	for i := 0; i < 2; i++ {
		pid := bpage.ID(i)
		page := db.getPageFromBuffer(buf, pid)
		page.ID = pid
		page.Flags = bpage.MetaFlag
		meta := page.GetMeta()
		meta.Magic = magic
		meta.Version = version
		meta.PageSize = db.pageSize
		meta.Freelist = 2
		meta.Root = bpage.Bucket{Root: 3}
		meta.ID = 4
		meta.TxID = bpage.TxID(i)
		meta.Checksum = meta.GenSum64()
	}
	page := db.getPageFromBuffer(buf, bpage.ID(2))
	page.ID = bpage.ID(2)
	page.Flags = bpage.FreelistFlag
	page.Count = 0

	if _, err := db.file.WriteAt(buf, 0); err != nil {
		return err
	}
	return fdatasync(db)
}

// getPageFromBuffer: get page from buffer, trans the byte to struct
func (db *DB) getPageFromBuffer(buf []byte, pgID bpage.ID) *bpage.Data {
	pageBytes := &buf[pgID*bpage.ID(pgID)]
	return (*bpage.Data)(unsafe.Pointer(pageBytes))
}

// fdatasync: this system call will just sync the data to the file.And not sync the meta of the file.
func fdatasync(db *DB) error {
	return syscall.Fdatasync(int(db.file.Fd()))
}

// mmap opens the underlying memory-mapped file and initializes the meta references.
// minsz is the minimum size that the new mmap can be.
func (db *DB) mmap(minsz int) error {
	db.mmaplock.Lock()
	defer db.mmaplock.Unlock()

	info, err := db.file.Stat()
	if err != nil {
		return fmt.Errorf("mmap stat error: %s", err)
	}
	if int(info.Size()) < db.pageSize*2 {
		return fmt.Errorf("file size too small")
	}

	// Ensure the size is at least the minimum size.
	var size = int(info.Size())
	if size < minsz {
		size = minsz
	}
	size, err = db.mmapSize(size)
	if err != nil {
		return err
	}

	// Unmap existing data before continuing.
	if err := db.munmap(); err != nil {
		return err
	}

	// Memory-map the data file as a byte slice.
	if err := mmap(db, size); err != nil {
		return err
	}

	// Save references to the meta pages.
	db.meta0 = db.page(0).GetMeta()
	db.meta1 = db.page(1).GetMeta()

	// Validate the meta pages. We only return an error if both meta pages fail
	// validation, since meta0 failing validation means that it wasn't saved
	// properly -- but we can recover using meta1. And vice-versa.
	err0 := db.meta0.Check()
	err1 := db.meta1.Check()
	if err0 != nil && err1 != nil {
		return err0
	}

	return nil
}

func (db *DB) mmapSize(size int) (int, error) {
	// Double the size from 32KB until 1GB.
	// the first num is 15
	// and if you use `1<<i`,means that i power of 2. Forget why, that is that.
	// so first you can check if size lower than 15 power of 2. You should know that's equal to size <= 32kb.
	// why? 10 power of 2 is 1kb. and 5 power of 2 is 32. and we get 32kb.
	// so, when i up 1. you can get double scale of the size.
	// but you maxmum is 30
	// easy to get that when file size is between 32kb and 1GB. you can always get the double room.
	for i := uint(15); i <= 30; i++ {
		if size <= 1<<i {
			return 1 << i, nil
		}
	}

	// Verify the requested size is not above the maximum allowed.
	if size > maxMapSize {
		return 0, fmt.Errorf("mmap too large")
	}

	// If larger than 1GB then grow by 1GB at a time.
	sz := int64(size)
	if remainder := sz % int64(maxMmapStep); remainder > 0 {
		sz += int64(maxMmapStep) - remainder
	}

	// Ensure that the mmap size is a multiple of the page size.
	// This should always be true since we're incrementing in MBs.
	pageSize := int64(db.pageSize)
	if (sz % pageSize) != 0 {
		sz = ((sz / pageSize) + 1) * pageSize
	}

	// If we've exceeded the max size then only grow up to the max size.
	if sz > maxMapSize {
		sz = maxMapSize
	}

	return int(sz), nil
}

func (db *DB) munmap() error {
	if err := munmap(db); err != nil {
		return fmt.Errorf("unmap error: " + err.Error())
	}
	return nil
}

// munmap unmaps a DB's data file from memory.
func munmap(db *DB) error {
	// Ignore the unmap if we have no mapped data.
	if db.dataref == nil {
		return nil
	}

	// Unmap using the original byte slice.
	err := syscall.Munmap(db.dataref)
	db.dataref = nil
	db.data = nil
	db.datasz = 0
	return err
}

// mmap memory maps a DB's data file.
func mmap(db *DB, sz int) error {
	// Map the data file to memory.
	b, err := syscall.Mmap(int(db.file.Fd()), 0, sz, syscall.PROT_READ, syscall.MAP_SHARED|db.MmapFlags)
	if err != nil {
		return err
	}

	// Advise the kernel that the mmap is accessed randomly.
	if err := madvise(b, syscall.MADV_RANDOM); err != nil {
		return fmt.Errorf("madvise: %s", err)
	}

	// Save the original byte slice and convert to a byte array pointer.
	db.dataref = b
	db.data = (*[maxMapSize]byte)(unsafe.Pointer(&b[0]))
	db.datasz = sz
	return nil
}

// NOTE: This function is copied from stdlib because it is not available on darwin.
func madvise(b []byte, advice int) (err error) {
	_, _, e1 := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), uintptr(advice))
	if e1 != 0 {
		err = e1
	}
	return
}

// page retrieves a page reference from the mmap based on the current page size.
func (db *DB) page(id bpage.ID) *bpage.Data {
	pos := id * bpage.ID(db.pageSize)
	return (*bpage.Data)(unsafe.Pointer(&db.data[pos]))
}

// read initializes the freelist from a freelist page.
func (f *freelist) read(p *bpage.Data) {
	// If the page.count is at the max uint16 value (64k) then it's considered
	// an overflow and the size of the freelist is stored as the first element.
	idx, count := 0, int(p.Count)
	if count == 0xFFFF {
		idx = 1
		count = int(((*[maxAllocSize]bpage.ID)(unsafe.Pointer(&p.Ptr)))[0])
	}

	// Copy the list of page ids from the freelist.
	if count == 0 {
		f.ids = nil
	} else {
		ids := ((*[maxAllocSize]bpage.ID)(unsafe.Pointer(&p.Ptr)))[idx:count]
		f.ids = make([]bpage.ID, len(ids))
		copy(f.ids, ids)

		// Make sure they're sorted.
		sort.Sort(bpage.IDs(f.ids))
	}

	// Rebuild the page cache.
	f.reindex()
}

// reindex rebuilds the free cache based on available and pending free lists.
func (f *freelist) reindex() {
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

// meta retrieves the current meta page reference.
func (db *DB) meta() *bpage.Meta {
	// We have to return the meta with the highest txid which doesn't fail
	// validation. Otherwise, we can cause errors when in fact the database is
	// in a consistent state. metaA is the one with the higher txid.
	metaA := db.meta0
	metaB := db.meta1
	if db.meta1.TxID > db.meta0.TxID {
		metaA = db.meta1
		metaB = db.meta0
	}

	// Use higher meta page if valid. Otherwise fallback to previous, if valid.
	if err := metaA.Check(); err == nil {
		return metaA
	} else if err := metaB.Check(); err == nil {
		return metaB
	}

	// This should never be reached, because both meta1 and meta0 were validated
	// on mmap() and we do fsync() on every write.
	panic("bolt.DB.meta(): invalid meta pages")
}
