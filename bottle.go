package bottle

import (
	"log"
	"os"
	"syscall"
	"unsafe"

	"github.com/ranhaoliuLeo/bottle/bpage"
)

// magic num indicate that file is bottle db file
const magic uint32 = 0xED0CDBBD
const version = 1

type DB struct {
	pageSize int
	file     *os.File
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
