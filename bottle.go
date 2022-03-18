package bottle

import (
	"os"
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
	file, err := os.OpenFile(path, os.O_CREATE, mode)
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
		dbIns.init()

	} else {
		// means that file is a old file,
	}
	return dbIns, nil
}

// dbInit if file was the created, you should init the file.
func (db *DB) init() {
	db.pageSize = os.Getpagesize()
	buf := make([]byte, db.pageSize*4)
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

}

// getPageFromBuffer: get page from buffer, trans the byte to struct
func (db *DB) getPageFromBuffer(buf []byte, pgID bpage.ID) *bpage.Data {
	pageBytes := &buf[pgID*bpage.ID(pgID)]
	return (*bpage.Data)(unsafe.Pointer(pageBytes))
}
