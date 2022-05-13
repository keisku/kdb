package disk

import (
	"os"
)

// PAGE_SIZE is a size of a page. Disk manager reads and writes data in this size.
// This is because a file system reads and writes data in a page size; it is typically
// 4096 bytes.
const PAGE_SIZE = 4096

// PageId is a page id.
type PageId uint64

// Manager is a disk manager that write/read data to/from disk.
type Manager interface {
	// Read reads data associated to a given page_id from disk.
	Read(pageId PageId, p []byte) (n int, err error)
	// Write writes data associated to a given page_id in disk.
	Write(pageId PageId, p []byte) (n int, err error)
	// Allocate a new page.
	Alloc() PageId
}

type manager struct {
	heapFile   *os.File
	nextPageId PageId
}

// NewManager creates a new disk manager and a new heap file with a given name.
func NewManager(dataFilePath string) (Manager, error) {
	heapFile, err := os.OpenFile(dataFilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	stat, err := heapFile.Stat()
	if err != nil {
		return nil, err
	}
	return &manager{
		heapFile:   heapFile,
		nextPageId: PageId(stat.Size() / PAGE_SIZE),
	}, nil
}

func (m *manager) Read(pageId PageId, p []byte) (int, error) {
	offset := PAGE_SIZE * pageId
	_, err := m.heapFile.Seek(int64(offset), 0)
	if err != nil {
		return 0, err
	}
	n, err := m.heapFile.Read(p)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (m *manager) Write(pageId PageId, p []byte) (int, error) {
	offset := PAGE_SIZE * pageId
	_, err := m.heapFile.Seek(int64(offset), 0)
	if err != nil {
		return 0, err
	}
	n, err := m.heapFile.Write(p)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (m *manager) Alloc() PageId {
	id := m.nextPageId
	m.nextPageId++
	return id
}
