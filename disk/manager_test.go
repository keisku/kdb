package disk_test

import (
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/keisku/kdb/disk"
)

func Test_Manger_impl(t *testing.T) {
	tests := []struct {
		name    string
		doTest  func(m disk.Manager) error
		wantErr error
	}{
		{
			name: "write 1, read 2, 3, 1, write 2, read 2",
			doTest: func(m disk.Manager) error {
				dataForWritePageId1 := strings.Repeat("x", 1000)
				dataForWritePageId2 := strings.Repeat("y", 500)
				dataForWritePageId3 := strings.Repeat("z", 20000)
				pageId1 := m.Alloc()
				pageId2 := m.Alloc()
				pageId3 := m.Alloc()

				// Write
				// PageId: 1
				n, err := m.Write(pageId1, []byte(dataForWritePageId1))
				if err != nil {
					return err
				}
				if n != len(dataForWritePageId1) {
					return fmt.Errorf("disk manager should write %d bytes, actual %d bytes", len(dataForWritePageId1), n)
				}

				// Read
				// PageId: 2
				dataForReadPageId2 := make([]byte, len(dataForWritePageId2))
				n, err = m.Read(pageId2, dataForReadPageId2)
				if err != io.EOF {
					return err
				}
				if n != 0 {
					return fmt.Errorf("disk manager should read %d bytes, actual %d bytes", 0, n)
				}
				if string(dataForReadPageId2) != string(make([]byte, len(dataForWritePageId2))) {
					return fmt.Errorf("disk manager failed to read expected data")
				}
				// PageId: 3
				dataForReadPageId3 := make([]byte, len(dataForWritePageId3))
				n, err = m.Read(pageId3, dataForReadPageId3)
				if err != io.EOF {
					return err
				}
				if n != 0 {
					return fmt.Errorf("disk manager should write %d bytes, actual %d bytes", 0, n)
				}
				if string(dataForReadPageId3) != string(make([]byte, len(dataForWritePageId3))) {
					return fmt.Errorf("disk manager failed to read expected data")
				}
				// PageId: 1
				dataForReadPageId1 := make([]byte, len(dataForWritePageId1))
				n, err = m.Read(pageId1, dataForReadPageId1)
				if err != nil {
					return err
				}
				if n != len(dataForReadPageId1) {
					return fmt.Errorf("disk manager should write %d bytes, actual %d bytes", len(dataForReadPageId1), n)
				}
				if string(dataForReadPageId1) != dataForWritePageId1 {
					return fmt.Errorf("disk manager failed to read expected data")
				}

				// Write
				// PageId: 2
				n, err = m.Write(pageId2, []byte(dataForWritePageId2))
				if err != nil {
					return err
				}
				if n != len(dataForWritePageId2) {
					return fmt.Errorf("disk manager should write %d bytes, actual %d bytes", len(dataForWritePageId2), n)
				}

				// Read
				// PageId: 2
				dataForReadPageId2 = make([]byte, len(dataForWritePageId2))
				n, err = m.Read(pageId2, dataForReadPageId2)
				if err != nil {
					return err
				}
				if n != len(dataForReadPageId2) {
					return fmt.Errorf("disk manager should write %d bytes, actual %d bytes", len(dataForReadPageId2), n)
				}
				if string(dataForReadPageId2) != dataForWritePageId2 {
					return fmt.Errorf("disk manager failed to read expected data")
				}
				return nil
			},
		},
		{
			name: "write 1, 3, 2 and read 3, 3",
			doTest: func(m disk.Manager) error {
				dataForWritePageId1 := strings.Repeat("x", 1000)
				dataForWritePageId2 := strings.Repeat("y", 500)
				dataForWritePageId3 := strings.Repeat("z", 20000)
				pageId1 := m.Alloc()
				pageId2 := m.Alloc()
				pageId3 := m.Alloc()

				// Write
				// PageId: 1
				n, err := m.Write(pageId1, []byte(dataForWritePageId1))
				if err != nil {
					return err
				}
				if n != len(dataForWritePageId1) {
					return fmt.Errorf("disk manager should write %d bytes, actual %d bytes", len(dataForWritePageId1), n)
				}
				// PageId: 3
				n, err = m.Write(pageId3, []byte(dataForWritePageId3))
				if err != nil {
					return err
				}
				if n != len(dataForWritePageId3) {
					return fmt.Errorf("disk manager should write %d bytes, actual %d bytes", len(dataForWritePageId3), n)
				}
				// PageId: 2
				n, err = m.Write(pageId2, []byte(dataForWritePageId2))
				if err != nil {
					return err
				}
				if n != len(dataForWritePageId2) {
					return fmt.Errorf("disk manager should write %d bytes, actual %d bytes", len(dataForWritePageId2), n)
				}

				// Read
				// PageId: 3
				dataForReadPageId3 := make([]byte, len(dataForWritePageId3))
				n, err = m.Read(pageId3, dataForReadPageId3)
				if err != nil {
					return err
				}
				if n != len(dataForReadPageId3) {
					return fmt.Errorf("disk manager should write %d bytes, actual %d bytes", len(dataForReadPageId3), n)
				}
				if string(dataForReadPageId3) != dataForWritePageId3 {
					return fmt.Errorf("disk manager failed to read expected data")
				}
				// PageId: 3
				dataForReadPageId3 = make([]byte, len(dataForWritePageId3))
				n, err = m.Read(pageId3, dataForReadPageId3)
				if err != nil {
					return err
				}
				if n != len(dataForReadPageId3) {
					return fmt.Errorf("disk manager should write %d bytes, actual %d bytes", len(dataForReadPageId3), n)
				}
				if string(dataForReadPageId3) != dataForWritePageId3 {
					return fmt.Errorf("disk manager failed to read expected data")
				}
				return nil
			},
		},
		{
			name: "write 1, 3, 2 and read 3, 2, 1",
			doTest: func(m disk.Manager) error {
				dataForWritePageId1 := strings.Repeat("x", 1000)
				dataForWritePageId2 := strings.Repeat("y", 500)
				dataForWritePageId3 := strings.Repeat("z", 20000)
				pageId1 := m.Alloc()
				pageId2 := m.Alloc()
				pageId3 := m.Alloc()

				// Write
				// PageId: 1
				n, err := m.Write(pageId1, []byte(dataForWritePageId1))
				if err != nil {
					return err
				}
				if n != len(dataForWritePageId1) {
					return fmt.Errorf("disk manager should write %d bytes, actual %d bytes", len(dataForWritePageId1), n)
				}
				// PageId: 3
				n, err = m.Write(pageId3, []byte(dataForWritePageId3))
				if err != nil {
					return err
				}
				if n != len(dataForWritePageId3) {
					return fmt.Errorf("disk manager should write %d bytes, actual %d bytes", len(dataForWritePageId3), n)
				}
				// PageId: 2
				n, err = m.Write(pageId2, []byte(dataForWritePageId2))
				if err != nil {
					return err
				}
				if n != len(dataForWritePageId2) {
					return fmt.Errorf("disk manager should write %d bytes, actual %d bytes", len(dataForWritePageId2), n)
				}

				// Read
				// PageId: 3
				dataForReadPageId3 := make([]byte, len(dataForWritePageId3))
				n, err = m.Read(pageId3, dataForReadPageId3)
				if err != nil {
					return err
				}
				if n != len(dataForReadPageId3) {
					return fmt.Errorf("disk manager should write %d bytes, actual %d bytes", len(dataForReadPageId3), n)
				}
				if string(dataForReadPageId3) != dataForWritePageId3 {
					return fmt.Errorf("disk manager failed to read expected data")
				}
				// PageId: 2
				dataForReadPageId2 := make([]byte, len(dataForWritePageId2))
				n, err = m.Read(pageId2, dataForReadPageId2)
				if err != nil {
					return err
				}
				if n != len(dataForReadPageId2) {
					return fmt.Errorf("disk manager should write %d bytes, actual %d bytes", len(dataForReadPageId2), n)
				}
				if string(dataForReadPageId2) != dataForWritePageId2 {
					return fmt.Errorf("disk manager failed to read expected data")
				}
				// PageId: 1
				dataForReadPageId1 := make([]byte, len(dataForWritePageId1))
				n, err = m.Read(pageId1, dataForReadPageId1)
				if err != nil {
					return err
				}
				if n != len(dataForReadPageId1) {
					return fmt.Errorf("disk manager should write %d bytes, actual %d bytes", len(dataForReadPageId1), n)
				}
				if string(dataForReadPageId1) != dataForWritePageId1 {
					return fmt.Errorf("disk manager failed to read expected data")
				}
				return nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := disk.NewManager("test")
			t.Cleanup(func() {
				_ = os.RemoveAll("test")
			})
			if err != nil {
				t.Errorf("NewManager: %s", err)
				return
			}
			err = tt.doTest(m)
			if tt.wantErr == nil {
				if err != nil {
					t.Errorf("%s: unexpected error: %s", tt.name, err)
				}
			} else {
				if err == nil {
					t.Errorf("%s: expected error: %s", tt.name, tt.wantErr)
				} else if err.Error() != tt.wantErr.Error() {
					t.Errorf("%s: unexpected error: %s", tt.name, err)
				}
			}
		})
	}
}
