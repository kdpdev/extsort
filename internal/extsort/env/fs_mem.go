package env

import (
	"bytes"
	"io"
	"os"
	"path"
	"strings"
	"sync"
)

func NewMemFs(storage map[string]*MemFsEntry) *MemFs {
	if storage == nil {
		storage = make(map[string]*MemFsEntry)
	}

	return &MemFs{
		guard:   newGuard(),
		storage: storage,
	}
}

type MemFs struct {
	*guard
	storage map[string]*MemFsEntry
}

func (this *MemFs) HasOpenedEntries() bool {
	defer this.lock()()
	for _, entry := range this.storage {
		if !entry.IsClosed() {
			return true
		}
	}
	return false
}

func (this *MemFs) GetFileSize(filePath string) (uint64, error) {
	file, size, err := this.OpenReadFile(filePath)
	if err != nil {
		return 0, err
	}
	return size, file.Close()
}

func (this *MemFs) CreateWriteFile(filePath string) (io.WriteCloser, error) {
	filePath, err := this.normalizePath(filePath)
	if err != nil {
		return nil, err
	}

	dirPath, _ := this.splitPath(filePath)

	defer this.lock()()

	if dirPath != "" {
		dirEntry := this.unsafeFindEntry(dirPath)

		if dirEntry == nil {
			return nil, os.ErrNotExist
		}

		if dirEntry.IsFile() {
			return nil, os.ErrInvalid
		}
	}

	fileEntry := this.unsafeFindEntry(filePath)
	if fileEntry != nil {
		return nil, os.ErrExist
	}

	fileEntry = NewOpenedMemFsFile(nil)

	this.storage[filePath] = fileEntry

	return fileEntry, nil
}

func (this *MemFs) OpenReadFile(filePath string) (io.ReadCloser, uint64, error) {
	filePath, err := this.normalizePath(filePath)
	if err != nil {
		return nil, 0, err
	}

	dirPath, _ := this.splitPath(filePath)

	defer this.lock()()

	if dirPath != "" {
		dirEntry := this.unsafeFindEntry(dirPath)

		if dirEntry == nil {
			return nil, 0, os.ErrNotExist
		}

		if dirEntry.IsFile() {
			return nil, 0, os.ErrInvalid
		}
	}

	fileEntry := this.unsafeFindEntry(filePath)
	if fileEntry == nil {
		return nil, 0, os.ErrNotExist
	}

	size, err := fileEntry.Open()
	if err != nil {
		return nil, 0, err
	}

	return fileEntry, uint64(size), nil
}

func (this *MemFs) MoveFile(src, dst string) error {
	src, err := this.normalizePath(src)
	if err != nil {
		return nil
	}

	dst, err = this.normalizePath(dst)
	if err != nil {
		return err
	}

	defer this.lock()()

	srcFile := this.unsafeFindEntry(src)
	if srcFile == nil {
		return os.ErrNotExist
	}

	if !srcFile.IsFile() {
		return os.ErrInvalid
	}

	if !srcFile.IsClosed() {
		return os.ErrPermission
	}

	dstFile := this.unsafeFindEntry(dst)
	if dstFile != nil {
		if !dstFile.IsFile() {
			return os.ErrInvalid
		}
		return os.ErrExist
	}

	dstDir, _ := this.splitPath(dst)
	if dstDir != "" {
		_, err = this.unsafeEnsureDirExists(dstDir)
		if err != nil {
			return err
		}
	}

	this.storage[dst] = srcFile
	delete(this.storage, src)

	return nil
}

func (this *MemFs) Remove(entryPath string) error {
	entryPath, err := this.normalizePath(entryPath)
	if err != nil {
		return err
	}

	defer this.lock()()

	entry := this.unsafeFindEntry(entryPath)
	if entry == nil {
		return os.ErrNotExist
	}

	if !entry.IsClosed() {
		return os.ErrPermission
	}

	if entry.IsFile() {
		delete(this.storage, entryPath)
		return nil
	}

	toBeDeleted := []string{entryPath}
	startsWith := entryPath + "/"
	for p, e := range this.storage {
		if strings.Index(p, startsWith) == 0 {
			if !e.IsClosed() {
				return os.ErrPermission
			}
			toBeDeleted = append(toBeDeleted, p)
		}
	}

	for _, p := range toBeDeleted {
		delete(this.storage, p)
	}

	return nil
}

func (this *MemFs) EnsureDirExists(dirPath string) (created bool, _ error) {
	defer this.lock()()
	return this.unsafeEnsureDirExists(dirPath)
}

func (this *MemFs) unsafeEnsureDirExists(dirPath string) (created bool, _ error) {
	dirPath, err := this.normalizePath(dirPath)
	if err != nil {
		return false, err
	}

	names := strings.Split(dirPath, "/")
	for i := range names {
		p := strings.Join(names[:i+1], "/")
		if entry := this.storage[p]; entry != nil {
			if entry.IsFile() {
				return false, os.ErrInvalid
			}
		} else {
			created = true
			this.storage[p] = NewClosedMemFsDir()
		}
	}

	return created, nil
}

func (this *MemFs) splitPath(entryPath string) (dirPath string, fileName string) {
	dirPath = ""
	lastSeparator := strings.LastIndexByte(entryPath, '/')
	if lastSeparator >= 0 {
		dirPath = entryPath[:lastSeparator]
	}
	fileName = entryPath[lastSeparator+1:]
	return dirPath, fileName
}

func (this *MemFs) normalizePath(entryPath string) (string, error) {
	entryPath = strings.ReplaceAll(entryPath, "\\", "/")
	entries := strings.Split(entryPath, "/")
	for _, entry := range entries {
		if badMemFsEntryNames[entry] {
			return "", path.ErrBadPattern
		}
	}
	if entryPath[len(entryPath)-1] == '/' {
		return "", path.ErrBadPattern
	}
	return entryPath, nil
}

func (this *MemFs) unsafeFindEntry(entryPath string) *MemFsEntry {
	return this.storage[entryPath]
}

var badMemFsEntryNames = map[string]bool{
	"":   true,
	"/":  true,
	"\\": true,
	".":  true,
	"..": true,
	":":  true,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func NewClosedMemFsDir() *MemFsEntry {
	return &MemFsEntry{
		guard:    newGuard(),
		isFile:   false,
		isClosed: true,
	}
}

func NewClosedMemFsFile(data []byte) *MemFsEntry {
	return &MemFsEntry{
		guard:    newGuard(),
		isFile:   true,
		isClosed: true,
		data:     data,
	}
}

func NewOpenedMemFsFile(data []byte) *MemFsEntry {
	return &MemFsEntry{
		guard:    newGuard(),
		isFile:   true,
		isClosed: false,
		data:     data,
	}
}

type MemFsEntry struct {
	*guard
	isFile        bool
	isClosed      bool
	readCursorPos int
	data          []byte
}

func (this *MemFsEntry) Read(p []byte) (n int, err error) {
	defer this.lock()()

	if !this.isFile {
		return 0, os.ErrInvalid
	}

	if this.isClosed {
		return 0, os.ErrClosed
	}

	buf := bytes.NewBuffer(this.data[this.readCursorPos:])
	n, err = buf.Read(p)
	this.readCursorPos += n

	return n, err
}

func (this *MemFsEntry) Write(p []byte) (n int, err error) {
	defer this.lock()()

	if !this.isFile {
		return 0, os.ErrInvalid
	}

	if this.isClosed {
		return 0, os.ErrClosed
	}

	this.data = append(this.data, p...)

	return len(p), nil
}

func (this *MemFsEntry) Open() (int, error) {
	defer this.lock()()

	if !this.isClosed {
		return 0, os.ErrPermission
	}

	this.isClosed = false
	this.readCursorPos = 0

	return len(this.data), nil
}

func (this *MemFsEntry) IsFile() bool {
	defer this.lock()()
	return this.isFile
}

func (this *MemFsEntry) IsClosed() bool {
	defer this.lock()()
	return this.isClosed
}

func (this *MemFsEntry) Close() error {
	defer this.lock()()

	if !this.isFile {
		return os.ErrInvalid
	}

	if this.isClosed {
		return os.ErrClosed
	}

	this.isClosed = true

	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func newGuard() *guard {
	return &guard{mutex: &sync.Mutex{}}
}

type guard struct {
	mutex *sync.Mutex
}

func (this *guard) lock() func() {
	this.mutex.Lock()
	return this.mutex.Unlock
}
