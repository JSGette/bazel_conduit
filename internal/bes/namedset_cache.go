package bes

import (
	"path"
	"sync"
)

// FileInfo represents a file from a namedSetOfFiles.
type FileInfo struct {
	// Name is the file path relative to the output root
	Name string

	// URI is the remote cache URI (if available)
	URI string

	// Digest is the content hash
	Digest string

	// Length is the file size in bytes
	Length int64

	// PathPrefix is the output path prefix (e.g., "bazel-out/darwin_arm64-fastbuild/bin")
	PathPrefix string
}

// FullPath returns the complete path including the prefix.
func (f *FileInfo) FullPath() string {
	if f.PathPrefix == "" {
		return f.Name
	}
	return path.Join(f.PathPrefix, f.Name)
}

// NamedSetCache caches file information from namedSet events.
// Files are stored by namedSet ID and retrieved when targetCompleted events reference them.
type NamedSetCache struct {
	mu       sync.RWMutex
	fileSets map[string][]FileInfo
}

// NewNamedSetCache creates a new NamedSetCache.
func NewNamedSetCache() *NamedSetCache {
	return &NamedSetCache{
		fileSets: make(map[string][]FileInfo),
	}
}

// Store adds a namedSet's files to the cache.
func (c *NamedSetCache) Store(id string, files []FileInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.fileSets[id] = files
}

// Get retrieves files for a namedSet by ID.
func (c *NamedSetCache) Get(id string) ([]FileInfo, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	files, exists := c.fileSets[id]
	return files, exists
}

// ResolveAll retrieves and combines files from multiple namedSet IDs.
// Missing IDs are silently ignored.
func (c *NamedSetCache) ResolveAll(ids []string) []FileInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result []FileInfo
	for _, id := range ids {
		if files, exists := c.fileSets[id]; exists {
			result = append(result, files...)
		}
	}
	return result
}

// Remove deletes a namedSet from the cache.
func (c *NamedSetCache) Remove(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.fileSets, id)
}

// Clear removes all entries from the cache.
func (c *NamedSetCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.fileSets = make(map[string][]FileInfo)
}

// Count returns the number of namedSets in the cache.
func (c *NamedSetCache) Count() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.fileSets)
}

