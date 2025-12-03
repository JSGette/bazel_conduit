package bes

import (
	"testing"
)

// TestNewNamedSetCache tests creating a new NamedSetCache
func TestNewNamedSetCache(t *testing.T) {
	cache := NewNamedSetCache()

	if cache == nil {
		t.Fatal("NewNamedSetCache returned nil")
	}
	if cache.Count() != 0 {
		t.Errorf("expected 0 entries, got %d", cache.Count())
	}
}

// TestNamedSetCacheStore tests storing a namedSet
func TestNamedSetCacheStore(t *testing.T) {
	cache := NewNamedSetCache()

	files := []FileInfo{
		{
			Name:   "deps/compile_policy/LICENSE",
			URI:    "bytestream://bazel-buildbarn-frontend/blobs/abc123/11342",
			Digest: "7dc0b59325e0a735cbdd089dc802c7e68e3796b36468c72e4b640ac0c0f0557c",
			Length: 11342,
		},
	}

	cache.Store("0", files)

	if cache.Count() != 1 {
		t.Errorf("expected 1 entry, got %d", cache.Count())
	}
}

// TestNamedSetCacheRetrieve tests retrieving a namedSet
func TestNamedSetCacheRetrieve(t *testing.T) {
	cache := NewNamedSetCache()

	files := []FileInfo{
		{
			Name:       "deps/compile_policy/LICENSE",
			URI:        "bytestream://bazel-buildbarn-frontend/blobs/abc123/11342",
			Digest:     "7dc0b59325e0a735cbdd089dc802c7e68e3796b36468c72e4b640ac0c0f0557c",
			Length:     11342,
			PathPrefix: "bazel-out/darwin_arm64-fastbuild/bin",
		},
	}

	cache.Store("0", files)

	retrieved, exists := cache.Get("0")
	if !exists {
		t.Fatal("namedSet not found")
	}
	if len(retrieved) != 1 {
		t.Errorf("expected 1 file, got %d", len(retrieved))
	}
	if retrieved[0].Name != "deps/compile_policy/LICENSE" {
		t.Errorf("file name mismatch: got %s", retrieved[0].Name)
	}
	if retrieved[0].Digest != "7dc0b59325e0a735cbdd089dc802c7e68e3796b36468c72e4b640ac0c0f0557c" {
		t.Errorf("digest mismatch")
	}
}

// TestNamedSetCacheRetrieveNonexistent tests retrieving a non-existent namedSet
func TestNamedSetCacheRetrieveNonexistent(t *testing.T) {
	cache := NewNamedSetCache()

	files, exists := cache.Get("nonexistent")
	if exists {
		t.Error("expected exists to be false")
	}
	if files != nil {
		t.Error("expected files to be nil")
	}
}

// TestNamedSetCacheMultipleEntries tests storing multiple namedSets
func TestNamedSetCacheMultipleEntries(t *testing.T) {
	cache := NewNamedSetCache()

	// Store multiple namedSets
	cache.Store("0", []FileInfo{{Name: "file0.txt"}})
	cache.Store("1", []FileInfo{{Name: "file1.txt"}, {Name: "file1b.txt"}})
	cache.Store("2", []FileInfo{{Name: "file2.txt"}})

	if cache.Count() != 3 {
		t.Errorf("expected 3 entries, got %d", cache.Count())
	}

	files, exists := cache.Get("1")
	if !exists {
		t.Fatal("namedSet 1 not found")
	}
	if len(files) != 2 {
		t.Errorf("expected 2 files in namedSet 1, got %d", len(files))
	}
}

// TestNamedSetCacheOverwrite tests overwriting a namedSet
func TestNamedSetCacheOverwrite(t *testing.T) {
	cache := NewNamedSetCache()

	cache.Store("0", []FileInfo{{Name: "original.txt"}})
	cache.Store("0", []FileInfo{{Name: "updated.txt"}})

	files, _ := cache.Get("0")
	if len(files) != 1 {
		t.Errorf("expected 1 file, got %d", len(files))
	}
	if files[0].Name != "updated.txt" {
		t.Errorf("expected 'updated.txt', got '%s'", files[0].Name)
	}
}

// TestNamedSetCacheResolveMultiple tests resolving multiple namedSet IDs
func TestNamedSetCacheResolveMultiple(t *testing.T) {
	cache := NewNamedSetCache()

	cache.Store("0", []FileInfo{{Name: "file0.txt"}})
	cache.Store("1", []FileInfo{{Name: "file1.txt"}})
	cache.Store("2", []FileInfo{{Name: "file2.txt"}})

	ids := []string{"0", "2"}
	files := cache.ResolveAll(ids)

	if len(files) != 2 {
		t.Errorf("expected 2 files, got %d", len(files))
	}

	// Check that files from both namedSets are included
	names := make(map[string]bool)
	for _, f := range files {
		names[f.Name] = true
	}
	if !names["file0.txt"] || !names["file2.txt"] {
		t.Error("missing expected files")
	}
}

// TestNamedSetCacheResolveMultipleWithMissing tests resolving with missing IDs
func TestNamedSetCacheResolveMultipleWithMissing(t *testing.T) {
	cache := NewNamedSetCache()

	cache.Store("0", []FileInfo{{Name: "file0.txt"}})

	ids := []string{"0", "nonexistent", "alsomissing"}
	files := cache.ResolveAll(ids)

	if len(files) != 1 {
		t.Errorf("expected 1 file, got %d", len(files))
	}
}

// TestNamedSetCacheRemove tests removing a namedSet
func TestNamedSetCacheRemove(t *testing.T) {
	cache := NewNamedSetCache()

	cache.Store("0", []FileInfo{{Name: "file.txt"}})
	cache.Remove("0")

	_, exists := cache.Get("0")
	if exists {
		t.Error("namedSet should not exist after removal")
	}
	if cache.Count() != 0 {
		t.Errorf("expected 0 entries, got %d", cache.Count())
	}
}

// TestNamedSetCacheClear tests clearing all entries
func TestNamedSetCacheClear(t *testing.T) {
	cache := NewNamedSetCache()

	cache.Store("0", []FileInfo{{Name: "file0.txt"}})
	cache.Store("1", []FileInfo{{Name: "file1.txt"}})
	cache.Store("2", []FileInfo{{Name: "file2.txt"}})

	cache.Clear()

	if cache.Count() != 0 {
		t.Errorf("expected 0 entries after clear, got %d", cache.Count())
	}
}

// TestFileInfoFields tests FileInfo struct fields
func TestFileInfoFields(t *testing.T) {
	info := FileInfo{
		Name:       "deps/gstatus/gstatus",
		URI:        "bytestream://bazel-buildbarn-frontend.ddbuild.io:443/remote-caching/datadog-agent/blobs/42d10a727f2d812f880baba59d85a61d84b4abc23aff9d6fea1137ed6ef2d8b7/8195221",
		Digest:     "42d10a727f2d812f880baba59d85a61d84b4abc23aff9d6fea1137ed6ef2d8b7",
		Length:     8195221,
		PathPrefix: "bazel-out/darwin_arm64-fastbuild/bin",
	}

	if info.Name != "deps/gstatus/gstatus" {
		t.Error("Name mismatch")
	}
	if info.Length != 8195221 {
		t.Error("Length mismatch")
	}
	if info.PathPrefix != "bazel-out/darwin_arm64-fastbuild/bin" {
		t.Error("PathPrefix mismatch")
	}
}

// TestFileInfoFullPath tests getting the full path of a file
func TestFileInfoFullPath(t *testing.T) {
	info := FileInfo{
		Name:       "deps/gstatus/gstatus",
		PathPrefix: "bazel-out/darwin_arm64-fastbuild/bin",
	}

	fullPath := info.FullPath()
	expected := "bazel-out/darwin_arm64-fastbuild/bin/deps/gstatus/gstatus"

	if fullPath != expected {
		t.Errorf("FullPath mismatch: got %s, want %s", fullPath, expected)
	}
}

// TestFileInfoFullPathNoPrefix tests getting full path without prefix
func TestFileInfoFullPathNoPrefix(t *testing.T) {
	info := FileInfo{
		Name: "deps/gstatus/LICENSE",
	}

	fullPath := info.FullPath()

	if fullPath != "deps/gstatus/LICENSE" {
		t.Errorf("FullPath without prefix mismatch: got %s", fullPath)
	}
}

// TestNamedSetCacheConcurrentAccess tests thread safety
func TestNamedSetCacheConcurrentAccess(t *testing.T) {
	cache := NewNamedSetCache()
	done := make(chan bool)

	// Concurrent writes
	for i := 0; i < 100; i++ {
		go func(n int) {
			id := string(rune('0' + n%10))
			cache.Store(id, []FileInfo{{Name: "file.txt"}})
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 100; i++ {
		<-done
	}

	// Should have at most 10 entries (0-9)
	if cache.Count() > 10 {
		t.Errorf("expected at most 10 entries, got %d", cache.Count())
	}
}

