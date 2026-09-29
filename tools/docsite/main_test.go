package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testSite(t *testing.T) *site {
	t.Helper()
	dir := t.TempDir()
	file := filepath.Join(dir, "pkg.go")
	src := "// Package pkg is a fixture.\npackage pkg\n\n// Hello greets.\nfunc Hello() {}\n"
	if err := os.WriteFile(file, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := load("example.com/m", "https://example.com/m", []string{file})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestPagesCarryTheKrabkaTheme(t *testing.T) {
	s := testSite(t)
	pages := map[string][]byte{
		"index":   s.indexPage(),
		"package": s.packagePage(s.packages[0]),
	}
	wants := []string{
		`<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,`,
		`class="logo"`,
		"--bg:#080d1a",
		"--accent:#ff4d2e",
		"--link:#ff8466",
		"JetBrains",
		":focus-visible{outline:2px solid var(--accent)",
		"prefers-reduced-motion",
		`<footer class="site-footer">`,
	}
	for name, page := range pages {
		text := string(page)
		for _, want := range wants {
			if !strings.Contains(text, want) {
				t.Errorf("%s page lacks %q", name, want)
			}
		}
		if !strings.Contains(text, "<style>@import url(\"https://fonts.googleapis.com/css2?family=Inter") {
			t.Errorf("%s page: the font import is not the first stylesheet rule", name)
		}
		if strings.Contains(text, "prefers-color-scheme") {
			t.Errorf("%s page keeps a light mode", name)
		}
	}
}
