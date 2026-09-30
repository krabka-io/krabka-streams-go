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
		"@media (max-width:48rem){.site-header{position:static}",
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

// exampleSite loads a package with two runnable examples. ExampleHello uses
// only Hello, so go/doc turns it into a whole file (Play is set).
// ExampleHello_bare refers to an identifier the package does not declare, so
// it stays a bare block (Play is nil).
func exampleSite(t *testing.T) *site {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"pkg.go": "// Package pkg is a fixture.\npackage pkg\n\n// Hello greets.\nfunc Hello() string { return \"hi\" }\n",
		"play_test.go": "package pkg_test\n\nimport (\n\t\"fmt\"\n\n\tpkg \"example.com/m\"\n)\n\n" +
			"func ExampleHello() {\n\tgreeting := pkg.Hello()\n\tfmt.Println(map[string]string{\n\t\t\"k\": greeting,\n\t})\n\t// Output: map[k:hi]\n}\n",
		"bare_test.go": "package pkg_test\n\nimport pkg \"example.com/m\"\n\nfunc ExampleHello_bare() {\n\tif missing {\n\t\tpkg.Hello()\n\t}\n}\n",
	}
	var paths []string
	for name, src := range files {
		file := filepath.Join(dir, name)
		if err := os.WriteFile(file, []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, file)
	}
	s, err := load("example.com/m", "https://example.com/m", paths)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestExamplesKeepTheirClosingBraceAndIndentation(t *testing.T) {
	s := exampleSite(t)
	examples := s.packages[0].doc.Funcs[0].Examples
	if len(examples) != 2 || examples[0].Play == nil || examples[1].Play != nil {
		t.Fatal("fixture should hold a Play example, then a bare one")
	}
	page := plain(string(s.packagePage(s.packages[0])))
	for _, want := range []string{
		"if missing {\n    pkg.Hello()\n}\n", // bare block: outer braces trimmed, then dedented
		"import (\n    \"fmt\"\n",
		"func main() {\n    greeting := pkg.Hello()\n    fmt.Println(map[string]string{\n        \"k\": greeting,\n    })\n}",
	} {
		if !strings.Contains(page, want) {
			t.Errorf("example lost its layout, want %q in:\n%s", want, page)
		}
	}
}

func TestDedent(t *testing.T) {
	for in, want := range map[string]string{
		"a {\n\tb\n}":            "a {\n\tb\n}",
		"\n\ta\n\t\tb\n":         "a\n\tb",
		"import (\n\t\"x\"\n)":   "import (\n\t\"x\"\n)",
		"\tbody\n\n\t\tnested\n": "body\n\n\tnested",
	} {
		if got := dedent(in); got != want {
			t.Errorf("dedent(%q) = %q, want %q", in, got, want)
		}
	}
}
