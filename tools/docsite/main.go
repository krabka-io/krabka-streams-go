// Command docsite renders a static API reference site for the module's
// packages from their godoc comments.
//
// It is the Go mirror of the Java repository's hermetic javadoc site: Bazel
// runs it with every Go source file as input, and the output directory is
// what the Pages workflow deploys. The tool uses only the standard library
// (go/doc and friends), so the site never depends on network access or an
// installed toolchain.
//
// Usage:
//
//	docsite --module <module path> --repo <github url> --out <directory> <file.go>...
//
// Files ending in _test.go contribute runnable examples; all other files
// contribute declarations and documentation.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/doc"
	"go/doc/comment"
	"go/parser"
	"go/printer"
	"go/token"
	"html"
	"log"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
)

func main() {
	module := flag.String("module", "", "module import path")
	repo := flag.String("repo", "", "repository URL for source links")
	out := flag.String("out", "", "output directory")
	flag.Parse()
	if *module == "" || *out == "" || flag.NArg() == 0 {
		log.Fatal("usage: docsite --module <path> --repo <url> --out <dir> <file.go>...")
	}
	site, err := load(*module, *repo, flag.Args())
	if err != nil {
		log.Fatal(err)
	}
	if err := site.render(*out); err != nil {
		log.Fatal(err)
	}
}

type site struct {
	module   string
	repo     string
	packages []*packageDoc
}

type packageDoc struct {
	doc      *doc.Package
	fset     *token.FileSet
	files    map[string]*ast.File // keyed by source path
	dir      string               // module-relative directory, "" for the root
	page     string               // output file name
	synopsis string
}

// load parses the given files, grouped by directory, into documented
// packages.
func load(module, repo string, args []string) (*site, error) {
	byDir := map[string][]string{}
	for _, file := range args {
		dir := filepath.ToSlash(filepath.Dir(file))
		if dir == "." {
			dir = ""
		}
		byDir[dir] = append(byDir[dir], file)
	}
	result := &site{module: module, repo: repo}
	for _, dir := range slices.Sorted(maps.Keys(byDir)) {
		importPath := module
		if dir != "" {
			importPath = module + "/" + dir
		}
		fset := token.NewFileSet()
		files := map[string]*ast.File{}
		parsed := make([]*ast.File, 0, len(byDir[dir]))
		for _, file := range slices.Sorted(slices.Values(byDir[dir])) {
			tree, err := parser.ParseFile(fset, file, nil, parser.ParseComments)
			if err != nil {
				return nil, fmt.Errorf("cannot parse %s: %w", file, err)
			}
			files[file] = tree
			parsed = append(parsed, tree)
		}
		documented, err := doc.NewFromFiles(fset, parsed, importPath)
		if err != nil {
			return nil, fmt.Errorf("cannot document %s: %w", importPath, err)
		}
		page := "index.html"
		if dir != "" {
			page = strings.ReplaceAll(dir, "/", "-") + ".html"
		} else {
			page = documented.Name + ".html"
		}
		result.packages = append(result.packages, &packageDoc{
			doc:      documented,
			fset:     fset,
			files:    files,
			dir:      dir,
			page:     page,
			synopsis: documented.Synopsis(documented.Doc),
		})
	}
	return result, nil
}

func (s *site) render(out string) error {
	if err := os.MkdirAll(out, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "index.html"), s.indexPage(), 0o644); err != nil {
		return err
	}
	for _, pkg := range s.packages {
		if err := os.WriteFile(filepath.Join(out, pkg.page), s.packagePage(pkg), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// pageFor maps a package import path to its page, or "" when the path is
// outside the module.
func (s *site) pageFor(importPath string) string {
	for _, pkg := range s.packages {
		if pkg.doc.ImportPath == importPath {
			return pkg.page
		}
	}
	return ""
}

func (s *site) indexPage() []byte {
	var body bytes.Buffer
	fmt.Fprintf(&body, "<h1>%s</h1>\n", html.EscapeString(s.module))
	body.WriteString("<p>API reference, generated from the package documentation. ")
	fmt.Fprintf(&body, `See the <a href="%s">repository</a> for guides and examples.</p>`,
		html.EscapeString(s.repo))
	body.WriteString("\n<table class=\"packages\">\n<tr><th>Package</th><th>Synopsis</th></tr>\n")
	for _, pkg := range s.packages {
		fmt.Fprintf(&body, "<tr><td><a href=\"%s\">%s</a></td><td>%s</td></tr>\n",
			pkg.page, html.EscapeString(pkg.doc.ImportPath), html.EscapeString(pkg.synopsis))
	}
	body.WriteString("</table>\n")
	return s.layout(s.module, body.Bytes())
}

func (s *site) packagePage(pkg *packageDoc) []byte {
	var body bytes.Buffer
	fmt.Fprintf(&body, "<h1>package %s</h1>\n", html.EscapeString(pkg.doc.Name))
	fmt.Fprintf(&body, "<p class=\"import\"><code>import %q</code></p>\n", pkg.doc.ImportPath)
	body.WriteString(s.docHTML(pkg, pkg.doc.Doc))
	s.examples(&body, pkg, pkg.doc.Examples)

	body.WriteString("<h2>Index</h2>\n<ul class=\"index\">\n")
	if len(pkg.doc.Consts) > 0 {
		body.WriteString("<li><a href=\"#pkg-constants\">Constants</a></li>\n")
	}
	if len(pkg.doc.Vars) > 0 {
		body.WriteString("<li><a href=\"#pkg-variables\">Variables</a></li>\n")
	}
	for _, fn := range pkg.doc.Funcs {
		fmt.Fprintf(&body, "<li><a href=\"#%s\">%s</a></li>\n", fn.Name, html.EscapeString(signature(pkg, fn)))
	}
	for _, typ := range pkg.doc.Types {
		fmt.Fprintf(&body, "<li><a href=\"#%s\">type %s</a>\n", typ.Name, typ.Name)
		var members []string
		for _, fn := range append(append([]*doc.Func{}, typ.Funcs...), typ.Methods...) {
			members = append(members, fmt.Sprintf("<li><a href=\"#%s\">%s</a></li>",
				anchor(fn), html.EscapeString(signature(pkg, fn))))
		}
		if len(members) > 0 {
			fmt.Fprintf(&body, "<ul>\n%s\n</ul>\n", strings.Join(members, "\n"))
		}
		body.WriteString("</li>\n")
	}
	body.WriteString("</ul>\n")

	if len(pkg.doc.Consts) > 0 {
		body.WriteString("<h2 id=\"pkg-constants\">Constants</h2>\n")
		for _, value := range pkg.doc.Consts {
			s.value(&body, pkg, value)
		}
	}
	if len(pkg.doc.Vars) > 0 {
		body.WriteString("<h2 id=\"pkg-variables\">Variables</h2>\n")
		for _, value := range pkg.doc.Vars {
			s.value(&body, pkg, value)
		}
	}
	if len(pkg.doc.Funcs) > 0 {
		body.WriteString("<h2>Functions</h2>\n")
		for _, fn := range pkg.doc.Funcs {
			s.function(&body, pkg, fn, "h3")
		}
	}
	if len(pkg.doc.Types) > 0 {
		body.WriteString("<h2>Types</h2>\n")
		for _, typ := range pkg.doc.Types {
			fmt.Fprintf(&body, "<h3 id=\"%s\">type %s %s</h3>\n", typ.Name, typ.Name, s.sourceLink(pkg, typ.Decl.Pos()))
			s.code(&body, pkg, typ.Decl)
			body.WriteString(s.docHTML(pkg, typ.Doc))
			s.examples(&body, pkg, typ.Examples)
			for _, value := range append(append([]*doc.Value{}, typ.Consts...), typ.Vars...) {
				s.value(&body, pkg, value)
			}
			for _, fn := range typ.Funcs {
				s.function(&body, pkg, fn, "h4")
			}
			for _, fn := range typ.Methods {
				s.function(&body, pkg, fn, "h4")
			}
		}
	}
	title := "package " + pkg.doc.Name + " - " + s.module
	return s.layout(title, body.Bytes())
}

func (s *site) function(body *bytes.Buffer, pkg *packageDoc, fn *doc.Func, heading string) {
	fmt.Fprintf(body, "<%s id=\"%s\">%s %s</%s>\n",
		heading, anchor(fn), html.EscapeString(signature(pkg, fn)), s.sourceLink(pkg, fn.Decl.Pos()), heading)
	s.code(body, pkg, fn.Decl)
	body.WriteString(s.docHTML(pkg, fn.Doc))
	s.examples(body, pkg, fn.Examples)
}

func (s *site) value(body *bytes.Buffer, pkg *packageDoc, value *doc.Value) {
	s.code(body, pkg, value.Decl)
	body.WriteString(s.docHTML(pkg, value.Doc))
}

func (s *site) examples(body *bytes.Buffer, pkg *packageDoc, examples []*doc.Example) {
	for _, example := range examples {
		name := "Example"
		if example.Suffix != "" {
			name += " (" + example.Suffix + ")"
		}
		fmt.Fprintf(body, "<details class=\"example\"><summary>%s</summary>\n", html.EscapeString(name))
		body.WriteString(s.docHTML(pkg, example.Doc))
		var code bytes.Buffer
		node := any(example.Code)
		if example.Play != nil {
			node = example.Play
		}
		if err := (&printer.Config{Mode: printer.UseSpaces, Tabwidth: 4}).Fprint(&code, pkg.fset, node); err != nil {
			code.WriteString(err.Error())
		}
		text := strings.TrimSpace(code.String())
		text = strings.TrimPrefix(text, "{")
		text = strings.TrimSuffix(text, "}")
		fmt.Fprintf(body, "<pre>%s</pre>\n", html.EscapeString(dedent(text)))
		if example.Output != "" {
			fmt.Fprintf(body, "<p>Output:</p>\n<pre>%s</pre>\n", html.EscapeString(strings.TrimSpace(example.Output)))
		}
		body.WriteString("</details>\n")
	}
}

// code prints a declaration with its interior comments, bodies already
// stripped by go/doc.
func (s *site) code(body *bytes.Buffer, pkg *packageDoc, decl ast.Decl) {
	var buffer bytes.Buffer
	node := &printer.CommentedNode{Node: decl, Comments: pkg.fileFor(decl.Pos()).Comments}
	if err := (&printer.Config{Mode: printer.UseSpaces, Tabwidth: 4}).Fprint(&buffer, pkg.fset, node); err != nil {
		buffer.WriteString(err.Error())
	}
	fmt.Fprintf(body, "<pre class=\"decl\">%s</pre>\n", html.EscapeString(buffer.String()))
}

func (pkg *packageDoc) fileFor(pos token.Pos) *ast.File {
	name := pkg.fset.Position(pos).Filename
	return pkg.files[name]
}

// docHTML renders a doc comment, cross-linking identifiers to their pages.
func (s *site) docHTML(pkg *packageDoc, text string) string {
	if strings.TrimSpace(text) == "" {
		return ""
	}
	parsed := pkg.doc.Parser().Parse(text)
	renderer := pkg.doc.Printer()
	renderer.HeadingLevel = 4
	renderer.DocLinkURL = func(link *comment.DocLink) string {
		importPath := link.ImportPath
		if importPath == "" {
			importPath = pkg.doc.ImportPath
		}
		target := s.pageFor(importPath)
		if target == "" {
			return link.DefaultURL("https://pkg.go.dev")
		}
		fragment := link.Name
		if link.Recv != "" {
			fragment = link.Recv + "." + link.Name
		}
		if fragment == "" {
			return target
		}
		return target + "#" + fragment
	}
	return string(renderer.HTML(parsed))
}

// signature is the index label of a function: "func Name" or "func (Recv) Name".
func signature(pkg *packageDoc, fn *doc.Func) string {
	if fn.Recv == "" {
		return "func " + fn.Name
	}
	return "func (" + fn.Recv + ") " + fn.Name
}

// anchor names a function or method fragment: "Name" or "Recv.Name".
func anchor(fn *doc.Func) string {
	if fn.Recv == "" {
		return fn.Name
	}
	recv := strings.TrimPrefix(fn.Recv, "*")
	if index := strings.IndexByte(recv, '['); index >= 0 {
		recv = recv[:index]
	}
	return recv + "." + fn.Name
}

// sourceLink links a position to the repository blob view.
func (s *site) sourceLink(pkg *packageDoc, pos token.Pos) string {
	if s.repo == "" {
		return ""
	}
	position := pkg.fset.Position(pos)
	href := s.repo + "/blob/main/" + path.Clean(filepath.ToSlash(position.Filename)) +
		fmt.Sprintf("#L%d", position.Line)
	return fmt.Sprintf("<a class=\"source\" href=\"%s\">source</a>", html.EscapeString(href))
}

func dedent(text string) string {
	lines := strings.Split(text, "\n")
	prefix := ""
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		indent := line[:len(line)-len(strings.TrimLeft(line, " \t"))]
		if prefix == "" || len(indent) < len(prefix) {
			prefix = indent
		}
	}
	for i, line := range lines {
		lines[i] = strings.TrimPrefix(line, prefix)
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func (s *site) layout(title string, body []byte) []byte {
	var page bytes.Buffer
	page.WriteString("<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n")
	page.WriteString("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n")
	fmt.Fprintf(&page, "<title>%s</title>\n<style>%s</style>\n</head>\n<body>\n", html.EscapeString(title), style)
	page.WriteString("<nav><a href=\"index.html\">" + html.EscapeString(s.module) + "</a><span>")
	for _, pkg := range s.packages {
		fmt.Fprintf(&page, " <a href=\"%s\">%s</a>", pkg.page, html.EscapeString(pkg.doc.Name))
	}
	page.WriteString("</span></nav>\n<main>\n")
	page.Write(body)
	page.WriteString("\n</main>\n</body>\n</html>\n")
	return page.Bytes()
}

const style = `
:root { color-scheme: light dark; --line: #d7dbe0; --accent: #0b6e99; --code-bg: #f5f6f8; }
@media (prefers-color-scheme: dark) { :root { --line: #3a4048; --accent: #6cb6d9; --code-bg: #22262c; } }
* { box-sizing: border-box; }
body { margin: 0; font: 16px/1.6 system-ui, sans-serif; }
nav { display: flex; flex-wrap: wrap; gap: .75rem; align-items: baseline; padding: .75rem 1.25rem;
      border-bottom: 1px solid var(--line); }
nav > a { font-weight: 600; text-decoration: none; color: inherit; }
nav span a { margin-right: .6rem; }
main { max-width: 60rem; margin: 0 auto; padding: 1rem 1.25rem 4rem; }
a { color: var(--accent); }
h1 { font-size: 1.6rem; }
h2 { border-bottom: 1px solid var(--line); padding-bottom: .25rem; margin-top: 2.5rem; }
h3 { margin-top: 2rem; }
h3 .source, h4 .source { font-size: .75rem; font-weight: 400; margin-left: .6rem; }
pre { background: var(--code-bg); padding: .75rem 1rem; border-radius: 6px; overflow-x: auto;
      font-size: .85rem; line-height: 1.5; }
code { background: var(--code-bg); padding: .1rem .3rem; border-radius: 4px; font-size: .9em; }
pre code { padding: 0; background: none; }
table.packages { border-collapse: collapse; width: 100%; }
table.packages th, table.packages td { text-align: left; padding: .4rem .75rem; border-bottom: 1px solid var(--line); }
ul.index { list-style: none; padding-left: 0; columns: 1; }
ul.index ul { list-style: none; padding-left: 1.25rem; }
details.example { margin: .75rem 0; border: 1px solid var(--line); border-radius: 6px; padding: .5rem .75rem; }
details.example summary { cursor: pointer; font-weight: 600; }
p.import code { font-size: .95rem; }
`
