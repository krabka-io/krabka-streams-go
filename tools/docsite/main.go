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
	"net/url"
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
		fmt.Fprintf(body, "<pre>%s</pre>\n", highlightGo(dedent(text)))
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
	fmt.Fprintf(body, "<pre class=\"decl\">%s</pre>\n", highlightGo(buffer.String()))
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
	page.WriteString("<meta name=\"color-scheme\" content=\"dark\">\n")
	fmt.Fprintf(&page, "<title>%s</title>\n", html.EscapeString(title))
	fmt.Fprintf(&page, "<link rel=\"icon\" type=\"image/svg+xml\" href=\"%s\">\n", logoDataURI)
	fmt.Fprintf(&page, "<style>%s</style>\n</head>\n<body>\n", style)
	page.WriteString("<header class=\"site-header\"><nav aria-label=\"Packages\">")
	fmt.Fprintf(&page, "<a class=\"brand\" href=\"index.html\"><img class=\"logo\" src=\"%s\" alt=\"\" width=\"24\" height=\"24\">%s</a>",
		logoDataURI, html.EscapeString(s.module))
	page.WriteString("<span class=\"pkgs\">")
	for _, pkg := range s.packages {
		fmt.Fprintf(&page, "<a href=\"%s\">%s</a>", pkg.page, html.EscapeString(pkg.doc.Name))
	}
	page.WriteString("</span></nav></header>\n<main>\n")
	page.Write(body)
	page.WriteString("\n</main>\n")
	fmt.Fprintf(&page, "<footer class=\"site-footer\">API reference for %s, generated from godoc comments.</footer>\n", html.EscapeString(s.module))
	page.WriteString("</body>\n</html>\n")
	return page.Bytes()
}

// logoSVG is the krabka logo, shared with the krabka.io sites.
const logoSVG = `<svg viewBox="0 0 90 90" width="100%" height="100%" fill="none" xmlns="http://www.w3.org/2000/svg"><g transform="matrix(1.2410837,0,0,1.2410837,-10.847205,1.7895666)"><ellipse style="fill:none;stroke:#FF4D2E;stroke-width:7.6591;stroke-dasharray:none" cx="45" cy="45" rx="13.749256" ry="8.6910944"/><g><circle style="fill:none;stroke:#FF4D2E;stroke-width:5.01105;stroke-dasharray:none" cx="31.509378" cy="21.770279" r="7.1520071"/><path style="fill:none;stroke:#FF4D2E;stroke-width:5.01105;stroke-dasharray:none" d="m -37.432668,-19.054935 a 7.1520071,7.1520071 0 0 1 -4.415052,6.607593 7.1520071,7.1520071 0 0 1 -7.794188,-1.550361 7.1520071,7.1520071 0 0 1 -1.55036,-7.794187 7.1520071,7.1520071 0 0 1 6.607593,-4.415053" transform="rotate(-135)"/><rect style="fill:#FF4D2E;stroke:none;stroke-width:5.29167;stroke-dasharray:none" width="4.4989443" height="12.777002" x="14.19296" y="41.136307" transform="rotate(-30)"/></g><g transform="matrix(-1,0,0,1,89.997483,0)"><circle style="fill:none;stroke:#FF4D2E;stroke-width:5.01105;stroke-dasharray:none" cx="31.509378" cy="21.770279" r="7.1520071"/><path style="fill:none;stroke:#FF4D2E;stroke-width:5.01105;stroke-dasharray:none" d="m -37.432668,-19.054935 a 7.1520071,7.1520071 0 0 1 -4.415052,6.607593 7.1520071,7.1520071 0 0 1 -7.794188,-1.550361 7.1520071,7.1520071 0 0 1 -1.55036,-7.794187 7.1520071,7.1520071 0 0 1 6.607593,-4.415053" transform="rotate(-135)"/><rect style="fill:#FF4D2E;stroke:none;stroke-width:5.29167;stroke-dasharray:none" width="4.4989443" height="12.777002" x="14.19296" y="41.136307" transform="rotate(-30)"/></g></g></svg>`

// logoDataURI is logoSVG percent-encoded for use in an href or src.
var logoDataURI = "data:image/svg+xml," + url.PathEscape(logoSVG)

// style is the krabka.io theme: one dark navy palette, Inter for text and
// JetBrains Mono for code. The font import must stay the first rule.
const style = `@import url("https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap");
:root{color-scheme:dark;--bg:#080d1a;--surface:#0c1322;--code-bg:#0f172a;--line:#1e293b;--line-strong:#334155;--text:#e5e7eb;--heading:#f3f4f6;--muted:#9ca3af;--accent:#ff4d2e;--link:#ff8466;--link-hover:#ffb39e;--font:Inter,system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;--mono:"JetBrains Mono",ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
*,*::before,*::after{box-sizing:border-box}
html{-webkit-text-size-adjust:100%;scroll-behavior:smooth;scroll-padding-top:6rem}
body{margin:0;min-height:100vh;color:var(--text);font:400 16px/1.65 var(--font);background-color:var(--bg);background-image:radial-gradient(ellipse 80% 50% at 50% -20%,rgba(30,58,138,.22),transparent 70%),radial-gradient(ellipse 60% 40% at 100% 30%,rgba(15,23,42,.4),transparent 60%);background-attachment:fixed}
a{color:var(--link);text-decoration:none}
a:hover,a:focus-visible{color:var(--link-hover);text-decoration:underline}
:focus-visible{outline:2px solid var(--accent);outline-offset:2px;border-radius:4px}
.site-header{position:sticky;top:0;z-index:10;background:rgba(12,19,34,.92);border-bottom:1px solid var(--line);-webkit-backdrop-filter:blur(8px);backdrop-filter:blur(8px)}
.site-header nav{display:flex;flex-wrap:wrap;align-items:center;gap:.25rem 1.25rem;max-width:62rem;margin:0 auto;padding:.7rem 1rem}
.brand{display:inline-flex;align-items:center;gap:.6rem;color:var(--heading);font-weight:700;letter-spacing:-.01em;overflow-wrap:anywhere}
.brand:hover,.brand:focus-visible{color:var(--heading)}
.logo{display:block;flex:none;width:1.5rem;height:1.5rem}
.pkgs{display:flex;flex-wrap:wrap;gap:.1rem .9rem;font-size:.9rem}
.pkgs a{color:var(--muted)}
.pkgs a:hover,.pkgs a:focus-visible{color:var(--link-hover)}
main{max-width:62rem;margin:0 auto;padding:2rem 1rem 3rem;min-width:0}
h1,h2,h3,h4{color:var(--heading);line-height:1.25;letter-spacing:-.015em}
h1{margin:0 0 .75rem;font-size:clamp(1.6rem,4vw,2.25rem);font-weight:700;letter-spacing:-.02em;overflow-wrap:anywhere}
h2{margin:3rem 0 1rem;padding-bottom:.5rem;border-bottom:1px solid var(--line);font-size:1.5rem;font-weight:700}
h3{margin:2.5rem 0 .75rem;padding-top:1.25rem;border-top:1px solid var(--line);font-size:1.2rem;font-weight:600;overflow-wrap:anywhere}
h4{margin:1.5rem 0 .5rem;font-size:1.02rem;font-weight:600;overflow-wrap:anywhere}
h2+h3{margin-top:1rem;padding-top:0;border-top:0}
h3:target,h4:target{color:var(--link-hover)}
h3:target,h4:target,h2:target{box-shadow:-.75rem 0 0 -.5rem var(--accent)}
h3 .source,h4 .source{margin-left:.6rem;font-family:var(--mono);font-size:.72rem;font-weight:500;color:var(--muted)}
h3 .source:hover,h4 .source:hover{color:var(--link-hover)}
p{margin:.75rem 0;overflow-wrap:anywhere}
ul,ol{padding-left:1.4rem}
code,pre{font-family:var(--mono)}
code{background:var(--code-bg);border:1px solid var(--line);padding:.08rem .35rem;border-radius:.3rem;font-size:.88em;font-weight:500}
pre{max-width:100%;margin:.9rem 0;padding:.9rem 1rem;overflow-x:auto;background:var(--code-bg);border:1px solid var(--line);border-radius:.6rem;font-size:.85rem;line-height:1.55;font-weight:400;tab-size:4}
pre code{padding:0;border:0;background:none;font-size:inherit}
pre.decl{border-left:3px solid var(--accent)}
.tk-k{color:#ff8466}.tk-t{color:#fcd9a8}.tk-l{color:#ffb39e}.tk-s{color:#86efac}.tk-n{color:#fdba74}.tk-f{color:#93c5fd}.tk-c{color:#8b949e}
p.import code{display:inline-block;max-width:100%;overflow-wrap:anywhere;color:var(--link-hover);font-size:.95rem}
table.packages{width:100%;border-collapse:separate;border-spacing:0;margin:1.5rem 0;overflow:hidden;background:var(--code-bg);border:1px solid var(--line);border-radius:.75rem}
table.packages th,table.packages td{padding:.8rem 1rem;text-align:left;vertical-align:top;border-bottom:1px solid var(--line)}
table.packages tr:last-child td{border-bottom:0}
table.packages th{background:rgba(12,19,34,.9);color:var(--muted);font-size:.78rem;font-weight:600;letter-spacing:.06em;text-transform:uppercase}
table.packages td{color:var(--muted)}
table.packages td:first-child{font-family:var(--mono);font-size:.9rem;overflow-wrap:anywhere}
table.packages tr:hover td{background:rgba(255,77,46,.05)}
ul.index{margin:1rem 0;padding:1rem 1.25rem;list-style:none;background:var(--surface);border:1px solid var(--line);border-radius:.75rem;columns:16rem;column-gap:2rem;font-family:var(--mono);font-size:.88rem}
ul.index li{margin:.2rem 0;break-inside:avoid;overflow-wrap:anywhere}
ul.index ul{margin:.2rem 0 .5rem;padding-left:1.1rem;list-style:none;border-left:1px solid var(--line-strong)}
ul.index ul li{padding-left:.6rem}
details.example{margin:1rem 0;background:var(--surface);border:1px solid var(--line);border-radius:.6rem;padding:0 1rem}
details.example[open]{border-color:var(--line-strong);padding-bottom:.25rem}
details.example summary{padding:.65rem 0;cursor:pointer;color:var(--heading);font-weight:600;list-style-position:inside}
details.example summary::marker{color:var(--accent)}
details.example summary:hover{color:var(--link-hover)}
details.example pre{background:var(--bg)}
.site-footer{max-width:62rem;margin:0 auto;padding:1.5rem 1rem 3rem;border-top:1px solid var(--line);color:var(--muted);font-size:.9rem}
@media (max-width:40rem){main{padding-top:1.5rem}table.packages th,table.packages td{padding:.65rem .7rem}h2{font-size:1.3rem}}
@media (max-width:48rem){.site-header{position:static}html{scroll-padding-top:1rem}}
@media (prefers-reduced-motion:reduce){html{scroll-behavior:auto}*,*::before,*::after{transition:none!important;animation:none!important}}
`
