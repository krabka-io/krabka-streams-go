package main

import (
	"html"
	"regexp"
	"strings"
	"testing"
)

var tags = regexp.MustCompile(`<[^>]*>`)

// plain removes the tags highlightGo added and unescapes the text.
func plain(s string) string {
	return html.UnescapeString(tags.ReplaceAllString(s, ""))
}

func TestHighlightGoReadsBackAsTheSource(t *testing.T) {
	sources := []string{
		"type Producer struct {\n\tName string // the producer's name\n\tRate int\n}",
		"func (p *Producer) Send(ctx context.Context, v any) (int64, error) {\n\treturn 0, nil\n}",
		"const (\n\tA = iota\n\tB\n)\n\nvar s = `raw <b>&amp;</b>`",
		"if a < b && c > d {\n\tfmt.Println(\"x\")\n}",
		"x := 3.5e2 + 0x1F",
		"",
	}
	for _, src := range sources {
		if got := plain(highlightGo(src)); got != src {
			t.Errorf("highlightGo(%q) reads back as %q", src, got)
		}
	}
}

func TestHighlightGoClassifiesTokens(t *testing.T) {
	out := highlightGo("func Send(n int) error {\n\t// note\n\treturn fmt.Errorf(\"no %d\", 42)\n}\nvar ok = true")
	for _, want := range []string{
		`<span class="tk-k">func</span>`,
		`<span class="tk-f">Send</span>`,
		`<span class="tk-t">int</span>`,
		`<span class="tk-t">error</span>`,
		`<span class="tk-c">// note</span>`,
		`<span class="tk-k">return</span>`,
		`<span class="tk-f">Errorf</span>`,
		`<span class="tk-s">&#34;no %d&#34;</span>`,
		`<span class="tk-n">42</span>`,
		`<span class="tk-l">true</span>`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("highlighted output lacks %s\n%s", want, out)
		}
	}
}

func TestHighlightGoEscapesWhatItCannotScan(t *testing.T) {
	// An unterminated string makes the scanner report an error; the text must
	// still come out escaped, not dropped or injected.
	src := "s := \"<script>"
	out := highlightGo(src)
	if strings.Contains(out, "<script>") || plain(out) != src {
		t.Errorf("unscannable source came out as %q", out)
	}
}

func TestDeclarationsAreHighlighted(t *testing.T) {
	s := testSite(t)
	page := string(s.packagePage(s.packages[0]))
	if !strings.Contains(page, `<pre class="decl"><span class="tk-k">func</span> <span class="tk-f">Hello</span>()`) {
		t.Errorf("the Hello declaration is not highlighted:\n%s", page)
	}
	if !strings.Contains(page, ".tk-k{color:#ff8466}") {
		t.Error("the stylesheet lacks the token colours")
	}
}
