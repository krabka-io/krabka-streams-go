package main

import (
	"go/scanner"
	"go/token"
	"html"
	"strings"
)

// highlightGo returns src as HTML with each token wrapped in a span whose
// class names its kind. The text between tokens (spaces, newlines) is copied
// as it is, so the output reads back as the input with tags added.
//
// The scanner is Go's own, so the highlighting cannot disagree with the
// language: keywords, literals and comments come from go/token, and the two
// rules that need context are small. An identifier followed by "(" is a
// call or a declared function, and an identifier in the predeclared set is a
// type or a constant. Source the scanner rejects is printed unhighlighted.
func highlightGo(src string) string {
	var out strings.Builder
	fset := token.NewFileSet()
	file := fset.AddFile("", fset.Base(), len(src))
	var errs int
	var s scanner.Scanner
	s.Init(file, []byte(src), func(token.Position, string) { errs++ }, scanner.ScanComments)

	type item struct {
		offset int
		tok    token.Token
		lit    string
	}
	var items []item
	for {
		pos, tok, lit := s.Scan()
		if tok == token.EOF {
			break
		}
		items = append(items, item{offset: file.Offset(pos), tok: tok, lit: lit})
	}
	if errs > 0 {
		return html.EscapeString(src)
	}

	cursor := 0
	for i, it := range items {
		// The scanner reports an automatic semicolon as ";" with the literal
		// "\n"; the source has no such token there, so it prints nothing and
		// the newline is copied with the text before the next token.
		if it.tok == token.SEMICOLON && it.lit == "\n" {
			continue
		}
		if it.offset > cursor {
			out.WriteString(html.EscapeString(src[cursor:it.offset]))
		}
		text := it.lit
		if text == "" {
			text = it.tok.String()
		}
		end := it.offset + len(text)
		if end > len(src) {
			end = len(src)
		}
		raw := src[it.offset:end]
		class := ""
		switch {
		case it.tok == token.COMMENT:
			class = "tk-c"
		case it.tok == token.STRING || it.tok == token.CHAR:
			class = "tk-s"
		case it.tok == token.INT || it.tok == token.FLOAT || it.tok == token.IMAG:
			class = "tk-n"
		case it.tok.IsKeyword():
			class = "tk-k"
		case it.tok == token.IDENT:
			switch {
			case predeclaredLiteral[raw]:
				class = "tk-l"
			case predeclaredType[raw]:
				class = "tk-t"
			case i+1 < len(items) && items[i+1].tok == token.LPAREN:
				class = "tk-f"
			}
		}
		if class == "" {
			out.WriteString(html.EscapeString(raw))
		} else {
			out.WriteString(`<span class="` + class + `">` + html.EscapeString(raw) + `</span>`)
		}
		cursor = end
	}
	if cursor < len(src) {
		out.WriteString(html.EscapeString(src[cursor:]))
	}
	return out.String()
}

var predeclaredType = map[string]bool{
	"any": true, "bool": true, "byte": true, "comparable": true, "complex64": true, "complex128": true,
	"error": true, "float32": true, "float64": true, "int": true, "int8": true, "int16": true,
	"int32": true, "int64": true, "rune": true, "string": true, "uint": true, "uint8": true,
	"uint16": true, "uint32": true, "uint64": true, "uintptr": true,
}

var predeclaredLiteral = map[string]bool{"true": true, "false": true, "nil": true, "iota": true}
