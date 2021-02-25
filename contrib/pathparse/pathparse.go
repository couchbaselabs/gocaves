package pathparse

import (
	"regexp"
	"strings"
)

// Parser is a helper to aid in the parsing of paths.
type Parser struct {
	rgx *regexp.Regexp
}

// NewParser creates a new parser based on a specific template.
func NewParser(parseTpl string) *Parser {
	rgxPath := parseTpl
	rgxPath = strings.ReplaceAll(rgxPath, "/", "\\/")
	rgxPath = strings.ReplaceAll(rgxPath, "\\*", "{{--ESCAPED-ASTERIX--}}")
	rgxPath = strings.ReplaceAll(rgxPath, "**", "{{--ANY-PATH--}}")
	rgxPath = strings.ReplaceAll(rgxPath, "*", "{{--DIRECTORY--}}")
	rgxPath = strings.ReplaceAll(rgxPath, "{{--ESCAPED-ASTERIX--}}", "\\*")
	rgxPath = strings.ReplaceAll(rgxPath, "{{--ANY-PATH--}}", "(.*)")
	rgxPath = strings.ReplaceAll(rgxPath, "{{--DIRECTORY--}}", "([^\\/]*)")

	if !strings.HasSuffix(rgxPath, "$") {
		rgxPath = rgxPath + "$"
	}

	return &Parser{
		rgx: regexp.MustCompile(rgxPath),
	}
}

// Match returns whether the provided path matches the path parser template.
func (p *Parser) Match(path string) bool {
	return p.rgx.MatchString(path)
}

// ParseParts returns the parsed parts of a path.
func (p *Parser) ParseParts(path string) []string {
	res := p.rgx.FindStringSubmatch(path)
	if len(res) == 0 {
		return nil
	}
	return res[1:]
}

// ParseParts parses a path by template and returns the parts.
func ParseParts(path, tpl string) []string {
	return NewParser(tpl).ParseParts(path)
}
