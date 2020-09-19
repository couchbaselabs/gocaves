package mock

import (
	"regexp"
	"strings"
)

// PathParser is a helper to aid in the parsing of paths.
type PathParser struct {
	rgx *regexp.Regexp
}

// NewPathParser creates a new parser based on a specific template.
func NewPathParser(parseTpl string) *PathParser {
	rgxPath := parseTpl
	rgxPath = strings.ReplaceAll(rgxPath, "/", "\\/")
	rgxPath = strings.ReplaceAll(rgxPath, "\\*", "{{--ESCAPED-ASTERIX--}}")
	rgxPath = strings.ReplaceAll(rgxPath, "**", "{{--ANY-PATH--}}")
	rgxPath = strings.ReplaceAll(rgxPath, "*", "{{--DIRECTORY--}}")
	rgxPath = strings.ReplaceAll(rgxPath, "{{--ESCAPED-ASTERIX--}}", "\\*")
	rgxPath = strings.ReplaceAll(rgxPath, "{{--ANY-PATH--}}", "(.*)")
	rgxPath = strings.ReplaceAll(rgxPath, "{{--DIRECTORY--}}", "([^\\/]*)")

	return &PathParser{
		rgx: regexp.MustCompile(rgxPath),
	}
}

// Match returns whether the provided path matches the path parser template.
func (p *PathParser) Match(path string) bool {
	return p.rgx.MatchString(path)
}

// ParseParts returns the parsed parts of a path.
func (p *PathParser) ParseParts(path string) []string {
	res := p.rgx.FindStringSubmatch(path)
	if len(res) == 0 {
		return nil
	}
	return res[1:]
}

// ParsePathParts parses a path by template and returns the parts.
func ParsePathParts(path, tpl string) []string {
	return NewPathParser(tpl).ParseParts(path)
}
