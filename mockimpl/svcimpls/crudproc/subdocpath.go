package crudproc

import (
	"errors"
	"strconv"
)

// SubDocPathComponent represents one part of a sub-document path.
type SubDocPathComponent struct {
	Path       string
	ArrayIndex int
}

// ParsePath takes a sub-document path and splits it into components.
func ParsePath(path string) ([]SubDocPathComponent, error) {
	parts := make([]SubDocPathComponent, 0)
	compBuffer := ""

	for charIdx := 0; charIdx < len(path); charIdx++ {
		char := path[charIdx]

		if char == '`' {
			charIdx++
			for ; charIdx < len(path); charIdx++ {
				char = path[charIdx]
				if char == '`' {
					break
				}

				compBuffer += string(char)
			}

			if charIdx == len(path) {
				// unterminated backtick
				return nil, errors.New("unterminated backtick")
			}

			if compBuffer == "" {
				return nil, errors.New("backticks with no content")
			}
			parts = append(parts, SubDocPathComponent{
				Path: compBuffer,
			})
			compBuffer = ""

			if charIdx+1 == len(path) {
				// end of the string now
				return parts, nil
			}

			charIdx++
		} else if char == '[' {
			if compBuffer != "" {
				parts = append(parts, SubDocPathComponent{
					Path: compBuffer,
				})
				compBuffer = ""
			}

			charIdx++
			for ; charIdx < len(path); charIdx++ {
				char = path[charIdx]
				if char == ']' {
					break
				}

				compBuffer += string(char)
			}

			if charIdx == len(path) {
				// unterminated array index
				return nil, errors.New("unterminated array index")
			}

			if compBuffer == "" {
				return nil, errors.New("array index with no index")
			}

			index, err := strconv.ParseInt(compBuffer, 10, 64)
			if err != nil {
				return nil, err
			}

			parts = append(parts, SubDocPathComponent{
				ArrayIndex: int(index),
			})
			compBuffer = ""

			if charIdx+1 == len(path) {
				// end of the string now
				return parts, nil
			}

			charIdx++
		} else if char == '\\' {
			if charIdx+1 == len(path) {
				// escape without a character
				return nil, errors.New("escape with no character")
			}

			compBuffer += string(path[charIdx+1])
			charIdx++
			charIdx++
		} else if char == '.' {
			if compBuffer == "" {
				return nil, errors.New("period with no content")
			}

			parts = append(parts, SubDocPathComponent{
				Path: compBuffer,
			})
			compBuffer = ""
		} else {
			compBuffer += string(char)
		}
	}

	if compBuffer == "" {
		return nil, errors.New("period with no content after")
	}

	parts = append(parts, SubDocPathComponent{
		Path: compBuffer,
	})

	return parts, nil
}
