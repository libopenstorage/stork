// Package truncate provides set of strategies to truncate strings
package truncate

import (
	"math"
	"unicode/utf8"
)

type TruncatePosition int

const DEFAULT_OMISSION = "…"

const (
	PositionStart TruncatePosition = iota
	PositionMiddle
	PositionEnd
)

// Strategy is an interface for truncation strategy
type Strategy interface {
	Truncate(string, int) string
}

// Truncator cuts a string to length using the truncation strategy
func Truncator(str string, length int, strategy Strategy) string {
	return strategy.Truncate(str, length)
}

// CutStrategy simply truncates the string to the desired length
type CutStrategy struct{}

func (CutStrategy) Truncate(str string, length int) string {
	return Truncate(str, length, "", PositionEnd)
}

// CutEllipsisStrategy simply truncates the string to the desired length and adds ellipsis at the end
type CutEllipsisStrategy struct{}

func (s CutEllipsisStrategy) Truncate(str string, length int) string {
	return Truncate(str, length, DEFAULT_OMISSION, PositionEnd)
}

// CutEllipsisLeadingStrategy simply truncates the string from the start the desired length and adds ellipsis at the front
type CutEllipsisLeadingStrategy struct{}

func (s CutEllipsisLeadingStrategy) Truncate(str string, length int) string {
	return Truncate(str, length, DEFAULT_OMISSION, PositionStart)
}

// EllipsisMiddleStrategy truncates the string to the desired length and adds ellipsis in the middle
type EllipsisMiddleStrategy struct{}

func (e EllipsisMiddleStrategy) Truncate(str string, length int) string {
	return Truncate(str, length, DEFAULT_OMISSION, PositionMiddle)
}

// Truncate truncates string according the parameters
func Truncate(str string, length int, omission string, pos TruncatePosition) string {
	r := []rune(str)
	sLen := len(r)
	if length >= sLen {
		return str
	}
	switch pos {
	case PositionStart:
		return truncateStart(r, length, omission)
	case PositionMiddle:
		return truncateMiddle(r, length, omission)
	default:
		return truncateEnd(r, length, omission)
	}
}

func truncateStart(r []rune, length int, omission string) string {
	return string(omission + string(r[len(r)-length+utf8.RuneCountInString(omission):]))
}

func truncateEnd(r []rune, length int, omission string) string {
	return string(string(r[:length-utf8.RuneCountInString(omission)]) + omission)
}

func truncateMiddle(r []rune, length int, omission string) string {
	sLen := len(r)
	oLen := utf8.RuneCountInString(omission)
	// Make sure we have one character before and after
	if length < oLen+2 {
		return truncateEnd(r, length, "")
	}
	var delta int
	if sLen%2 == 0 {
		delta = int(math.Ceil(float64(length-oLen) / 2))
	} else {
		delta = int(math.Floor(float64(length-oLen) / 2))
	}
	result := make([]rune, length)
	copy(result, r[0:delta])
	copy(result[delta:], []rune(omission))
	copy(result[delta+oLen:], r[sLen-length+oLen+delta:])
	return string(result)
}
