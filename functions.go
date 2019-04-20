package golib

import (
	"strings"
	"unicode"

	"github.com/felipefoliatti/errors"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

//TryError cria um novo objeto Logger que irá logar no console.
func TryError(err *errors.Error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

//TryErrorStack cria um novo objeto Logger que irá logar no console.
func TryErrorStack(err *errors.Error) string {
	if err != nil {
		return err.ErrorStack()
	}
	return ""
}

func FilterNewLines(s string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case 0x000A, 0x000B, 0x000C, 0x000D, 0x0085, 0x2028, 0x2029:
			return -1
		default:
			return r
		}
	}, s)
}

func RemoveAccents(s string) string {
	isMn := func(r rune) bool {
		return unicode.Is(unicode.Mn, r) // Mn: nonspacing marks
	}

	t := transform.Chain(norm.NFD, transform.RemoveFunc(isMn), norm.NFC)
	result, _, _ := transform.String(t, s)
	return result
}
