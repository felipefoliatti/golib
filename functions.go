package golib

import "strings"

//NewLogger cria um novo objeto Logger que irá logar no console.
func TryError(err error) string {
	if err != nil {
		return err.Error()
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
