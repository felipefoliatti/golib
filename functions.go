package golib

//NewLogger cria um novo objeto Logger que irá logar no console.
func TryError(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}
