package golib

import "os"

//Getenv helps to handle null value.
//If the env variable doesn't exist, then the fallback string is returned
func Getenv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}
