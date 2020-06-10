package util

import "log"

// PanicErr checks and panics any error
func PanicErr(err error) {
	if err != nil {
		panic(err)
	}
}

// LogErr checks and logs any error
func LogErr(err error, msg string) {
	if err != nil {
		log.Println(msg, err)
	}
}
