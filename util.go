package godynamo

import (
	"reflect"
)

func attributeType(k reflect.Kind) string {
	switch k {
	case reflect.Uint8:
		return "B"
	case reflect.String:
		return "S"
	default:
		return "N"
	}
}

func valueElem(t reflect.Type) reflect.Type {
	switch t.Kind() {
	case reflect.Interface, reflect.Ptr:
		for t.Kind() == reflect.Interface || t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
	}
	return t
}
