package misc

import (
	"fmt"
	"reflect"

	"github.com/kdpdev/extsort/internal/utils/alg"
)

func ToPrettyString(obj interface{}) string {

	var impl func(obj interface{}, indent int) string
	impl = func(obj interface{}, indent int) string {
		typeOf := reflect.TypeOf(obj)
		if typeOf.Kind() != reflect.Struct {
			return fmt.Sprint(obj)
		}

		val := reflect.ValueOf(obj)
		numField := val.NumField()
		result := val.Type().Name() + " {"
		if numField == 0 {
			result += "}"
			return result
		}

		result += "\n"
		names := make([]string, 0, numField)
		values := make([]interface{}, 0, numField)
		for i := 0; i < numField; i++ {
			names = append(names, val.Type().Field(i).Name)
			values = append(values, val.Field(i).Interface())
		}

		maxNameIdx := alg.MaxElemIfIdx(names, func(lhs, rhs int) bool {
			return len(names[lhs]) < len(names[rhs])
		})

		maxNameLen := len(names[maxNameIdx])
		lineFmt := fmt.Sprintf("  %%-%vv%%-%vv = %%v\n", indent, maxNameLen)
		for i := 0; i < len(names); i++ {
			result += fmt.Sprintf(lineFmt, "", names[i], impl(values[i], indent+maxNameLen+len(" = { ")))
		}

		result += fmt.Sprintf(fmt.Sprintf("%%+%vv", indent+1), "}")

		return result
	}

	return impl(obj, 0)
}
