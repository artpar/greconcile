package main
import (
	"bytes"
	"text/template"
	"reflect"
	"time"
	"github.com/fatih/structs"
	"fmt"
	"strconv"
)

func Divide(a interface{}, b int) string {
	strVal := fmt.Sprintf("%v", a)
	aFloat, err := strconv.ParseFloat(strVal, 64)
	if err != nil {
		return "FailedToParseFloat[" + strVal + "]"
	}
	//	log.Error("---- ------------------Divide %v by %v -> %v", aFloat, b, fmt.Sprintf("%.2f", aFloat / float64(b)))
	return fmt.Sprintf("%.2f", aFloat / float64(b))
}

func EvaluateTemplate(templateString string, context map[string]interface{}) string {
	context["Sha512"] = Sha512
	funcMap := template.FuncMap{
		"Sha512": Sha512,
		"Divide": Divide,
	}
	tmpl, err := template.New("dummy").Funcs(funcMap).Parse(templateString)
	if err != nil {
		mainLogger.Error("Failed to parse template - [%s]\n%v", templateString, err)
		return "[TemplateFail]"
	}

	writer := &bytes.Buffer{}
	tmpl.Execute(writer, context)
	mainLogger.Debug("Executed template value [%s] -> [%s]", templateString, writer.String())
	return writer.String()
}

func ConvertToString(context interface{}) interface{} {
	kindOfContext := reflect.TypeOf(context)
	switch context.(type) {
	case Row:
		newMap := map[string]interface{}{}
		m := context.(Row)
		for key, val := range m {
			keyString := ConvertToString(key).(string)
			keyVal := ConvertToString(val)
			newMap[keyString] = keyVal
		}
		return newMap
	case map[string]interface{}:
		newMap := map[string]interface{}{}
		m := context.(map[string]interface{})
		for key, val := range m {
			keyString := ConvertToString(key).(string)
			keyVal := ConvertToString(val)
			newMap[keyString] = keyVal
		}
		return newMap

	case []uint8:
		return string(context.([]uint8))
	case uint8:
		return context
	case int64:
		return context
	case float64:
		return context
	case func(string) string:
		return "function(string)string"
	case KeyCompareResult:
		return structs.Map(context)
	case time.Time:
		return context.(time.Time).String()

	case string:
		return context
	case nil:
		return ""
	default:
		mainLogger.Info("Dont know kind of context %v", kindOfContext)
		return context
	}
}

