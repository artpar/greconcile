package main
import (
"bytes"
"html/template"
)


func EvaluateTemplate(templateString string, context map[string]interface{}) string {
	context["Sha512"] = Sha512
	funcMap := template.FuncMap{
		"Sha512": Sha512,
	}
	tmpl, err := template.New("dummy").Funcs(funcMap).Parse(templateString)
	if err != nil {
		log.Error("Failed to parse template - [%s]\n%v", templateString, err)
		return "[TemplateFail]"
	}

	writer := &bytes.Buffer{}
	tmpl.Execute(writer, context)
	log.Debug("Executed template value [%s] -> [%s]", templateString, writer.String())
	return writer.String()
}
