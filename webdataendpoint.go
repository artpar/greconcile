package main

import (
	"crypto/sha512"
	"encoding/hex"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"github.com/alecthomas/template"
	"bytes"
	"reflect"
	"encoding/json"
)

type RequestConfig struct {
	method     string
	params     map[string]string
	constructs map[string]interface{}
}

type ResponseConfig struct {
	key          string
	responseType string
}

type WebEndPoint struct {
	url      string
	request  RequestConfig
	response ResponseConfig
}

func (this WebEndPoint) Iterate(after uint64, limit int) (Result, error) {
	return Result{}, nil
}

func (this WebEndPoint) Get(row map[string]interface{}) (Row, error) {
	var response *http.Response
	var err error
	params := this.request.params
	dataMap := make(map[string]string)
	stringContext := convertToStringMap(row)
	//	constructs := this.request.constructs
	for key, val := range params {
		var value string
		switch val[0] {
		//		case '#':
		//			log.Info("Construct is function type: %s: [%s]: %v", val, val[1:], constructs[val[1:]])
		//			value = evaluateFunction(constructs[val[1:]].(map[string]interface{}), row)
		default:
			value = evaluateTemplate(val, stringContext)
		}
		dataMap[key] = value
	}
	switch strings.ToLower(this.request.method) {
	case "post":
		data := url.Values{}
		for key, val := range dataMap {
			data[key] = []string{val}
		}
		log.Debug("Post body: %v", data)
		response, err = http.PostForm(this.url, data)
		if err != nil {
			log.Error("Failed to get data from [" + this.url + "]")
			return Row{}, err
		}
	case "get":
	default:
		restOfTheUrl := "&"
		if strings.Index(this.url, "?") == -1 {
			restOfTheUrl = "?"
		}
		for key, val := range dataMap {
			restOfTheUrl = restOfTheUrl + key + "=" + val + "&"
		}
		response, err = http.Get(this.url + restOfTheUrl)
		if err != nil {
			log.Error("Failed to get data from [" + this.url + "]")
			return Row{}, err
		}
	}

	returnRow, err := processHttpResponse(response, this.response, stringContext)
	if err != nil {
		return nil, err
	}
	return returnRow, nil
}

func processHttpResponse(response *http.Response, responseConfig ResponseConfig, context map[string]interface{}) (Row, error) {
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	stringBody := string(body)
	log.Debug("String response: %s", stringBody)
	log.Debug("Response config %v", responseConfig)
	var mapValue map[string]interface{}
	json.Unmarshal(body, &mapValue)
	context["x"] = mapValue
	log.Debug("Map: %v", mapValue)
	result := evaluateTemplate(responseConfig.key, context)
	log.Debug("Response Extracted: %v", result)
	return mapValue, nil
}

func convertToStringMap(context map[string]interface{}) map[string]interface{} {
	newMap := map[string]interface{}{}
	for key, val := range context {
		var stringVal interface{}
		stringVal = val
		switch val.(type) {
		case byte:
			stringVal = string(stringVal.(byte))
		case []uint8:
			stringVal = string(stringVal.([]uint8))
		case map[string]interface{}:
			stringVal = convertToStringMap(val.(map[string]interface{}))
		}
		log.Info("Convert value[%v] %s -> %v to %v", reflect.TypeOf(val), key, val, stringVal)
		newMap[key] = stringVal
	}
	return newMap
}

func evaluateTemplate(templateString string, context map[string]interface{}) string {
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

func NewWebApi(config map[string]interface{}) (WebEndPoint, error) {
	requestConfig := config["request"].(map[string]interface{})
	we := WebEndPoint{
		url: config["url"].(string),
		request: RequestConfig{
			method:     requestConfig["method"].(string),
			constructs: requestConfig["constructs"].(map[string]interface{}),
		},
	}
	params := requestConfig["params"].(map[string]interface{})
	log.Debug("Url Params: %v", params)
	stringParams := make(map[string]string)
	for key, val := range params {
		stringParams[key] = val.(string)
	}

	responseParams := config["response"].(map[string]interface{})
	responseConfig := ResponseConfig{}
	responseConfig.key = responseParams["key"].(string)
	responseConfig.responseType = responseParams["responseType"].(string)
	we.request.params = stringParams
	we.response = responseConfig
	return we, nil
}

func Sha512(str string) string {
	log.Info("String to hash - [" + str + "]")
	h := sha512.New()
	h.Write([]byte(str))
	sha1_hash := hex.EncodeToString(h.Sum(nil))
	log.Info("Hash [" + sha1_hash + "]")
	return sha1_hash
}
