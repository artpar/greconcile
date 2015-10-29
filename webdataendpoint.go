package main

import (
	"crypto/sha512"
	"encoding/hex"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

type RequestConfig struct {
	method     string
	params     map[string]string
	constructs map[string]interface{}
}

type ResponseConfig struct {
	key string
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
	constructs := this.request.constructs
	for key, val := range params {
		var value string
		switch val[0] {
		case '$':
			value = evaluateTemplate(val, row)
		case '#':
			log.Info("Construct is function type: %s: [%s]: %v", val, val[1:], constructs[val[1:]])
			value = evaluateFunction(constructs[val[1:]].(map[string]interface{}), row)
		default:
			value = val
		}
		dataMap[key] = value
	}
	switch strings.ToLower(this.request.method) {
	case "post":
		data := url.Values{}
		for key, val := range dataMap {
			data[key] = []string{val}
		}
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

	returnRow, err := processHttpResponse(response, this.response)
	if err != nil {
		return nil, err
	}
	return returnRow, nil
}

func processHttpResponse(response *http.Response, responseConfig ResponseConfig) (Row, error) {
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	stringBody := string(body)
	log.Info("String response: %s", stringBody)
	return Row{}, nil
}

func evaluateTemplate(template string, context map[string]interface{}) string {
	return "template ok"
}

func evaluateFunction(construct map[string]interface{}, context map[string]interface{}) string {
	template := construct["arg"].(string)
	templateValue := evaluateTemplate(template, context)
	return "function ok(" + templateValue + ")"
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
	we.request.params = stringParams
	return we, nil
}

func hash(str string) string {
	log.Info("String to hash - [" + str + "]")
	h := sha512.New()
	h.Write([]byte(str))
	sha1_hash := hex.EncodeToString(h.Sum(nil))
	log.Info("Hash [" + sha1_hash + "]")
	return sha1_hash
}
