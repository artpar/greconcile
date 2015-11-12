package main

import (
	"crypto/sha512"
	"encoding/hex"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"encoding/json"
	"errors"
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
	stringContext := ConvertToString(row).(map[string]interface{})
	//	constructs := this.request.constructs
	for key, val := range params {
		var value string
		switch val[0] {
		//		case '#':
		//			log.Info("Construct is function type: %s: [%s]: %v", val, val[1:], constructs[val[1:]])
		//			value = evaluateFunction(constructs[val[1:]].(map[string]interface{}), row)
		default:
			value = EvaluateTemplate(val, stringContext)
		}
		dataMap[key] = value
	}
	switch strings.ToLower(this.request.method) {
	case "post":
		data := url.Values{}
		for key, val := range dataMap {
			data[key] = []string{val}
		}
		mainLogger.Debug("Post body: %v", data)
		response, err = http.PostForm(this.url, data)
		if err != nil {
			mainLogger.Error("Failed to get data from [" + this.url + "]")
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
			mainLogger.Error("Failed to get data from [" + this.url + "]")
			return Row{}, err
		}
	}

	data, err := processHttpResponse(response, this.response)
	returnRow, err := makeRowFromByte(data, this.response, stringContext)
	if err != nil {
		return nil, err
	}
	return returnRow, nil
}

func makeRowFromByte(data []byte, responseConfig ResponseConfig, context map[string]interface{}) (Row, error) {
	switch responseConfig.responseType {
	case "json" :
		var x map[string]interface{}
		err := json.Unmarshal(data, &x)
		return x, err
	}
	return nil, errors.New("Invalid type of response in config [" + responseConfig.responseType + "]")
}

func processHttpResponse(response *http.Response, responseConfig ResponseConfig) ([]byte, error) {
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	stringBody := string(body)
	mainLogger.Debug("String response: %s", stringBody)
	return body, nil
}

func NewWebApi(config map[string]interface{}) (WebEndPoint, error) {
	requestConfig := config["request"].(map[string]interface{})
	we := WebEndPoint{
		url: config["url"].(string),
		request: RequestConfig{
			method:     requestConfig["method"].(string),
		},
	}
	params := requestConfig["params"].(map[string]interface{})
	//	mainLogger.Debug("Url Params: %v", params)
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
	mainLogger.Info("String to hash - [" + str + "]")
	h := sha512.New()
	h.Write([]byte(str))
	sha1_hash := hex.EncodeToString(h.Sum(nil))
	mainLogger.Info("Hash [" + sha1_hash + "]")
	return sha1_hash
}
