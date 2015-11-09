package main

import (
	"encoding/json"
	"errors"
	logger "github.com/op/go-logging"
	config "github.com/spf13/viper"
	flag "gopkg.in/alecthomas/kingpin.v2"
	"io/ioutil"
	"os"
	"path/filepath"
)

var (
	configFileName = flag.Flag("config", "Config file name").Short('c').Required().String()
	taskDirectoryName = flag.Flag("tasks", "Task Directory name").Short('t').Required().String()
	moduleName = "greconcile"
	log = logger.MustGetLogger(moduleName)
)

func initLog(logConfig map[string]interface{}) {

	log.Info("LogConfig: %v", logConfig)
	_, ok := logConfig["level"]
	if ok {
		switch logConfig["level"].(string) {
		case "debug":
			logger.SetLevel(logger.NOTICE, moduleName)
		case "error":
			logger.SetLevel(logger.ERROR, moduleName)
		case "info":
			logger.SetLevel(logger.INFO, moduleName)
		case "warn":
			logger.SetLevel(logger.WARNING, moduleName)
		}
	}

	f, err := logger.NewStringFormatter("%{shortfile} %{time:2006-01-02T15:04:05} %{level:.1s} %{id:04d} %{module} %{message}")
	if err != nil {
		log.Info("failed to set format: %s", err)
	}
	logger.SetFormatter(f)

	_, ok = logConfig["file"]
	if ok {
		logFileName := logConfig["file"].(string)

		logFile, err := os.OpenFile(logFileName, os.O_RDWR | os.O_APPEND | os.O_CREATE, 0660)
		backend := logger.NewLogBackend(logFile, "", 0)
		if err != nil {
			log.Error("Failed to open log file - " + logFileName)
			panic(err)
		} else {
			logger.SetBackend(backend)
		}
	} else {
		backend := logger.NewLogBackend(os.Stdout, "", 0)
		logger.SetBackend(backend)

	}

}

func init() {
	flag.Parse()
	config.SetConfigFile(*configFileName)

	err := config.ReadInConfig()
	if err != nil {
		panic("Config file not found")
	}
	logConfig := config.GetStringMap("log")
	initLog(logConfig)
}

func main() {
	log.Info("Started")
	log.Info("Tasks Directory %s", *taskDirectoryName)
	taskFiles := make([]string, 0)
	filepath.Walk(*taskDirectoryName, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			log.Info("Task file: %s\n", path)
			taskFiles = append(taskFiles, path)
		}
		return nil
	})

	// read tasks config
	taskConfigs := make([]ReconTaskConfig, len(taskFiles))
	for i, taskFile := range taskFiles {
		data, err := ioutil.ReadFile(taskFile)
		if err != nil {
			log.Panic("Failed to read task [%s] - %v", taskFile, err)
		}
		var v ReconTaskConfig
		json.Unmarshal(data, &v)
		v.FileName = taskFile
		taskConfigs[i] = v
		log.Info("Task %s config\n%v", taskFile, v)
	}

	// make actual tasks object
	tasks := make([]ReconTask, len(taskConfigs))
	for i, taskConfig := range taskConfigs {
		var err error
		task := ReconTask{}
		task.Source, err = makeDataEndPoint(taskConfig.Source)
		checkErr(err, "Failed to make source data end point for - " + taskConfig.FileName)
		task.Target, err = makeDataEndPoint(taskConfig.Target)
		checkErr(err, "Failed to make target data end point for - " + taskConfig.FileName)
		task.CompareList = taskConfig.Compare
		task.Action = taskConfig.Action
		tasks[i] = task
	}

	for _, task := range tasks {
		task.Execute()
	}
}

func checkErr(err error, message string) {
	if err != nil {
		log.Panic(message, err)
	}
}

func makeDataEndPoint(config DataEndPointConfig) (DataEndPoint, error) {
	log.Debug("Make type %s", config.Type)
	switch config.Type {
	case "mysql":
		endPoint, err := NewMysqlDataProvider(config.Config)
		if err != nil {
			return nil, err
		}
		return endPoint, nil
	case "webApi":
		endPoint, err := NewWebApi(config.Config)
		if err != nil {
			return nil, err
		}
		return endPoint, nil

	}
	return nil, errors.New("Failed to identify type of data source - " + config.Type)
}
