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
	"time"
)

var (
	configFileName = flag.Flag("config", "Config file name").Short('c').Required().String()
	taskDirectoryName = flag.Flag("tasks", "Task Directory name").Short('t').Required().String()
	moduleName = "greconcile"
	mainLogger = logger.MustGetLogger(moduleName)
)

func initLog(logConfig map[string]interface{}) {

	mainLogger.Info("LogConfig: %v", logConfig)
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
		mainLogger.Info("failed to set format: %s", err)
	}
	logger.SetFormatter(f)

	_, ok = logConfig["file"]
	if ok {
		logFileName := logConfig["file"].(string)

		logFile, err := os.OpenFile(logFileName, os.O_RDWR | os.O_APPEND | os.O_CREATE, 0660)
		backend := logger.NewLogBackend(logFile, "", 0)
		if err != nil {
			mainLogger.Error("Failed to open log file - " + logFileName)
			panic(err)
		} else {
			logger.SetBackend(backend)
		}
	} else {
		backend := logger.NewLogBackend(os.Stdout, "", 0)
		logger.SetBackend(backend)
	}
}

func getLogger(name string, logConfig LogConfig) *logger.Logger {
	mainLogger.Info("Make logger of %s at file %s", name, logConfig.Filename)
	log1 := logger.MustGetLogger(name)

	var leveledBackend logger.LeveledBackend
	f, err := logger.NewStringFormatter("%{shortfile} %{time:2006-01-02T15:04:05} %{level:.1s} %{id:04d} %{module} %{message}")
	if err != nil {
		mainLogger.Info("failed to set format: %s", err)
	}
	logger.SetFormatter(f)

	if logConfig.Filename != "" {
		logFileName := logConfig.Filename

		logFile, err := os.OpenFile(logFileName, os.O_RDWR | os.O_APPEND | os.O_CREATE, 0660)
		backend := logger.NewLogBackend(logFile, "", 0)
		if err != nil {
			mainLogger.Error("Failed to open log file - " + logFileName)
			panic(err)
		} else {
			leveledBackend = logger.AddModuleLevel(backend)
		}
	} else {
		backend := logger.NewLogBackend(os.Stdout, "", 0)
		leveledBackend = logger.AddModuleLevel(backend)

	}

	switch logConfig.Level {
	case "debug":
		leveledBackend.SetLevel(logger.NOTICE, name)
	case "error":
		logger.SetLevel(logger.ERROR, name)
	case "info":
		logger.SetLevel(logger.INFO, name)
	case "warn":
		logger.SetLevel(logger.WARNING, name)
	}

	log1.SetBackend(leveledBackend)
	return log1
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
	mainLogger.Info("Started")
	mainLogger.Info("Tasks Directory %s", *taskDirectoryName)
	taskFiles := make([]string, 0)
	filepath.Walk(*taskDirectoryName, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			mainLogger.Info("Task file: %s\n", path)
			taskFiles = append(taskFiles, path)
		}
		return nil
	})

	// read tasks config
	taskConfigs := make([]ReconTaskConfig, len(taskFiles))
	for i, taskFile := range taskFiles {
		data, err := ioutil.ReadFile(taskFile)
		if err != nil {
			mainLogger.Panic("Failed to read task [%s] - %v", taskFile, err)
		}
		var v ReconTaskConfig
		json.Unmarshal(data, &v)
		v.FileName = taskFile
		taskConfigs[i] = v
		mainLogger.Info("Task %s config\n%v", taskFile, v)
	}

	// make actual tasks object
	tasks := make([]ReconTask, len(taskConfigs))
	for i, taskConfig := range taskConfigs {
		var err error
		task := ReconTask{}
		task.Name = taskConfig.FileName

		taskConfig.Log.Filename = config.GetString("log.directory") + "/" + taskConfig.Log.Filename
		task.log = getLogger(taskConfig.Name, taskConfig.Log)

		task.BigDelay = taskConfig.BigDelay
		task.SmallDelay = taskConfig.SmallDelay
		task.Source, err = makeDataEndPoint(taskConfig.Source)
		checkErr(err, "Failed to make source data end point for - " + taskConfig.FileName)
		task.Target, err = makeDataEndPoint(taskConfig.Target)
		checkErr(err, "Failed to make target data end point for - " + taskConfig.FileName)
		task.CompareList = taskConfig.Compare
		task.Action = taskConfig.Action
		task.SourceIdString = taskConfig.SourceIdString
		tasks[i] = task
	}
	var state State
	bytes, err := ioutil.ReadFile("state.json")
	if err != nil {
		mainLogger.Info("Failed to read state.json file - %v", err)
		state = State{}
		state.TaskState = make(map[string]*TaskState)
	} else {
		json.Unmarshal(bytes, &state)
		if state.TaskState == nil {
			state.TaskState = make(map[string]*TaskState)
		}
	}

	for _, task := range tasks {
		var ok bool
		task.State, ok = state.TaskState[task.Name]
		if !ok {
			s := TaskState{}
			s.LastId = 0
			state.TaskState[task.Name] = &s
			task.State = &s
		}
		go task.Execute()
	}
	for ;; {
		state.UpdatedAt = time.Now()
		//		for _, task := range tasks {
		//			state.TaskState[task.Name] = task.State
		//		}
		mainLogger.Info("Task state - %v", state.TaskState)
		stateFile, _ := json.Marshal(state)
		ioutil.WriteFile("state.json", stateFile, 0755)
		time.Sleep(1 * time.Second)
	}
}

type State struct {
	CreatedAt time.Time
	UpdatedAt time.Time
	TaskState map[string]*TaskState
}

type TaskState struct {
	LastId uint64
}

func checkErr(err error, message string) {
	if err != nil {
		mainLogger.Panic(message, err)
	}
}

func makeDataEndPoint(config DataEndPointConfig) (DataEndPoint, error) {
	//	mainLogger.Debug("Make type %s", config.Type)
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
