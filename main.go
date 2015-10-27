package main
import (
	config "github.com/spf13/viper"
	logger "github.com/op/go-logging"
	flag "gopkg.in/alecthomas/kingpin.v2"
	"os"
	"path/filepath"
)

var (
	configFileName = flag.Flag("config", "Config file name").Short('c').Required().String()
	taskDirectoryName = flag.Flag("tasks", "Task Directory name").Short('t').Required().String()
	moduleName = "greconcile"
	log = logger.MustGetLogger(moduleName)
)

type Status int

const (
	Success = iota
	Failed
	Pending
	Initiated
)


type Row map[string]interface{}
type Result []Row


type DataInstance interface {
	GetStatus() Status
}

type DataRepository interface {
	Iterate(after uint64, limit int) []DataInstance
	GetByKey(key string, value string) DataInstance
}


func initLog(logConfig map[string]interface{}) {

	log.Info("LogConfig: %v", logConfig)
	_, ok := logConfig["level"]
	if ok {
		switch logConfig["level"].(string) {
		case "debug" :
			logger.SetLevel(logger.NOTICE, moduleName)
		case "error" :
			logger.SetLevel(logger.ERROR, moduleName)
		case "info" :
			logger.SetLevel(logger.INFO, moduleName)
		case "warn" :
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
	tasks := make([]string, 0)
	filepath.Walk(*taskDirectoryName, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			tasks = append(tasks, info.Name())
			log.Info("Task file: %s", path)
		}
		return nil
	})
}