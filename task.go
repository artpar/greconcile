package main
import (
	"github.com/nlopes/slack"
	"time"
	"github.com/op/go-logging"
	"strconv"
)

type Compare struct {
	Name, Source, Target string
}

type ReconTask struct {
	Name                 string
	Source               DataEndPoint
	Target               DataEndPoint
	CompareList          []Compare
	State                *TaskState
	Action               map[string]ActionConfig
	BigDelay, SmallDelay int
	log                  *logging.Logger
	SourceIdString       string
}

func (rt ReconTask) Execute() {
	rt.log.Info("Started task[%s] from id[%v]", rt.Name, rt.State.LastId)
	for {

		result, err := rt.Source.Iterate(rt.State.LastId, 1000)
		if err != nil {
			rt.log.Error("Failed to get data from source: %v", err)
			return
		}
		for _, sourceData := range result {
			rt.log.Debug("Process row: %v", sourceData)
			targetData, err := rt.Target.Get(sourceData)
			if err != nil {
				rt.log.Error("Failed to get target data for %v\n%v", sourceData, err)
				continue
			}
			rt.log.Debug("Got data from target [%v]", targetData)
			compareResult := rt.CompareData(sourceData, targetData, rt.CompareList)
			rt.Act(sourceData, targetData, compareResult, rt.Action)
			i, _ := strconv.ParseUint(EvaluateTemplate(rt.SourceIdString, sourceData), 10, 32)
			rt.State.LastId = i
			rt.log.Info("Last id task update %v - %v", rt.Name, rt.State.LastId)
			time.Sleep(time.Duration(rt.SmallDelay) * time.Millisecond)
			// panic("1 complete")
		}
		time.Sleep(time.Duration(rt.BigDelay) * time.Millisecond)
	}
}

func (rt ReconTask) Act(source, target Row, compareResult CompareResult, actionConfig map[string]ActionConfig) {
	for _, res := range compareResult.Results {
		actionsToPerform := actionConfig[res.Name].Match
		if !res.Match {
			actionsToPerform = actionConfig[res.Name].MisMatch

		}
		if len(actionsToPerform.Tasks) > 0 {
			if !res.Match {
				rt.log.Info("Mismatch %v Vs %v", res.sourceValue, res.targetValue)
			} else {
				rt.log.Info("Match %v Vs %v", res.sourceValue, res.targetValue)
			}
		} else {
			continue
		}
		context := make(map[string]interface{})
		context["source"] = source
		context["target"] = target
		context["result"] = res
		stringContext := ConvertToString(context).(map[string]interface{})


		for _, task := range actionsToPerform.Tasks {
			rt.performTask(task, stringContext)
		}
	}
}

func (rt ReconTask) performTask(task ActionItemConfig, stringContext map[string]interface{}) {
	rt.log.Debug("Task Config - %v", task.Config)
	switch task.Type {
	case "sendToSlack":
		message := EvaluateTemplate(task.Config["message"].(string), stringContext)
		slackClient := slack.New(task.Config["slackKey"].(string))
		slackClient.PostMessage(task.Config["slackTo"].(string), message, slack.NewPostMessageParameters())
	case "executeMysql":
		query := EvaluateTemplate(task.Config["query"].(string), stringContext)
		params := task.Config["params"].([]interface{})
		paramValues := make([]interface{}, 0)
		for _, param := range params {
			paramValue := EvaluateTemplate(param.(string), stringContext)
			paramValues = append(paramValues, paramValue)
		}
		endPoint, err := NewMysqlDataProvider(task.Config["mysqlConfig"].(map[string]interface{}))
		if err != nil {
			rt.log.Error("Failed to connect to mysql - %v", err)
		}
		stmt, err := endPoint.db.Prepare(query)
		if err != nil {
			rt.log.Error("Failed to prepare statement- %v", err)
		}
		defer stmt.Close()
		rt.log.Info("Executing [%s] with params [%v]", query, paramValues)
		result, err := stmt.Exec(paramValues...)
		if err != nil {
			rt.log.Error("Failed to Execute query - %v", err)
		}
		count, _ := result.RowsAffected()
		if count < 1 {
			rt.log.Info("0 rows affected, might want to check - %v", stringContext)
		}
	}
}

type CompareResult struct {
	Results []KeyCompareResult
	Match   bool
}

type KeyCompareResult struct {
	sourceKey   string
	targetKey   string
	sourceValue string
	targetValue string
	Match       bool
	Name        string
}

func (rt ReconTask) CompareData(source, target Row, compareList []Compare) CompareResult {
	resultList := CompareResult{}
	list := make([]KeyCompareResult, 0)
	rt.log.Debug("We have %d items to compare", len(compareList))
	context := make(map[string]interface{})
	context["source"] = source
	context["target"] = target
	context = ConvertToString(context).(map[string]interface{})

	for _, compare := range compareList {

		sourceValue := EvaluateTemplate(compare.Source, context)
		rt.log.Debug("Source value for %v - %v", compare.Source, sourceValue)
		targetValue := EvaluateTemplate(compare.Target, context)
		rt.log.Debug("Target  value for %v - %v", compare.Target, targetValue)
		result := KeyCompareResult{
			sourceKey:   compare.Source,
			targetKey:   compare.Target,
			sourceValue: sourceValue,
			targetValue: targetValue,
			Name:        compare.Name,
		}
		result.Match = sourceValue == targetValue
		rt.log.Debug("Compare result [%v] - [%v]", result, result.Match)
		list = append(list, result)
		if (!result.Match) {
			resultList.Match = false
		}
	}
	resultList.Results = list
	return resultList
}


