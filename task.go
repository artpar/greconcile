package main
import "github.com/nlopes/slack"

type Compare struct {
	Source, Target string
}

type ReconTask struct {
	Source      DataEndPoint
	Target      DataEndPoint
	CompareList []Compare
	Action      ActionConfig
}


func (rt ReconTask) Execute() {
	log.Info("Started task")

	result, err := rt.Source.Iterate(0, 1000)
	if err != nil {
		log.Error("Failed to get data from source: %v", err)
		return
	}
	for _, sourceData := range result {
		log.Debug("Process row: %v", sourceData)
		targetData, err := rt.Target.Get(sourceData)
		if err != nil {
			log.Error("Failed to get target data for %v\n%v", sourceData, err)
			continue
		}
		log.Debug("Got data from target [%v]", targetData)
		compareResult := CompareData(sourceData, targetData, rt.CompareList)
		rt.Act(sourceData, targetData, compareResult, rt.Action)
		panic("1 complete")
	}
}

func (rt ReconTask) Act(source, target Row, compareResult CompareResult, actionConfig ActionConfig) {
	for _, res := range compareResult.Results {
		actionsToPerform := actionConfig.Match
		if !res.Match {
			actionsToPerform = actionConfig.MisMatch

		}
		if len(actionsToPerform.Tasks) > 0 {
			if !res.Match {
				log.Info("Mismatch %v Vs %v", res.sourceValue, res.targetValue)
			} else {
				log.Info("Match %v Vs %v", res.sourceValue, res.targetValue)
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
	log.Debug("Task Config - %v", task.Config)
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
			log.Error("Failed to connect to mysql - %v", err)
		}
		result, err := endPoint.db.Exec(query, paramValues...)
		if err != nil {
			log.Error("Failed to Execute query - %v", err)
		}
		count, _ := result.RowsAffected()
		if count < 1 {
			log.Info("0 rows affected, might want to check - %v", stringContext)
		}
	}
}

type CompareResult struct {
	Results []KeyCompareResult
}

type KeyCompareResult struct {
	sourceKey   string
	targetKey   string
	sourceValue string
	targetValue string
	Match       bool
}

func CompareData(source, target Row, compareList []Compare) CompareResult {
	resultList := CompareResult{}
	list := make([]KeyCompareResult, 0)
	log.Debug("We have %d items to compare", len(compareList))
	context := make(map[string]interface{})
	context["source"] = source
	context["target"] = target
	context = ConvertToString(context).(map[string]interface{})

	for _, compare := range compareList {

		sourceValue := EvaluateTemplate(compare.Source, context)
		log.Debug("Source value for %v - %v", compare.Source, sourceValue)
		targetValue := EvaluateTemplate(compare.Target, context)
		log.Debug("Target  value for %v - %v", compare.Target, targetValue)
		result := KeyCompareResult{
			sourceKey:   compare.Source,
			targetKey:   compare.Target,
			sourceValue: sourceValue,
			targetValue: targetValue,
		}
		result.Match = sourceValue == targetValue
		log.Debug("Compare result [%v] - [%v]", result, result.Match)
		list = append(list, result)
	}
	resultList.Results = list
	return resultList
}


