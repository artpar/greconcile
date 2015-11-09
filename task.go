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
		Act(sourceData, targetData, compareResult, rt.Action)
		panic("1 complete")
	}
}

func Act(source, target Row, compareResult CompareResult, actionConfig ActionConfig) {
	for _, res := range compareResult.Results {
		log.Debug("Mismatch %v Vs %v", res.sourceValue, res.targetValue)
		actionsToPerform := actionConfig.Match
		if !res.Match {
			actionsToPerform = actionConfig.MisMatch
		}
		for _, task := range actionsToPerform.Tasks {
			performTask(task, res, source, target)
		}
	}
}

func performTask(task ActionItemConfig, res KeyCompareResult, source, target Row) {
	switch task.Type {
	case "sendToSlack":
		context := make(map[string]interface{})
		context["source"] = source
		context["target"] = target
		context["result"] = res
		log.Debug("Task Config - %v", task.Config)
		message := EvaluateTemplate(task.Config["message"].(string), ConvertToString(context).(map[string]interface{}))
		slackClient := slack.New(task.Config["slackKey"].(string))
		slackClient.PostMessage(task.Config["slackTo"].(string), message, slack.NewPostMessageParameters())
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
	for _, compare := range compareList {
		sourceValue := EvaluateTemplate(compare.Source, source)
		log.Debug("Source value for %v - %v", compare.Source, sourceValue)
		targetValue := EvaluateTemplate(compare.Target, target)
		log.Debug("Source value for %v - %v", compare.Target, targetValue)
		result := KeyCompareResult{
			sourceKey:   compare.Source,
			targetKey:   compare.Target,
			sourceValue: sourceValue,
			targetValue: targetValue,
		}
		log.Debug("Compare result [%v] - [%v]", result, result.Match)
		result.Match = sourceValue == targetValue
		list = append(list, result)
	}
	resultList.Results = list
	return resultList
}


