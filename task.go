package main

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
		compareResult := CompareData(sourceData, targetData, rt.CompareList)
		Act(sourceData, targetData, compareResult, rt.Action)
	}
}

func Act(source, target Row, compareResult CompareResult, actionConfig ActionConfig) {

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
	for _, compare := range compareList {
		sourceValue := EvaluateTemplate(compare.Source, source)
		targetValue := EvaluateTemplate(compare.Target, target)
		result := KeyCompareResult{
			sourceKey:   compare.Source,
			targetKey:   compare.Target,
			sourceValue: sourceValue,
			targetValue: targetValue,
		}
		result.Match = sourceValue == targetValue
		list = append(list, result)
	}
	resultList.Results = list
	return resultList
}


