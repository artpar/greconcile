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
		CompareData(sourceData, targetData, rt.CompareList)
	}
}

type CompareResult struct {

}

type KeyCompareResult struct {
	sourceKey   string
	targetKey   string
	sourceValue string
	targetValue string
}

func CompareData(source, target Row, compareList []Compare) CompareResult {

	for _, compare := range compareList {
		sourceValue := EvaluateTemplate(compare.Source, source)
		targetValue := EvaluateTemplate(compare.Target, target)
		if sourceValue != targetValue {

		}
	}
}


