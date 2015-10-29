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
		rt.Target.Get(sourceData)
	}
}
