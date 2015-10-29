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

	result := rt.Source.Iterate(0, 1000)
	for _, row := range result {
		log.Debug("Process row: %v", row)
	}
}
