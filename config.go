package main


type DataEndPointConfig struct {
	Type   string
	Config map[string]interface{}
}

type ActionList struct {
	Tasks []ActionItemConfig
}

type ActionItemConfig struct {
	Type   string
	Config map[string]interface{}
}


type ActionConfig struct {
	Match    ActionList
	MisMatch ActionList
}

type LogConfig struct {
	Filename string
	Level    string
	Format   string
}

type ReconTaskConfig struct {
	Name                 string
	FileName             string
	Source               DataEndPointConfig
	Target               DataEndPointConfig
	Compare              []Compare
	Action               map[string]ActionConfig
	BigDelay, SmallDelay int
	Log                  LogConfig
	SourceIdString       string

}
