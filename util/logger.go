package util

type LogLevel uint8

const (
	DebugLvl = iota
	InfoLvl
	WarnLvl
	ErrorLvl
	CriticalLvl
)

// Log level string representations (used in configuration files)
const (
	DebugStr    = "debug"
	InfoStr     = "info"
	WarnStr     = "warn"
	ErrorStr    = "error"
	CriticalStr = "critical"
)

var levelToStringMap = map[LogLevel]string{
	DebugLvl:    DebugStr,
	InfoLvl:     InfoStr,
	WarnLvl:     WarnStr,
	ErrorLvl:    ErrorStr,
	CriticalLvl: CriticalStr,
}

func (l LogLevel) String() string {
	levelStr, ok := levelToStringMap[l]
	if ok {
		return levelStr
	}

	return ""
}

type LoggerFuncHandler func(level LogLevel, message string)
