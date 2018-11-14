package dsmigration

// LoggerInterface defines the logger interface
// this matches the std go log package
type LoggerInterface interface {
	Printf(string, ...interface{})
}

// Logger is the logger used by the algorithm
var Logger LoggerInterface = &nullLogger{}

type nullLogger struct{}

func (*nullLogger) Printf(string, ...interface{}) {}
