package logger

import (
	"io"
	"os"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

type Logger struct {
	logger *log.Logger
	entry  *logrus.Entry
}

type Config struct {
	formatter   log.Formatter
	out         io.Writer
	level       log.Level
	fixedFields log.Fields
}

// Default returns a default logger
func Default(hooks ...log.Hook) *Logger {

	logger := &Logger{
		logger: logrus.New(),
	}

	logger.entry = logrus.NewEntry(logger.logger)

	logger.logger.SetFormatter(&log.JSONFormatter{})
	logger.logger.SetOutput(os.Stdout)
	logger.logger.SetLevel(log.InfoLevel)

	for _, hook := range hooks {
		logger.logger.AddHook(hook)
	}
	return logger
}

// NewLogger returns a logger with configuration
func NewLogger(cfg *Config, hooks ...log.Hook) *Logger {
	logger := &Logger{
		logger: log.New(),
	}
	logger.entry = logrus.NewEntry(logger.logger)
	if cfg != nil {
		logger.logger.SetFormatter(cfg.formatter)
		logger.logger.SetOutput(cfg.out)
		logger.logger.SetLevel(cfg.level)
		logger.entry = logger.entry.WithFields(cfg.fixedFields)
	}

	for _, hook := range hooks {
		logger.logger.AddHook(hook)
	}

	return logger
}

// AddFixedFields adds fixed fields to logger, after that, all log with contains these fields
func (l *Logger) AddFixedFields(fields log.Fields) {
	l.entry = l.entry.WithFields(fields)
}

// WithFields add new fields to current log, it wouldn't appear the late log
func (l *Logger) WithFields(fields log.Fields) *Logger {
	var newL = &Logger{
		logger: l.logger,
		entry:  l.entry.WithFields(fields),
	}
	return newL
}

func (l *Logger) Info(args ...interface{}) {
	l.entry.Info(args...)
}

func (l *Logger) Infoln(args ...interface{}) {
	l.entry.Infoln(args...)
}

func (l *Logger) Infof(format string, args ...interface{}) {
	l.entry.Infof(format, args...)
}

func (l *Logger) Debug(args ...interface{}) {
	l.entry.Debug(args...)
}

func (l *Logger) Debugln(args ...interface{}) {
	l.entry.Debugln(args...)
}

func (l *Logger) Debugf(format string, args ...interface{}) {
	l.entry.Debugf(format, args...)
}

func (l *Logger) Warn(args ...interface{}) {
	l.entry.Warn(args...)
}

func (l *Logger) Warnln(args ...interface{}) {
	l.entry.Warnln(args...)
}

func (l *Logger) Warnf(format string, args ...interface{}) {
	l.entry.Warnf(format, args...)
}

func (l *Logger) Error(args ...interface{}) {
	l.entry.Error(args...)
}

func (l *Logger) Errorln(args ...interface{}) {
	l.entry.Errorln(args...)
}

func (l *Logger) Errorf(format string, args ...interface{}) {
	l.entry.Errorf(format, args...)
}

func (l *Logger) Fatal(args ...interface{}) {
	l.entry.Fatal(args...)
}

func (l *Logger) Fatalln(args ...interface{}) {
	l.entry.Fatalln(args...)
}

func (l *Logger) Fatalf(format string, args ...interface{}) {
	l.entry.Fatalf(format, args...)
}

func (l *Logger) Panic(args ...interface{}) {
	l.entry.Panic(args...)
}

func (l *Logger) Panicln(args ...interface{}) {
	l.entry.Panicln(args...)
}

func (l *Logger) Panicf(format string, args ...interface{}) {
	l.entry.Panicf(format, args...)
}
