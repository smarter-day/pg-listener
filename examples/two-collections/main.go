package main

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/tracelog"
	"github.com/smarter-day/pgwatcher"
	"log"
	"os"
	"time"
)

// CustomLogger implements pgx tracelog.Logger
type CustomLogger struct{}

// Log logs pgxpool events with standard log package
func (l *CustomLogger) Log(ctx context.Context, level tracelog.LogLevel, msg string, data map[string]interface{}) {
	log.Printf("[%s] %s - %v", level, msg, data)
}

func main() {
	// Initialize PostgreSQL Pool
	connStr := os.Getenv("DATABASE_DSN")
	if connStr == "" {
		log.Fatal("DATABASE_DSN not set")
	}

	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}

	// Enable logging with pgx tracelog
	config.ConnConfig.Tracer = &tracelog.TraceLog{
		Logger:   &CustomLogger{},
		LogLevel: tracelog.LogLevelDebug,
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		log.Fatalf("Failed to create pool: %v", err)
	}

	defer pool.Close()

	// Initialize IListener and INotifier
	l := pgwatcher.NewListener(pool)
	n := pgwatcher.NewNotifier(l)

	// Run INotifier with context cancellation
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	// Subscribe to dynamic topics
	entitySub := n.Listen("entities_changes")
	projectSub := n.Listen("projects_changes")

	go func() {
		for msg := range entitySub.NotificationChannel() {
			log.Printf("[Entity Notification] %s\n", msg)
		}
	}()

	go func() {
		for msg := range projectSub.NotificationChannel() {
			log.Printf("[Project Notification] %s\n", msg)
		}
	}()

	// Run INotifier
	if err := n.Run(ctx); err != nil {
		log.Fatalf("INotifier run failed: %v", err)
	}

	select {}
}
