package pgwatcher

import "context"

// Notification holds data about a single notification.
type Notification struct {
	Channel   string `json:"channel"`
	Payload   []byte `json:"payload"`
	Operation string `json:"operation"`
	ID        string `json:"id"`
	Table     string `json:"table"`
	Timestamp string `json:"timestamp"`
}

// IListener abstracts low-level LISTEN/NOTIFY operations (Connect, Listen, etc.).
type IListener interface {
	Connect(ctx context.Context) error
	Close(ctx context.Context) error
	Listen(ctx context.Context, channel string) error
	UnListen(ctx context.Context, channel string) error
	WaitForNotification(ctx context.Context) (*Notification, error)
	Ping(ctx context.Context) error
}

// ISubscription represents a subscription to a channel.
type ISubscription interface {
	NotificationChannel() <-chan []byte
	EstablishedChannel() <-chan struct{}
	UnListen(ctx context.Context)
}

// INotifier manages multiple subscriptions and dispatches notifications.
type INotifier interface {
	Listen(channel string) ISubscription
	Run(ctx context.Context) error
	Stop(ctx context.Context) error
}
