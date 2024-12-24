package pgwatcher

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// pgxListener implements IListener using a pgxpool.Conn.
type pgxListener struct {
	conn   *pgxpool.Conn
	dbPool *pgxpool.Pool
	mu     sync.Mutex
}

func NewListener(dbp *pgxpool.Pool) IListener {
	return &pgxListener{dbPool: dbp}
}

// Connect acquires a connection from the pool if none is active yet.
func (l *pgxListener) Connect(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conn != nil {
		return nil // already connected
	}
	c, err := l.dbPool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	l.conn = c
	return nil
}

// Close releases the connection if active.
func (l *pgxListener) Close(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conn == nil {
		return nil
	}
	err := l.conn.Conn().Close(ctx)
	l.conn.Release()
	l.conn = nil
	return err
}

// Listen starts listening on a channel with PostgreSQL LISTEN <channel>.
func (l *pgxListener) Listen(ctx context.Context, channel string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conn == nil {
		return errors.New("not connected")
	}
	_, err := l.conn.Exec(ctx, fmt.Sprintf(`LISTEN "%s"`, channel))
	return err
}

// UnListen unsubscribes from a channel with PostgreSQL UNLISTEN <channel>.
func (l *pgxListener) UnListen(ctx context.Context, channel string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conn == nil {
		return errors.New("not connected")
	}
	_, err := l.conn.Exec(ctx, fmt.Sprintf(`UNLISTEN "%s"`, channel))
	return err
}

// WaitForNotification blocks for the next notification from PostgreSQL.
func (l *pgxListener) WaitForNotification(ctx context.Context) (*Notification, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conn == nil {
		return nil, errors.New("not connected")
	}
	n, err := l.conn.Conn().WaitForNotification(ctx)
	if err != nil {
		return nil, err
	}
	return &Notification{
		Channel: n.Channel,
		Payload: []byte(n.Payload),
	}, nil
}

// Ping checks if the connection is still alive.
func (l *pgxListener) Ping(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conn == nil {
		return errors.New("not connected")
	}
	return l.conn.Ping(ctx)
}
