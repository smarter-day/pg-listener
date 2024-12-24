package listener_test

import (
	"context"
	"errors"
	"listener"
	"sync"
	"testing"
	"time"

	"github.com/matryer/is"
)

// mockListener simulates a DB listener in-memory.
type mockListener struct {
	mu             sync.Mutex
	connected      bool
	channels       map[string]bool
	notificationsC chan *listener.Notification
}

func newMockListener() *mockListener {
	return &mockListener{
		channels:       make(map[string]bool),
		notificationsC: make(chan *listener.Notification, 100),
	}
}

func (m *mockListener) Connect(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connected = true
	return nil
}

func (m *mockListener) Close(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connected = false
	close(m.notificationsC) // no more notifications
	return nil
}

func (m *mockListener) Listen(ctx context.Context, channel string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.connected {
		return errors.New("not connected")
	}
	m.channels[channel] = true
	return nil
}

func (m *mockListener) UnListen(ctx context.Context, channel string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.channels, channel)
	return nil
}

func (m *mockListener) WaitForNotification(ctx context.Context) (*listener.Notification, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case ntf, ok := <-m.notificationsC:
		if !ok {
			return nil, errors.New("mockListener closed")
		}
		return ntf, nil
	}
}

func (m *mockListener) Ping(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.connected {
		return errors.New("not connected")
	}
	return nil
}

// pushNotification simulates server pushing a notification.
func (m *mockListener) pushNotification(ntf *listener.Notification) {
	m.notificationsC <- ntf
}

// ---------------------------
// TESTS
// ---------------------------

func TestNotifierWithSingleChannel(t *testing.T) {
	is := is.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	mockL := newMockListener()
	n := listener.NewNotifier(mockL)

	// Start the Notifier (it will Connect automatically in the runLoop).
	err := n.Run(ctx)
	is.NoErr(err)

	// Listen to a single channel, "foo".
	sub := n.Listen("foo")

	// We'll push 3 messages in correct order.
	want := []string{"hello1", "hello2", "hello3"}

	go func() {
		time.Sleep(200 * time.Millisecond) // Let it set up
		for _, w := range want {
			mockL.pushNotification(&listener.Notification{
				Channel: "foo",
				Payload: []byte(w),
			})
		}
	}()

	var got []string
	for i := 0; i < len(want); i++ {
		select {
		case msg := <-sub.NotificationChannel():
			got = append(got, string(msg))
		case <-ctx.Done():
			t.Fatal("timeout waiting for messages")
		}
	}

	// Must preserve correct order, because we have one dedicated worker for channel "foo".
	is.Equal(got, want)

	// Cleanup
	sub.UnListen(ctx)
	err = n.Stop(ctx)
	is.NoErr(err)
}

func TestNotifierWithMultipleChannels(t *testing.T) {
	is := is.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	mockL := newMockListener()
	n := listener.NewNotifier(mockL)
	err := n.Run(ctx)
	is.NoErr(err)

	// Listen to two different channels
	subFoo := n.Listen("foo")
	subBar := n.Listen("bar")

	// We want to ensure concurrency across channels, so we push them at the same time.
	go func() {
		time.Sleep(200 * time.Millisecond)
		mockL.pushNotification(&listener.Notification{Channel: "foo", Payload: []byte("foo1")})
		mockL.pushNotification(&listener.Notification{Channel: "bar", Payload: []byte("bar1")})
		mockL.pushNotification(&listener.Notification{Channel: "foo", Payload: []byte("foo2")})
		mockL.pushNotification(&listener.Notification{Channel: "bar", Payload: []byte("bar2")})
	}()

	var gotFoo, gotBar []string
	for i := 0; i < 2; i++ {
		select {
		case msg := <-subFoo.NotificationChannel():
			gotFoo = append(gotFoo, string(msg))
		case <-ctx.Done():
			t.Fatal("timeout for foo messages")
		}
	}
	for i := 0; i < 2; i++ {
		select {
		case msg := <-subBar.NotificationChannel():
			gotBar = append(gotBar, string(msg))
		case <-ctx.Done():
			t.Fatal("timeout for bar messages")
		}
	}

	// Even if "foo1" arrives after "bar1" in real time,
	// the order within "foo" must remain "foo1" then "foo2".
	is.Equal(gotFoo, []string{"foo1", "foo2"})
	is.Equal(gotBar, []string{"bar1", "bar2"})

	// Cleanup
	subFoo.UnListen(ctx)
	subBar.UnListen(ctx)
	err = n.Stop(ctx)
	is.NoErr(err)
}
