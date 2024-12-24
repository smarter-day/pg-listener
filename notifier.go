package listener

import (
	"context"
	"sync"
	"time"
)

// notifier implements INotifier.
// Goal: concurrency across different channels but guaranteed FIFO within each channel.
type notifier struct {
	mu            sync.RWMutex
	listener      IListener
	subscriptions map[string][]*subscription

	// For tracking which channels exist.
	channels map[string]struct{}

	// workers holds per-channel queues to preserve ordering within that channel.
	workers map[string]chan []byte

	cancelFunc context.CancelFunc
	running    bool
}

type subscription struct {
	channel    string
	listenChan chan []byte
	notifier   *notifier
}

func (s *subscription) NotificationChannel() <-chan []byte {
	return s.listenChan
}
func (s *subscription) EstablishedChannel() <-chan struct{} {
	// Not used in this example; always returns a new empty channel
	return make(chan struct{})
}
func (s *subscription) UnListen(ctx context.Context) {
	s.notifier.unListen(ctx, s)
}

// NewNotifier returns a new INotifier using the provided IListener.
func NewNotifier(listener IListener) INotifier {
	return &notifier{
		listener:      listener,
		subscriptions: make(map[string][]*subscription),
		channels:      make(map[string]struct{}),
		workers:       make(map[string]chan []byte),
	}
}

// Listen registers a subscription for the specified channel.
func (n *notifier) Listen(channel string) ISubscription {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.channels[channel] = struct{}{}

	sub := &subscription{
		channel:    channel,
		listenChan: make(chan []byte, 64),
		notifier:   n,
	}
	n.subscriptions[channel] = append(n.subscriptions[channel], sub)
	return sub
}

// Run starts listening for notifications in the background.
func (n *notifier) Run(ctx context.Context) error {
	n.mu.Lock()
	if n.running {
		n.mu.Unlock()
		return nil
	}
	n.running = true
	runCtx, cancel := context.WithCancel(ctx)
	n.cancelFunc = cancel
	n.mu.Unlock()

	go n.runLoop(runCtx)
	return nil
}

// Stop halts the notifier and closes the listener.
func (n *notifier) Stop(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if !n.running {
		return nil
	}
	n.running = false
	if n.cancelFunc != nil {
		n.cancelFunc()
	}
	return n.listener.Close(ctx)
}

// runLoop is the main background loop. Reconnect on errors with backoff.
func (n *notifier) runLoop(ctx context.Context) {
	defer func() {
		n.mu.Lock()
		n.running = false
		n.mu.Unlock()
	}()

	backoff := time.Second
	for {
		err := n.connectAndListenAll(ctx)
		if err != nil {
			// log error if you want
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
				if backoff < 30*time.Second {
					backoff *= 2
				}
				continue
			}
		}
		backoff = time.Second

		err = n.pollNotifications(ctx)
		if err != nil && ctx.Err() == nil {
			// log error if you want
			_ = n.listener.Close(ctx)
			continue
		}
		return
	}
}

// connectAndListenAll ensures the underlying listener is connected, then listens on all known channels.
func (n *notifier) connectAndListenAll(ctx context.Context) error {
	if err := n.listener.Connect(ctx); err != nil {
		return err
	}
	n.mu.RLock()
	defer n.mu.RUnlock()

	for ch := range n.channels {
		if err := n.listener.Listen(ctx, ch); err != nil {
			return err
		}
	}
	return nil
}

// pollNotifications receives notifications and distributes them to channel-specific worker queues.
func (n *notifier) pollNotifications(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		notification, err := n.listener.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		channel := notification.Channel
		payload := notification.Payload

		// Create or use existing worker queue for this channel
		n.mu.RLock()
		queue, found := n.workers[channel]
		n.mu.RUnlock()

		if !found {
			n.mu.Lock()
			// double-check again in case another goroutine just created it
			queue, found = n.workers[channel]
			if !found {
				queue = make(chan []byte, 10_000)
				n.workers[channel] = queue
				go n.channelWorker(ctx, channel, queue)
			}
			n.mu.Unlock()
		}

		// Push payload onto that channel's queue
		select {
		case queue <- payload:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// channelWorker is a dedicated goroutine per channel that ensures FIFO dispatch.
func (n *notifier) channelWorker(ctx context.Context, channel string, queue <-chan []byte) {
	for {
		select {
		case payload, ok := <-queue:
			if !ok {
				return
			}
			// For each item in the channel queue, deliver in order to each subscription.
			n.mu.RLock()
			subs := n.subscriptions[channel]
			n.mu.RUnlock()

			for _, sub := range subs {
				select {
				case sub.listenChan <- payload:
				case <-ctx.Done():
					return
				case <-time.After(15 * time.Second):
					// optional ping if needed
					_ = n.listener.Ping(ctx) // ignore error
				}
			}

		case <-ctx.Done():
			return
		}
	}
}

// unListen removes a single subscription and, if it was the last sub in that channel, unsubscribes entirely.
func (n *notifier) unListen(ctx context.Context, sub *subscription) {
	n.mu.Lock()
	defer n.mu.Unlock()

	subs := n.subscriptions[sub.channel]
	filtered := make([]*subscription, 0, len(subs))
	for _, s := range subs {
		if s != sub {
			filtered = append(filtered, s)
		}
	}
	n.subscriptions[sub.channel] = filtered

	if len(filtered) == 0 {
		// close out the entire channel
		delete(n.subscriptions, sub.channel)
		delete(n.channels, sub.channel)

		// close worker queue if present
		if q, ok := n.workers[sub.channel]; ok {
			close(q)
			delete(n.workers, sub.channel)
		}
		_ = n.listener.UnListen(ctx, sub.channel)
	}
}
