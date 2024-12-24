# PG Listener

A lightweight Go library for listening to PostgreSQL notifications in parallel across channels while preserving in-order delivery within each channel. It reconnects on errors, ensuring fault tolerance and redundancy.

## Key Features

- Concurrency across multiple channels using per-channel worker goroutines.
- FIFO order for notifications within the same channel.
- Fault-tolerant: automatically reconnects and re-subscribes on transient errors.
- Easily mockable for testing (no real DB needed).


## Why Use It?

- **Problem:** Standard PG notifications aren’t guaranteed to come in consistent order if processed concurrently.
- **Solution:** This library spawns one worker per channel, giving concurrency across different channels but strict in-order handling within each channel.
- **Fault Tolerance:** Reconnects automatically upon errors, re-listens to all channels, and resumes operations seamlessly.

## Quick Start

1. Implement or use an existing `IListener` for PostgreSQL, e.g. using pgxpool.
2. Create the `INotifier` with `NewNotifier()`.
3. Run the notifier in a background context.
4. Listen to channels by calling `Listen(channelName)`.
5. Receive messages from the subscription’s `NotificationChannel()`.

### Example

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/smarter-day/pg-listener"
)

func main() {
    // 1. Setup a pgxpool
    dbpool, err := pgxpool.New(context.Background(), "postgres://user:password@localhost:5432/dbname")
    if err != nil {
        panic(err)
    }
    defer dbpool.Close()

    // 2. Create an IListener
    l := listener.NewListener(dbpool)

    // 3. Create and run the notifier
    n := listener.NewNotifier(l)
    ctx, cancel := context.WithCancel(context.Background())
    if err := n.Run(ctx); err != nil {
        panic(err)
    }

    // 4. Listen to a channel
    sub := n.Listen("my_channel")

    // 5. Launch a goroutine to handle incoming notifications
    go func() {
        for msg := range sub.NotificationChannel() {
            fmt.Printf("Received: %s\n", string(msg))
        }
    }()

    // Let the program run a bit
    time.Sleep(10 * time.Second)

    // Clean up
    sub.UnListen(ctx)
    _ = n.Stop(ctx)
    cancel()
}
```

## Prepare postgres

The `notify_table_change` function is a trigger function written in PL/pgSQL. It sends a notification using `pg_notify` whenever an INSERT, UPDATE, or DELETE operation occurs on a table. The notification includes a JSON payload with details about the operation, the affected row's ID, the table name, and a timestamp.


```sql
-- Create Notification Function
CREATE OR REPLACE FUNCTION notify_table_change()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify(
        TG_TABLE_NAME || '_changes',
        json_build_object(
            'operation', TG_OP,
            'id', NEW.id,
            'table', TG_TABLE_NAME,
            'timestamp', now()
        )::text
    );
RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Add Trigger to Entities Table
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        JOIN pg_class ON pg_class.oid = pg_trigger.tgrelid
        WHERE pg_class.relname = 'entities' AND pg_trigger.tgname = 'entity_changes_trigger'
    ) THEN
CREATE TRIGGER entity_changes_trigger
    AFTER INSERT OR UPDATE OR DELETE ON entities
    FOR EACH ROW EXECUTE FUNCTION notify_table_change();
END IF;
END $$;
```

## License

[MIT License](./LICENSE)