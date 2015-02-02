package gocassa

import (
	"time"
)

// Options allows specification of (optional, hah) query parameters.
type Options struct {
	// TTL specifies a duration over which data is valid. It will be truncated to second precision upon statement
	// execution.
	TTL time.Duration
}
