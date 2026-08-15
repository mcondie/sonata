package store

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync"
	"time"
)

// idState serializes ID generation so IDs are strictly monotonic even within
// one millisecond — `ORDER BY id` is the queue ordering, so ties are not
// acceptable. This is RFC 9562's "monotonic random" method: rand_a is a
// counter seeded per millisecond, incremented for same-millisecond IDs.
var idState struct {
	sync.Mutex
	ms  int64  // last timestamp used
	seq uint16 // 12-bit rand_a counter
}

// NewID returns a UUIDv7: 48-bit unix-millisecond timestamp, then a
// monotonic counter, then random bits. Time-ordered, so lexicographic order
// is creation order.
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[8:]); err != nil {
		// crypto/rand failing means the platform is broken beyond use.
		panic(fmt.Sprintf("store: read random: %v", err))
	}

	now := time.Now().UnixMilli()

	idState.Lock()
	if now <= idState.ms {
		// Same or regressed millisecond: keep the clock we already used and
		// count within it. On counter overflow, borrow the next millisecond —
		// still monotonic, and the wall clock catches up immediately.
		idState.seq++
		if idState.seq > 0x0fff {
			idState.ms++
			idState.seq = 0
		}
	} else {
		idState.ms = now
		// Seed the counter with entropy but keep the top bit clear, leaving
		// at least 2048 increments of headroom before overflow.
		idState.seq = uint16(b[8])<<3 | uint16(b[9])>>5 // 11 random bits
	}
	ms, seq := idState.ms, idState.seq
	idState.Unlock()

	binary.BigEndian.PutUint64(b[:8], uint64(ms)<<16) // b[0:6] = timestamp
	b[6] = 0x70 | byte(seq>>8)                        // version 7 + counter high
	b[7] = byte(seq)                                  // counter low
	b[8] = (b[8] & 0x3f) | 0x80                       // variant 10

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// nowUTC returns the current time formatted the way the store persists
// timestamps.
func nowUTC() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}
