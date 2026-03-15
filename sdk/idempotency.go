package sdk

import (
	"container/list"
	"sync"
)

// idempotencyCache is a fixed-size LRU cache for event ID deduplication.
// It implements constitution layer 2: SDK-level at-least-once idempotency.
type idempotencyCache struct {
	mu       sync.Mutex
	maxSize  int
	items    map[string]*list.Element
	eviction *list.List // front = most recently used, back = least recently used
}

func newIdempotencyCache(maxSize int) *idempotencyCache {
	return &idempotencyCache{
		maxSize:  maxSize,
		items:    make(map[string]*list.Element, maxSize),
		eviction: list.New(),
	}
}

// Has returns true if the event ID has already been processed.
func (c *idempotencyCache) Has(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[id]; ok {
		c.eviction.MoveToFront(el)
		return true
	}
	return false
}

// Add records an event ID as processed. If the cache is full, the least
// recently used entry is evicted to make room.
func (c *idempotencyCache) Add(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.items[id]; ok {
		return
	}

	if c.eviction.Len() >= c.maxSize {
		oldest := c.eviction.Back()
		if oldest != nil {
			c.eviction.Remove(oldest)
			delete(c.items, oldest.Value.(string))
		}
	}

	el := c.eviction.PushFront(id)
	c.items[id] = el
}
