package keepalive

import (
	"gravel/env"
	"sync"
	"time"
)

const MissedKeepAliveThreshold = 3

var DefaultInterval = time.Duration(env.GetEnvIntOrDefault("CLIENT_KEEPALIVE_FALLBACK_INTERVAL_SECONDS", 30)) * time.Second

type Tracker struct {
	defaultInterval time.Duration
	lastSeen        map[string]time.Time
	intervals       map[string]time.Duration
	mutex           sync.RWMutex
}

func NewTracker(defaultInterval time.Duration) *Tracker {
	return &Tracker{
		defaultInterval: defaultInterval,
		lastSeen:        make(map[string]time.Time),
		intervals:       make(map[string]time.Duration),
	}
}

func IntervalDuration(defaultInterval time.Duration, keepAliveIntervalMs int) time.Duration {
	if keepAliveIntervalMs <= 0 {
		return defaultInterval
	}

	return time.Duration(keepAliveIntervalMs) * time.Millisecond
}

func (tracker *Tracker) Update(clientID string, keepAliveIntervalMs int) {
	tracker.mutex.Lock()
	tracker.lastSeen[clientID] = time.Now()
	tracker.intervals[clientID] = IntervalDuration(tracker.defaultInterval, keepAliveIntervalMs)
	tracker.mutex.Unlock()
}

func (tracker *Tracker) Remove(clientID string) {
	tracker.mutex.Lock()
	delete(tracker.lastSeen, clientID)
	delete(tracker.intervals, clientID)
	tracker.mutex.Unlock()
}

func (tracker *Tracker) ExpiredClients(now time.Time) []string {
	tracker.mutex.RLock()
	defer tracker.mutex.RUnlock()

	expiredClientIDs := make([]string, 0)
	for clientID, lastSeen := range tracker.lastSeen {
		interval := tracker.intervals[clientID]
		if interval <= 0 {
			interval = tracker.defaultInterval
		}

		if now.Sub(lastSeen) > interval*MissedKeepAliveThreshold {
			expiredClientIDs = append(expiredClientIDs, clientID)
		}
	}

	return expiredClientIDs
}

func (tracker *Tracker) IntervalFor(clientID string) time.Duration {
	tracker.mutex.RLock()
	defer tracker.mutex.RUnlock()

	interval := tracker.intervals[clientID]
	if interval <= 0 {
		return tracker.defaultInterval
	}

	return interval
}
