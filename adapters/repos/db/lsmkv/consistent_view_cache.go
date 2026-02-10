//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"sync"
	"sync/atomic"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/errors"
)

type ConsistentViewCache interface {
	Get() *BucketConsistentView
	Invalidate()
}

// ----------------------------------------------------------------------------

type ConsistentViewCacheNoop struct {
	createConsistentView func() *BucketConsistentView
}

func NewConsistentViewCacheNoop(createConsistentView func() *BucketConsistentView) *ConsistentViewCacheNoop {
	return &ConsistentViewCacheNoop{
		createConsistentView: createConsistentView,
	}
}

func (c *ConsistentViewCacheNoop) Get() *BucketConsistentView {
	return c.createConsistentView()
}

func (c *ConsistentViewCacheNoop) Invalidate() {}

// ----------------------------------------------------------------------------

type ConsistentViewCacheDefault struct {
	cached       *refsView
	lock         *sync.RWMutex
	invalidateCh chan struct{}

	logger               logrus.FieldLogger
	createConsistentView func() *BucketConsistentView
}

func NewConsistentViewCache(logger logrus.FieldLogger, createConsistentView func() *BucketConsistentView) *ConsistentViewCacheDefault {
	return &ConsistentViewCacheDefault{
		lock:                 new(sync.RWMutex),
		invalidateCh:         make(chan struct{}, 1),
		logger:               logger,
		createConsistentView: createConsistentView,
	}
}

func (c *ConsistentViewCacheDefault) Get() *BucketConsistentView {
	select {
	case <-c.invalidateCh:
		var prevRefsCount int32
		var prevRefsView *refsView
		var view *BucketConsistentView

		func() {
			c.lock.Lock()
			defer c.lock.Unlock()

			if prevRefsView = c.cached; prevRefsView != nil {
				prevRefsCount = prevRefsView.refsCounter.Load()
			}

			c.cached = c.newRefsView()
			c.cached.refsCounter.Add(1)
			view = c.cached.view
		}()

		if prevRefsCount == 0 && prevRefsView != nil {
			errors.GoWrapper(prevRefsView.origRelease, c.logger)
		}
		return view

	default:
		c.lock.RLock()
		if refsView := c.cached; refsView != nil {
			defer c.lock.RUnlock()
			refsView.refsCounter.Add(1)
			return refsView.view
		}
		c.lock.RUnlock()

		c.lock.Lock()
		defer c.lock.Unlock()
		if c.cached == nil {
			c.cached = c.newRefsView()
		}
		c.cached.refsCounter.Add(1)
		return c.cached.view
	}
}

// invalidate asynchronously either here or in Get call (whatever comes first)
func (c *ConsistentViewCacheDefault) Invalidate() {
	select {
	case c.invalidateCh <- struct{}{}:
	default:
		// nothing to do, already marked
		return
	}

	errors.GoWrapper(func() {
		select {
		case <-c.invalidateCh:
		default:
			// nothing to do, already processed
			return
		}

		var refsCount int32
		var refsView *refsView

		c.lock.Lock()
		if refsView = c.cached; refsView != nil {
			refsCount = refsView.refsCounter.Load()
			c.cached = nil
		}
		c.lock.Unlock()

		if refsCount == 0 && refsView != nil {
			refsView.origRelease()
		}
	}, c.logger)
}

func (c *ConsistentViewCacheDefault) newRefsView() *refsView {
	consistentView := c.createConsistentView()
	refsView := &refsView{
		view:        consistentView,
		origRelease: consistentView.ReleaseView,
	}

	// TODO aliszka:cachedview ensure release callable once
	consistentView.release = func() {
		c.lock.RLock()
		refsCount := refsView.refsCounter.Add(-1)
		isCached := refsView == c.cached
		c.lock.RUnlock()

		if refsCount == 0 && !isCached {
			refsView.origRelease()
		}
	}
	return refsView
}

// ----------------------------------------------------------------------------

type refsView struct {
	view        *BucketConsistentView
	origRelease func()
	refsCounter atomic.Int32
}
