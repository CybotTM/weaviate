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

type ConsistentViewCache struct {
	cached *viewWrapper
	lock   *sync.RWMutex

	logger     logrus.FieldLogger
	createView func() *BucketConsistentView
}

func NewConsistentViewCache(logger logrus.FieldLogger, createView func() *BucketConsistentView) *ConsistentViewCache {
	return &ConsistentViewCache{
		lock:       new(sync.RWMutex),
		logger:     logger,
		createView: createView,
	}
}

func (c *ConsistentViewCache) Get() (view *BucketConsistentView, release func()) {
	c.lock.RLock()
	if vw := c.cached; vw != nil {
		vw.inUseCounter.Add(1)
		c.lock.RUnlock()
		return vw.view, c.makeRelease(vw)
	}
	c.lock.RUnlock()

	vw := func() *viewWrapper {
		c.lock.Lock()
		defer c.lock.Unlock()

		if c.cached == nil {
			c.cached = &viewWrapper{view: c.createView()}
		}
		return c.cached
	}()
	return vw.view, c.makeRelease(vw)
}

func (c *ConsistentViewCache) Invalidate() {
	var inUseCount int32
	var vw *viewWrapper

	c.lock.Lock()
	if vw = c.cached; vw != nil {
		inUseCount = vw.inUseCounter.Load()
		c.cached = nil
	}
	c.lock.Unlock()

	if inUseCount == 0 && vw != nil {
		errors.GoWrapper(vw.view.release, c.logger)
	}
}

func (c *ConsistentViewCache) makeRelease(vw *viewWrapper) func() {
	// TODO aliszka:viewcache disable multiple releases not to mess with inusecounter
	return func() {
		c.lock.RLock()
		inUseCount := vw.inUseCounter.Add(-1)
		isCached := vw == c.cached
		c.lock.RUnlock()

		if inUseCount == 0 && !isCached {
			errors.GoWrapper(vw.view.release, c.logger)
		}
	}
}

type viewWrapper struct {
	view         *BucketConsistentView
	inUseCounter atomic.Int32
}
