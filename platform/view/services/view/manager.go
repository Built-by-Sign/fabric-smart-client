/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package view

import (
	"context"
	"reflect"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap/zapcore"

	"github.com/hyperledger-labs/fabric-smart-client/pkg/utils/errors"
	"github.com/hyperledger-labs/fabric-smart-client/platform/common/services/logging"
	"github.com/hyperledger-labs/fabric-smart-client/platform/view/services"
	"github.com/hyperledger-labs/fabric-smart-client/platform/view/services/tracing"
	"github.com/hyperledger-labs/fabric-smart-client/platform/view/view"
)

const (
	SuccessLabel       tracing.LabelName = "success"
	ViewLabel          tracing.LabelName = "view"
	InitiatorViewLabel tracing.LabelName = "initiator_view"
)

var logger = logging.MustGetLogger()

// DisposableContext extends view.Context with additional functions for lifecycle management.
//
//go:generate counterfeiter -o mock/disposable_context.go -fake-name DisposableContext . DisposableContext
type DisposableContext interface {
	view.Context
	// Dispose releases all resources held by the context.
	Dispose()
	// PutSessionByID registers a session with the given ID and party in the context.
	PutSessionByID(viewID string, party view.Identity, session view.Session) error
}

// ContextFactory defines the interface for creating new view contexts.
//
//go:generate counterfeiter -o mock/context_factory.go -fake-name ContextFactory . ContextFactory
type ContextFactory interface {
	// NewForInitiator returns a new ParentContext for an initiator view.
	NewForInitiator(
		ctx context.Context,
		contextID string,
		id view.Identity,
		view view.View,
	) (ParentContext, error)

	// NewForResponder returns a new ParentContext for a responder view.
	NewForResponder(
		ctx context.Context,
		contextID string,
		me view.Identity,
		session view.Session,
		party view.Identity,
	) (ParentContext, error)
}

// numContextShards is the number of independently-locked shards backing the
// context store. Responder context create/delete is on the hot path; a single
// global lock convoys under high concurrency, so contexts are sharded by ID.
const numContextShards = 256

// contextShard is one independently-locked partition of the context store.
type contextShard struct {
	mu sync.RWMutex
	m  map[string]DisposableContext
}

// shardedContexts maps contextID -> context across numContextShards partitions,
// each with its own lock. count tracks the total live contexts for the metric.
type shardedContexts struct {
	shards [numContextShards]contextShard
	count  atomic.Int64
}

func newShardedContexts() *shardedContexts {
	s := &shardedContexts{}
	for i := range s.shards {
		s.shards[i].m = make(map[string]DisposableContext)
	}
	return s
}

// shard returns the partition holding the given contextID (FNV-1a, no alloc).
func (s *shardedContexts) shard(contextID string) *contextShard {
	const offset32 = 2166136261
	const prime32 = 16777619
	h := uint32(offset32)
	for i := 0; i < len(contextID); i++ {
		h ^= uint32(contextID[i])
		h *= prime32
	}
	return &s.shards[h%numContextShards]
}

func (s *shardedContexts) len() int64 { return s.count.Load() }

// Manager is responsible for managing view contexts and protocols.
type Manager struct {
	contextFactory   ContextFactory
	identityProvider IdentityProvider
	registry         *Registry
	metrics          *Metrics
	runner           Runner

	contexts *shardedContexts
}

// NewManager returns a new instance of the view manager.
func NewManager(
	identityProvider IdentityProvider,
	registry *Registry,
	metrics *Metrics,
	contextFactory ContextFactory,
	runner Runner,
) *Manager {
	return &Manager{
		identityProvider: identityProvider,

		contexts: newShardedContexts(),
		registry: registry,

		metrics:        metrics,
		contextFactory: contextFactory,

		runner: runner,
	}
}

// GetManager returns an instance of *Manager, if available, an error otherwise.
func GetManager(sp services.Provider) (*Manager, error) {
	s, err := sp.GetService(reflect.TypeOf((*Manager)(nil)))
	if err != nil {
		return nil, err
	}
	return s.(*Manager), nil
}

// RegisterFactory registers a view factory for the given ID.
func (cm *Manager) RegisterFactory(id string, factory Factory) error {
	return cm.registry.RegisterFactory(id, factory)
}

// NewView returns a new view instance for the given ID and input.
func (cm *Manager) NewView(id string, in []byte) (f view.View, err error) {
	return cm.registry.NewView(id, in)
}

// RegisterResponder registers a responder view for the given initiator view.
func (cm *Manager) RegisterResponder(responder view.View, initiatedBy any) error {
	return cm.registry.RegisterResponder(responder, initiatedBy)
}

// RegisterResponderWithIdentity registers a responder view for the given initiator view and responder identity.
func (cm *Manager) RegisterResponderWithIdentity(responder view.View, id view.Identity, initiatedBy any) error {
	return cm.registry.RegisterResponderWithIdentity(responder, id, initiatedBy)
}

// GetResponder returns the responder view for the given initiator view.
func (cm *Manager) GetResponder(initiatedBy any) (view.View, error) {
	return cm.registry.GetResponder(initiatedBy)
}

// Initiate initiates a protocol for the given view ID.
func (cm *Manager) Initiate(ctx context.Context, id string) (any, error) {
	v, err := cm.registry.GetView(id)
	if err != nil {
		return nil, err
	}

	return cm.InitiateViewWithIdentity(ctx, v, cm.identityProvider.DefaultIdentity())
}

// InitiateView initiates a protocol for the given view.
func (cm *Manager) InitiateView(ctx context.Context, view view.View) (any, error) {
	return cm.InitiateViewWithIdentity(ctx, view, cm.identityProvider.DefaultIdentity())
}

// InitiateViewWithIdentity initiates a protocol for the given view and initiator identity.
func (cm *Manager) InitiateViewWithIdentity(ctx context.Context, view view.View, id view.Identity) (any, error) {
	if ctx == nil {
		panic("context is nil")
	}
	ctx = trace.ContextWithSpanContext(ctx, trace.SpanContextFromContext(ctx))
	c, err := cm.newChildContextForInitiator(ctx, view, id, "")
	if err != nil {
		return nil, err
	}
	defer cm.DeleteContext(c.ID())

	if logger.IsEnabledFor(zapcore.DebugLevel) {
		logger.DebugfContext(ctx, "[%s] InitiateView [view:%s], [ContextID:%s], from [%s]", id, logging.Identifier(view), c.ID(), string(debug.Stack()))
	}
	res, err := cm.runner.RunView(c, view)
	if err != nil {
		logger.DebugfContext(ctx, "[%s] InitiateView [view:%s], [ContextID:%s] failed [%s]", id, logging.Identifier(view), c.ID(), err)
		return nil, err
	}
	logger.DebugfContext(ctx, "[%s] InitiateView [view:%s], [ContextID:%s] terminated", id, logging.Identifier(view), c.ID())
	return res, nil
}

// InitiateContext initiates a view context for the given view.
func (cm *Manager) InitiateContext(ctx context.Context, view view.View) (view.Context, error) {
	return cm.InitiateContextFrom(ctx, view, cm.identityProvider.DefaultIdentity(), "")
}

// InitiateContextWithIdentity initiates a view context for the given view and initiator identity.
func (cm *Manager) InitiateContextWithIdentity(ctx context.Context, view view.View, id view.Identity) (view.Context, error) {
	return cm.InitiateContextFrom(ctx, view, id, "")
}

// InitiateContextWithIdentityAndID initiates a view context for the given view, initiator identity, and context ID.
func (cm *Manager) InitiateContextWithIdentityAndID(ctx context.Context, view view.View, id view.Identity, contextID string) (view.Context, error) {
	return cm.InitiateContextFrom(ctx, view, id, contextID)
}

// InitiateContextFrom initiates a view context for the given view, initiator identity, and context ID from the given go context.
// It is responsibility of the caller to delete context from this manager when not needed anymore.
func (cm *Manager) InitiateContextFrom(ctx context.Context, view view.View, id view.Identity, contextID string) (view.Context, error) {
	if id.IsNone() {
		id = cm.identityProvider.DefaultIdentity()
	}
	c, err := cm.newChildContextForInitiator(ctx, view, id, contextID)
	if err != nil {
		return nil, err
	}

	logger.DebugfContext(ctx, "[%s] InitiateContext [view:%s], [ContextID:%s]\n", id, logging.Identifier(view), c.ID())

	return c, nil
}

func (cm *Manager) newChildContextForInitiator(ctx context.Context, view view.View, id view.Identity, contextID string) (*ChildContext, error) {
	viewContext, err := cm.contextFactory.NewForInitiator(
		ctx,
		contextID,
		id,
		view,
	)
	if err != nil {
		return nil, err
	}
	c := NewChildContextFromParent(viewContext)
	sh := cm.contexts.shard(c.ID())
	sh.mu.Lock()
	if _, ok := sh.m[c.ID()]; !ok {
		cm.contexts.count.Add(1)
	}
	sh.m[c.ID()] = c
	sh.mu.Unlock()
	cm.metrics.Contexts.Set(float64(cm.contexts.len()))

	return c, nil
}

// Context returns a view.Context for a given contextID. If the context does not exist, an error is returned.
func (cm *Manager) Context(contextID string) (view.Context, error) {
	sh := cm.contexts.shard(contextID)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	viewCtx, ok := sh.m[contextID]
	if !ok {
		return nil, errors.Wrapf(ErrContextNotFound, "context %s not found", contextID)
	}
	return viewCtx, nil
}

// RegisterContext registers a view context.
// It is responsibility of the caller to remove context from this manager when not needed anymore.
func (cm *Manager) RegisterContext(contextID string, ctx DisposableContext) error {
	sh := cm.contexts.shard(contextID)
	sh.mu.Lock()
	if _, ok := sh.m[contextID]; !ok {
		cm.contexts.count.Add(1)
	}
	sh.m[contextID] = ctx
	sh.mu.Unlock()
	cm.metrics.Contexts.Set(float64(cm.contexts.len()))

	return nil
}

// NewResponderContext returns a context to be used to respond to an incoming message on the given session.
// It returns the context, a boolean indicating if it's new, and an error.
func (cm *Manager) NewResponderContext(ctx context.Context, contextID string, session view.Session, me, remote view.Identity) (view.Context, bool, error) {
	if me.IsNone() {
		me = cm.identityProvider.DefaultIdentity()
	}

	sessionID := session.Info().ID
	caller := session.Info().Caller

	// Existing context: handle session swap / reuse under the shard lock.
	sh := cm.contexts.shard(contextID)
	sh.mu.Lock()
	if viewContext, ok := sh.m[contextID]; ok {
		if viewContext.Session() != nil && viewContext.Session().Info().ID != sessionID {
			// next we need to unwrap the actual context to store the session
			vCtx, ok := viewContext.(ParentContext)
			if !ok {
				sh.mu.Unlock()
				panic("Not a ParentContext!")
			}

			// TODO: replace this with `vCtx.PutSession`, however, that method requires a view as input but we only have the viewID
			if err := vCtx.PutSessionByID(string(caller), remote, session); err != nil {
				sh.mu.Unlock()
				return nil, false, errors.Wrapf(err, "failed registering session for [%s]", caller)
			}

			// we wrap our context and set our new session as the default session
			c := NewChildContextFromParentAndSession(vCtx, session)
			sh.m[contextID] = c
			sh.mu.Unlock()

			return c, false, nil
		}
		logger.DebugfContext(viewContext.Context(), "[%s] No new context to respond, reuse [contextID:%s]\n", me, contextID)
		sh.mu.Unlock()
		return viewContext, false, nil
	}
	sh.mu.Unlock()

	// Create the new context outside the lock so concurrent responders don't serialize on the shard.
	logger.Debugf("[%s] Create new context to respond [contextID:%s]\n", me, contextID)
	newCtx, err := cm.contextFactory.NewForResponder(
		ctx,
		contextID,
		me,
		session,
		remote,
	)
	if err != nil {
		return nil, false, err
	}

	c := NewChildContextFromParent(newCtx)

	// Publish under the shard lock; double-check in case a concurrent responder for the same contextID won the race.
	sh.mu.Lock()
	if existing, ok := sh.m[contextID]; ok {
		sh.mu.Unlock()
		return existing, false, nil
	}
	sh.m[contextID] = c
	cm.contexts.count.Add(1)
	sh.mu.Unlock()
	cm.metrics.Contexts.Set(float64(cm.contexts.len()))

	context.AfterFunc(c.Context(), func() {
		cm.DeleteContext(contextID)
	})

	return c, true, nil
}

// GetIdentifier returns the identifier for the given view.
func (cm *Manager) GetIdentifier(f view.View) string {
	return GetIdentifier(f)
}

// ExistResponderForCaller returns the responder view and identity for the given caller identifier.
func (cm *Manager) ExistResponderForCaller(caller string) (view.View, view.Identity, error) {
	return cm.registry.ExistResponderForCaller(caller)
}

// DeleteContext removes a context from the manager and calls Dispose on the context.
func (cm *Manager) DeleteContext(contextID string) {
	sh := cm.contexts.shard(contextID)
	sh.mu.Lock()
	// dispose context
	if viewCtx, ok := sh.m[contextID]; ok {
		viewCtx.Dispose()
		delete(sh.m, contextID)
		cm.contexts.count.Add(-1)
		sh.mu.Unlock()
		cm.metrics.Contexts.Set(float64(cm.contexts.len()))
		return
	}
	sh.mu.Unlock()
}

// Runner models a view runner.
type Runner interface {
	// RunView runs the given responder view in the given view context.
	RunView(viewCtx view.Context, responder view.View) (any, error)
}

type defaultRunner struct{}

func (r *defaultRunner) RunView(viewCtx view.Context, responder view.View) (any, error) {
	return viewCtx.RunView(responder)
}

// NewDefaultRunner returns a new instance of the default view runner.
func NewDefaultRunner() Runner {
	return &defaultRunner{}
}
