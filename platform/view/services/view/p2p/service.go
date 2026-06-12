/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package p2p

import (
	"context"
	"runtime/debug"
	"sync"

	"github.com/hyperledger-labs/fabric-smart-client/pkg/utils/errors"
	"github.com/hyperledger-labs/fabric-smart-client/platform/common/services/logging"
	"github.com/hyperledger-labs/fabric-smart-client/platform/view/view"
)

var logger = logging.MustGetLogger()

// IdentityProvider models the identity provider for P2P operations.
type IdentityProvider interface {
	// DefaultIdentity returns the default identity.
	DefaultIdentity() view.Identity
}

// ViewManager models the view manager for P2P operations.
type ViewManager interface {
	// ExistResponderForCaller returns the responder view for the given caller.
	ExistResponderForCaller(caller string) (view.View, view.Identity, error)
	// NewResponderContext returns a context used to respond to an invocation.
	NewResponderContext(ctx context.Context, contextID string, session view.Session, me, remote view.Identity) (view.Context, bool, error)
	// DeleteContext deletes the view context for the given context ID.
	DeleteContext(contextID string)
}

// CommLayer models the communication layer for P2P operations.
//
//go:generate counterfeiter -o mock/comm.go -fake-name CommLayer . CommLayer
type CommLayer interface {
	// MasterSession returns the master session.
	MasterSession() (view.Session, error)
	// NewResponderSession returns a new session for the given arguments.
	NewResponderSession(caller []byte, msg *view.Message) (view.Session, error)
}

// EndpointService models the dependency to the view-sdk's endpoint service.
// It provides methods to retrieve identities.
//
//go:generate counterfeiter -o mock/resolver.go -fake-name EndpointService . EndpointService
type EndpointService interface {
	// GetIdentity returns the identity for the given endpoint and public key ID.
	GetIdentity(endpoint string, pkID []byte) (view.Identity, error)
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

// Service is responsible for handling incoming messages from the communication layer.
type Service struct {
	viewManager      ViewManager
	identityProvider IdentityProvider
	endpointService  EndpointService
	commLayer        CommLayer
	runner           Runner

	// activeMu guards activeResponders: in-flight responder counts per context
	// ID. A caller may open a second session into the same context (e.g. a
	// retry after a lost message); the context must be disposed only when the
	// LAST responder finishes, not when the first one does.
	activeMu         sync.Mutex
	activeResponders map[string]*responderContextRef
}

// responderContextRef tracks how many responders are running on a context and
// whether this node created the context for responding (and thus owns its
// disposal).
type responderContextRef struct {
	count     int
	deletable bool
}

// NewService returns a new instance of the P2P service.
func NewService(
	viewManager ViewManager,
	identityProvider IdentityProvider,
	commLayer CommLayer,
	endpointService EndpointService,
	runner Runner,
) *Service {
	return &Service{
		viewManager:      viewManager,
		identityProvider: identityProvider,
		commLayer:        commLayer,
		endpointService:  endpointService,
		runner:           runner,
		activeResponders: map[string]*responderContextRef{},
	}
}

// retainContext registers an in-flight responder for the given context ID.
// It is called before the context is resolved so a concurrent last-responder
// release cannot dispose the context in between.
func (s *Service) retainContext(contextID string) {
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	ref, ok := s.activeResponders[contextID]
	if !ok {
		ref = &responderContextRef{}
		s.activeResponders[contextID] = ref
	}
	ref.count++
}

// markContextDeletable records that this node created the context for
// responding, so the last responder to finish must dispose it.
func (s *Service) markContextDeletable(contextID string) {
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	if ref, ok := s.activeResponders[contextID]; ok {
		ref.deletable = true
	}
}

// releaseContext unregisters an in-flight responder and reports whether the
// caller must dispose the context (last responder out on a context this node
// created for responding).
func (s *Service) releaseContext(contextID string) bool {
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	ref, ok := s.activeResponders[contextID]
	if !ok {
		return false
	}
	ref.count--
	if ref.count > 0 {
		return false
	}
	delete(s.activeResponders, contextID)
	return ref.deletable
}

// Start starts the P2P service.
func (s *Service) Start(ctx context.Context) error {
	session, err := s.commLayer.MasterSession()
	if err != nil {
		return errors.Wrap(err, "failed getting master session")
	}
	go func() {
		for {
			ch := session.Receive()
			select {
			case msg := <-ch:
				go s.handleMessage(msg)
			case <-ctx.Done():
				logger.DebugfContext(ctx, "received done signal, stopping listening to messages on the master session")
				return
			}
		}
	}()
	return nil
}

// handleMessage handles an incoming message.
func (s *Service) handleMessage(msg *view.Message) {
	logger.Debugf("Will call responder view for context [%s]", msg.ContextID)
	responder, id, err := s.viewManager.ExistResponderForCaller(msg.Caller)
	if err != nil {
		logger.Errorf("[%s] No responder exists for [%s]: [%s]", s.identityProvider.DefaultIdentity(), msg.String(), err)
		return
	}
	if id.IsNone() {
		id = s.identityProvider.DefaultIdentity()
	}

	if err := s.respond(responder, id, msg); err != nil {
		logger.Errorf("[%s] error during respond [%s]", s.identityProvider.DefaultIdentity(), err)
	}
}

// respond executes a given responder view.
func (s *Service) respond(responder view.View, id view.Identity, msg *view.Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("respond triggered panic: %s\n%s\n", r, debug.Stack())
			err = errors.Errorf("failed responding [%s]", r)
		}
	}()

	// Retain before resolving the context: a caller may run several sessions
	// on the same context (e.g. a retry after a lost message), and disposing
	// on the FIRST responder's exit would tear down sessions the others are
	// still using. The context is disposed by the LAST responder out.
	s.retainContext(msg.ContextID)
	defer func() {
		if s.releaseContext(msg.ContextID) {
			s.viewManager.DeleteContext(msg.ContextID)
		}
	}()

	// get context
	viewCtx, isNew, err := s.getOrCreateContext(id, msg)
	if err != nil {
		return errors.WithMessagef(err, "failed getting context for [%s,%s]", msg.ContextID, id)
	}

	logger.DebugfContext(viewCtx.Context(), "[%s] Respond [from:%s], [sessionID:%s], [contextID:%s](%v), [view:%s]", id, msg.FromEndpoint, msg.SessionID, msg.ContextID, isNew, logging.Identifier(responder))

	// if a new context has been created to run the responder, this node owns
	// its disposal (performed by the last responder's release above)
	if isNew {
		s.markContextDeletable(msg.ContextID)
	}

	// run view
	_, err = s.runner.RunView(viewCtx, responder)
	if err != nil {
		logger.DebugfContext(viewCtx.Context(), "[%s] Respond Failure [from:%s], [sessionID:%s], [contextID:%s] [%s]\n", id, msg.FromEndpoint, msg.SessionID, msg.ContextID, err)

		// try to send error back to caller
		if serr := viewCtx.Session().SendError([]byte(err.Error())); serr != nil {
			logger.Error(serr.Error())
		}
	}

	return nil
}

// getOrCreateContext returns a view context for the given arguments.
func (s *Service) getOrCreateContext(me view.Identity, msg *view.Message) (view.Context, bool, error) {
	// get the caller identity
	remote, err := s.endpointService.GetIdentity(msg.FromEndpoint, msg.FromPKID)
	if err != nil {
		return nil, false, err
	}

	// create a new session with the ID we received
	responderSession, err := s.commLayer.NewResponderSession(remote, msg)
	if err != nil {
		return nil, false, err
	}

	return s.viewManager.NewResponderContext(
		msg.Ctx,
		msg.ContextID,
		responderSession,
		me,
		remote,
	)
}
