package xtframework

import "errors"

var (
	ErrNodeNotFound     = errors.New("node not found")
	ErrServiceNotFound  = errors.New("service not found")
	ErrServiceExists    = errors.New("service already registered")
	ErrFactoryNotFound  = errors.New("service factory not found")
	ErrInvalidMessage   = errors.New("invalid message")
	ErrNotRequest       = errors.New("message is not a request")
	ErrAlreadyResponded = errors.New("request already responded")
	ErrNodeRunning      = errors.New("node is already running")
	ErrNodeStopped      = errors.New("node is stopped")
	ErrRPCDisconnected  = errors.New("rpc connection is disconnected")
)
