package xtframework

import (
	"fmt"
	"time"
)

const (
	mainRecoveryInitialDelay = 100 * time.Millisecond
	mainRecoveryMaxDelay     = 3 * time.Second
)

// startMainRecovery 最多启动一个后台恢复协程。
// 非主 Node 在运行期间失去主 Node 连接时调用。
func (n *Node) startMainRecovery() {
	if n.IsMainNode() || n.state.Load() != nodeStateRunning {
		return
	}

	n.mainRecoveryMu.Lock()
	defer n.mainRecoveryMu.Unlock()
	if n.mainRecoveryRunning || n.state.Load() != nodeStateRunning || n.mainRecoveryStopped() {
		return
	}
	n.mainRecoveryRunning = true
	n.mainRecoveryWG.Add(1)
	go n.recoverMainNode()
}

func (n *Node) recoverMainNode() {
	recovered := false
	defer func() {
		n.mainRecoveryMu.Lock()
		n.mainRecoveryRunning = false
		n.mainRecoveryMu.Unlock()
		n.mainRecoveryWG.Done()
		if !recovered || n.state.Load() != nodeStateRunning || n.mainRecoveryStopped() {
			return
		}
		n.clientsMu.RLock()
		client := n.rpcClients[n.mainNodeID]
		connected := client != nil && client.Connected()
		n.clientsMu.RUnlock()
		if !connected {
			n.startMainRecovery()
		}
	}()
	mainConfig, exists := n.config.Node(n.mainNodeID)
	if !exists {
		n.report(fmt.Errorf("%w: main node %d", ErrNodeNotFound, n.mainNodeID))
		return
	}
	delay := time.Duration(0)
	for {
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-n.mainRecoveryStop:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return
			}
		}
		if n.state.Load() != nodeStateRunning || n.mainRecoveryStopped() {
			return
		}

		n.clientCreateMu.Lock()
		client, err := n.recoverMainNodeClient(mainConfig)
		if err == nil && client != nil && (n.state.Load() != nodeStateRunning || n.mainRecoveryStopped()) {
			err = ErrNodeStopped
		}
		if err == nil && client != nil {
			err = n.restoreMainNodeState(client)
			if err != nil {
				err = fmt.Errorf("restore node %d state on main node: %w", n.id, err)
			}
		}
		if err == nil && client != nil {
			n.clientsMu.Lock()
			if !client.Connected() {
				err = ErrRPCDisconnected
			} else {
				n.rpcClients[n.mainNodeID] = client
			}
			n.clientsMu.Unlock()
		}
		if err == nil && client != nil {
			client.startHeartbeat()
		} else if client != nil {
			client.closeTransport()
		}
		n.clientCreateMu.Unlock()

		if err == nil {
			recovered = true
			n.logger.LogDebug("restored connection and local state on main node %d", n.mainNodeID)
			return
		}
		n.logger.LogWarn("restore connection to main node %d failed: %v", n.mainNodeID, err)

		if delay == 0 {
			delay = mainRecoveryInitialDelay
		} else {
			delay *= 2
			if delay > mainRecoveryMaxDelay {
				delay = mainRecoveryMaxDelay
			}
		}
	}
}

// recoverMainNodeClient 清理失效连接，并创建一个尚未发布的新主 Node 连接。
// 返回 nil client 表示已有可用连接，无需再次恢复。
func (n *Node) recoverMainNodeClient(mainConfig NodeConfig) (*RPCClient, error) {
	n.clientsMu.RLock()
	current := n.rpcClients[n.mainNodeID]
	if current != nil && current.Connected() && current.Addr() == mainConfig.ListenAddr {
		n.clientsMu.RUnlock()
		return nil, nil
	}
	n.clientsMu.RUnlock()

	var stale *RPCClient
	n.clientsMu.Lock()
	if current = n.rpcClients[n.mainNodeID]; current != nil {
		if current.Connected() && current.Addr() == mainConfig.ListenAddr {
			n.clientsMu.Unlock()
			return nil, nil
		}
		stale = current
		delete(n.rpcClients, n.mainNodeID)
	}
	n.clientsMu.Unlock()
	if stale != nil {
		stale.closeTransport()
	}

	return n.connectRPCClient(mainConfig.ID, mainConfig.ListenAddr)
}

func (n *Node) stopMainRecovery() {
	n.mainRecoveryStopOnce.Do(func() { close(n.mainRecoveryStop) })
	// 与可能发生的恢复协程创建串行化，避免 Wait 与 Add 并发执行。
	n.mainRecoveryMu.Lock()
	n.mainRecoveryMu.Unlock()
	n.mainRecoveryWG.Wait()
}

func (n *Node) mainRecoveryStopped() bool {
	select {
	case <-n.mainRecoveryStop:
		return true
	default:
		return false
	}
}

// restoreMainNodeState 先重新注册所有仍然活跃的本地 Service，
// 再恢复它们的订阅，使每个订阅者都能收到最新的服务快照。
func (n *Node) restoreMainNodeState(client *RPCClient) error {
	for _, key := range n.serviceOrder {
		if n.state.Load() != nodeStateRunning {
			return ErrNodeStopped
		}
		runtime := n.localServices[key]
		if !runtime.registered.Load() {
			continue
		}
		if err := n.registerServiceWithClient(client, key); err != nil {
			return fmt.Errorf("register service %s: %w", key, err)
		}
	}

	for _, subscriber := range n.serviceOrder {
		if n.state.Load() != nodeStateRunning {
			return ErrNodeStopped
		}
		runtime := n.localServices[subscriber]
		if !runtime.registered.Load() {
			continue
		}
		if err := n.subscribeRuntimeWithClient(client, subscriber, runtime); err != nil {
			return fmt.Errorf("subscribe service %s: %w", subscriber, err)
		}
	}
	return nil
}
