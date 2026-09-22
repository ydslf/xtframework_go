# xtframework

`xtframework` 是基于 [xtnet_go](https://github.com/ydslf/xtnet_go) 的分布式游戏服务器框架。它把一个进程抽象为 `Node`，把中心服、网关服、地图服、房间服等逻辑单元抽象为 `Service`。

## 特性

- 一个 Node 可按 YAML 配置加载多个 Service。
- 每个 Service 独占一个 `xtnet/frame.Loop`，消息串行执行。
- 本地 Service 通过 Loop 直接投递，远程 Service 通过 xtnet TCP RPC 通信。
- 主 Node 维护内存服务注册表，其他 Node 查询后直连目标 Node。
- 非主 Node 缓存主 Node 返回的 Service 路由，避免每条消息重复查询。
- 支持异步单向 `Send2Service`、回调式 `CallService` 和阻塞式 `CallServiceSync`。
- 框架传输业务消息号和原始字节，序列化格式由应用层决定。
- 框架不复制业务负载；调用 `Send2Service` 或 `Respond` 后不得修改或复用传入切片。

## 配置

```yaml
main_node: 1

nodes:
  - id: 1
    listen_addr: "127.0.0.1:7001"
    service_call_timeout: 3s
    logger:
      dir: "./logs/node_1"
      level: "debug"
      file_size: 67108864
      screen: true
      async: true
    services:
      - name: center
        id: 1

  - id: 2
    listen_addr: "127.0.0.1:7002"
    service_call_timeout: 3s
    logger:
      dir: "./logs/node_2"
      level: "debug"
      file_size: 67108864
      screen: true
      async: true
    services:
      - name: room
        id: 1
      - name: room
        id: 2
```

## 快速开始

业务 Service 通常嵌入 `BaseService`，并按需覆盖单向消息或请求处理方法：

```go
type Echo struct {
    xtframework.BaseService
}

func (s *Echo) HandleRPCDirect(ctx *xtframework.MessageContext, messageID uint32, payload []byte) error {
	var request Request
	if err := json.Unmarshal(payload, &request); err != nil {
		return err
	}
	// 处理单向消息
    return nil
}

func (s *Echo) HandleRPCRequest(ctx *xtframework.MessageContext, messageID uint32, payload []byte) ([]byte, error) {
	var request Request
	if err := json.Unmarshal(payload, &request); err != nil {
		return nil, err
	}
	return json.Marshal(&Reply{Text: "ok"})
}
```

也可以使用字符串消息 ID。发送方调用 `Send2ServiceString`、`CallServiceString` 或 `CallServiceSyncString`；`Service` 接口包含对应的字符串处理方法，嵌入 `BaseService` 后可按需覆盖 `HandleRPCDirectString` 或 `HandleRPCRequestString`：

```go
func (s *Echo) HandleRPCRequestString(ctx *xtframework.MessageContext, messageID string, payload []byte) ([]byte, error) {
	if messageID != "echo.request" {
		return nil, fmt.Errorf("unknown message id %q", messageID)
	}
	return json.Marshal(&Reply{Text: "ok"})
}
```

字符串消息 ID 必须是非空的有效 UTF-8，且最长 256 字节；原有 `uint32` API 和处理接口保持兼容。

在创建 Node 前注册 Service 工厂：

```go
factories := xtframework.NewFactoryRegistry()
_ = factories.Register("echo", func(node *xtframework.Node, cfg xtframework.ServiceConfig) (xtframework.Service, error) {
    return &Echo{BaseService: xtframework.NewBaseService(node, cfg)}, nil
})

cfg, _ := xtframework.LoadConfig("nodes.yaml")
node, _ := xtframework.NewNode(cfg, 1,
    xtframework.WithFactoryRegistry(factories),
)
_ = node.Start()
defer node.Stop()
```

## Service 订阅

Service 可以按名字订阅其他 Service 的发现事件。`Subscribe` 可以在 `Start`
中调用；框架会先完成当前 Service 的注册，再向主 Node 提交订阅：

```go
func (s *Gateway) Start() error {
    return s.Subscribe("room")
}

func (s *Gateway) HandleServiceSnapshot(serviceName string, services []xtframework.ServiceKey) {
    // 订阅建立时的完整快照；当前没有实例时 services 为空。
}

func (s *Gateway) HandleServiceOnline(service xtframework.ServiceKey) {
    // 新的 room Service 注册成功。
}

func (s *Gateway) HandleServiceOffline(service xtframework.ServiceKey) {
    // room Service 注销，或其 Node 与主 Node 断开。
}
```

这些回调都在订阅者自己的 Service Loop 中按顺序执行。重复订阅不会重复保存
订阅关系，但会重新发送当前完整快照。订阅者注销或所在 Node 断开时，主 Node
会自动清理它的订阅关系。

非主 Node 与主 Node 的连接断开后，会在后台按退避策略自动重连。重连成功后
框架会重新注册仍在运行的本地 Service、重放期望订阅，并重新发送完整快照。
断线期间的增量事件不会补发，业务层应把重连后的 Snapshot 当作权威状态，
用它整体替换此前保存的同名 Service 列表。

## 日志

框架约定一个进程只运行一个 Node。每个 Node 从自己的 `logger` 配置创建
`*xtnet/log.Logger`，并将其同时用于 xtnet、Node 和全部 Service。Node 日志自动
带有 `node` 字段，`BaseService.Logger()` 返回的日志器还会带有 `service` 和
`service_id` 字段。不同 Node 的 `logger.dir` 必须不同。

```go
node, err := xtframework.NewNode(cfg, 1,
    xtframework.WithFactoryRegistry(factories),
)
```

也可以显式注入 Logger，以覆盖 YAML 配置：

```go
logger := xtlog.NewLogger("./logs", xtlog.FileSizeMax, true, true)
logger.SetLogLevel(xtlog.LevelDebug)
defer logger.Close()

node, err := xtframework.NewNode(cfg, 1,
    xtframework.WithFactoryRegistry(factories),
    xtframework.WithLogger(logger),
)
```

YAML 创建的 Logger 由 Node 在停止或启动失败时关闭；通过 `WithLogger` 注入的
Logger 由调用方关闭。Service 不拥有 Logger，也不应调用 `Close()`。

`Node` 的运行状态和内部容器均由框架管理。可通过 `ID()`、`MainNodeID()`、
`ListenAddr()`、`LocalService()`、`LocalServices()`、`RegisteredService()` 等
只读方法查询，不应直接修改 Node 内部的 Service、注册表或 RPC 连接。

完整示例见 [`examples/basic`](examples/basic)，进一步说明见 [`docs/quickstart.md`](docs/quickstart.md)。

## 验证

```bash
go test ./...
go test -race ./...
```

当前版本的 Node 间数据包受 xtnet TCP 默认最大包长限制（64 KiB）。注册表仅
保存在主 Node 内存中；主 Node 重启后，其他 Node 会自动重连并恢复注册及订阅状态。
