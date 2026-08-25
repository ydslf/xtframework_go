# xtframework

`xtframework` 是基于 [xtnet_go](https://github.com/ydslf/xtnet_go) 的分布式游戏服务器框架。它把一个进程抽象为 `Node`，把中心服、网关服、地图服、房间服等逻辑单元抽象为 `Service`。

## 特性

- 一个 Node 可按 YAML 配置加载多个 Service。
- 每个 Service 独占一个 `xtnet/frame.Loop`，消息串行执行。
- 本地 Service 通过 Loop 直接投递，远程 Service 通过 xtnet TCP RPC 通信。
- 主 Node 维护内存服务注册表，其他 Node 查询后直连目标 Node。
- 非主 Node 缓存主 Node 返回的 Service 路由，避免每条消息重复查询。
- 支持异步单向 `Send2Service` 和带 `context.Context` 的 `CallService`。
- 业务消息支持 xtnet 二进制编码或 Protobuf 编码。

## 配置

```yaml
main_node: 1
codec: xtnet

nodes:
  - id: 1
    listen_addr: "127.0.0.1:7001"
    services:
      - name: center
        id: 1

  - id: 2
    listen_addr: "127.0.0.1:7002"
    services:
      - name: room
        id: 1
      - name: room
        id: 2
```

## 快速开始

业务 Service 通常嵌入 `BaseService`，并覆盖消息处理方法：

```go
type Echo struct {
    xtframework.BaseService
}

func (s *Echo) HandleMessage(ctx *xtframework.MessageContext, msg *xtframework.Message) error {
    if ctx.IsRequest() {
        return ctx.Respond(&xtframework.Message{ID: 2, Payload: &Reply{Text: "ok"}})
    }
    return nil
}
```

在创建 Node 前注册 Service 工厂和业务消息类型：

```go
factories := xtframework.NewFactoryRegistry()
_ = factories.Register("echo", func(node *xtframework.Node, cfg xtframework.ServiceConfig) (xtframework.Service, error) {
    return &Echo{BaseService: xtframework.NewBaseService(node, cfg)}, nil
})

messages := xtframework.NewMessageRegistry()
_ = messages.Register(1, func() any { return &Request{} })
_ = messages.Register(2, func() any { return &Reply{} })

cfg, _ := xtframework.LoadConfig("nodes.yaml")
node, _ := xtframework.NewNode(cfg, 1,
    xtframework.WithFactoryRegistry(factories),
    xtframework.WithMessageRegistry(messages),
)
_ = node.Start()
defer node.Stop()
```

`Node` 的运行状态和内部容器均由框架管理。可通过 `ID()`、`MainNodeID()`、
`ListenAddr()`、`LocalService()`、`LocalServices()`、`RegisteredService()` 等
只读方法查询，不应直接修改 Node 内部的 Service、注册表或 RPC 连接。

完整示例见 [`examples/basic`](examples/basic)，进一步说明见 [`docs/quickstart.md`](docs/quickstart.md)。

## 验证

```bash
go test ./...
go test -race ./...
```

当前版本的 Node 间数据包受 xtnet TCP 默认最大包长限制（64 KiB）。注册表仅保存在主 Node 内存中；主 Node 重启后，各 Node 需要重新启动以完成重新注册。
