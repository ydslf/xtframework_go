# 使用指南

## 启动模型

应用先加载完整集群配置，再以当前进程的 Node ID 创建一个 `Node`。`Node.Start` 按以下顺序执行：

1. 启动 Node 控制 Loop 和内部 TCP RPC Server。
2. 通过注册的 Factory 创建并启动本地 Service。
3. 主 Node 将本地 Service 写入内存注册表；其他 Node 连接主 Node 并注册。

非主 Node 必须在主 Node 已经监听后启动。停止时框架先注销 Service，再关闭内部连接，最后停止各 Service Loop。

## Service 与消息

每个 Service 必须拥有不同的 `*frame.Loop`。推荐嵌入 `BaseService`，它已提供身份、Loop、配置以及 `Send2Service`、`CallService` 方法。

`Message.ID` 是业务协议号，`Payload` 是已注册的具体消息指针。远程消息必须在发送和接收 Node 的 `MessageRegistry` 中使用相同 ID 注册相同结构。

单向发送：

```go
err := service.Send2Service("room", 1, &xtframework.Message{
    ID:      1001,
    Payload: &PlayerEnter{PlayerID: 42},
})
```

请求响应：

```go
ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()

reply, err := service.CallService(ctx, "center", 1, request)
```

目标 Service 必须在 `HandleMessage` 内调用一次 `ctx.Respond`。未响应、重复响应、处理器 panic 或返回错误都会转换为调用错误。`CallService` 会等待结果，因此不要在延迟敏感的 Service Loop 中进行长超时同步等待；可由业务层启动 goroutine，或封装自己的异步回调模式。

## Codec

配置 `codec: xtnet` 时，消息体使用 `xtnet/encoding`，工厂应返回可由该编码器处理的结构体指针。

配置 `codec: protobuf` 时，消息工厂必须返回实现 `proto.Message` 的生成类型：

```go
messages.Register(1001, func() any { return &gamepb.PlayerEnter{} })
```

Node 间 RPC 头和框架控制消息不受业务 Codec 影响。一个 Node 实例只能选择一个业务 Codec，同一集群应保持一致。

## 路由与故障语义

- 本地目标：直接投递到目标 Service Loop。
- 远程目标：向主 Node 查询 `ServiceLocation`，然后复用或创建到目标 Node 的 TCP 连接。
- 非主 Node 默认缓存 Service 路由 30 秒；可用 `WithRouteCacheTTL` 调整，设置为 `0` 可禁用。
- 同一 Service 的并发缓存未命中只会触发一次主节点查询；路由相关发送错误会使缓存失效。
- Service 停止时主动注销；Node 异常断开时，主 Node 根据连接关联的 Node ID 清理其注册项。
- TCP 断开后，下一次发送会重新创建连接；本次在途调用由调用方 Context 超时结束。
- 注册表不持久化，也没有主 Node 高可用能力。

## Service 配置参数

YAML 中可为 Service 增加业务参数：

```yaml
- name: room
  id: 1
  options:
    max_players: 100
```

工厂可通过 `ServiceConfig.Options` 读取这些参数。框架只校验 Service 名称与 ID，不解释业务参数。
