# 使用指南

## 启动模型

应用先加载完整集群配置，再以当前进程的 Node ID 创建一个 `Node`。`Node.Start` 按以下顺序执行：

1. 启动 Node 控制 Loop 和内部 TCP RPC Server。
2. 通过注册的 Factory 创建并启动本地 Service。
3. 主 Node 将本地 Service 写入内存注册表；其他 Node 连接主 Node 并注册。

非主 Node 必须在主 Node 已经监听后启动。停止时框架先注销 Service，再关闭内部连接，最后停止各 Service Loop。

## Service 与消息

每个 Service 必须拥有不同的 `*frame.Loop`。推荐嵌入 `BaseService`，它已提供身份、Loop、配置以及 `Send2Service`、`CallService` 方法。

框架只识别 `uint32` 业务消息号并传输 `[]byte` 负载。应用负责使用 Protobuf、JSON 或其他格式编码和解码；空负载是合法消息，消息号 `0` 保留为无效值。

单向发送：

```go
payload, err := proto.Marshal(&gamepb.PlayerEnter{PlayerId: 42})
if err == nil {
    err = service.Send2Service("room", 1, 1001, payload)
}
```

请求响应：

```go
replyID, replyPayload, err := service.CallService(
    2*time.Second, "center", 1, 1001, requestPayload,
)
```

目标 Service 必须在 `HandleMessage` 内调用一次 `ctx.Respond(replyID, replyPayload)`。未响应、重复响应、处理器 panic 或返回错误都会转换为调用错误。`CallService` 会等待结果，因此不要在延迟敏感的 Service Loop 中进行长超时同步等待；可由业务层启动 goroutine，或封装自己的异步回调模式。

## 应用层编解码

Service 收到消息号和原始负载后自行解码：

```go
func (s *Room) HandleMessage(ctx *xtframework.MessageContext, messageID uint32, payload []byte) error {
    switch messageID {
    case 1001:
        var request gamepb.PlayerEnter
        if err := proto.Unmarshal(payload, &request); err != nil {
            return err
        }
        // 处理 request
        return nil
    default:
        return fmt.Errorf("unknown message id %d", messageID)
    }
}
```

Node 间 RPC 信封和框架控制消息仍由框架内部编码，与业务负载格式无关。本地投递也使用字节负载，并复制切片以避免发送方后续修改造成数据竞态。

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
