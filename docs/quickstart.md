# 使用指南

## 启动模型

应用先加载完整集群配置，再以当前进程的 Node ID 创建一个 `Node`。框架约定一个
进程只运行一个 Node；Node 创建时会从自己的 `logger` 配置创建并安装 xtnet 的
进程级 Logger。`Node.Start` 按以下顺序执行：

1. 启动 Node 控制 Loop 和内部 TCP RPC Server。
2. 通过注册的 Factory 创建并启动本地 Service。
3. 主 Node 将本地 Service 写入内存注册表；其他 Node 连接主 Node 并注册。

非主 Node 必须在主 Node 已经监听后启动。停止时框架先注销 Service，再关闭内部连接，最后停止各 Service Loop。

## Service 与消息

每个 Service 必须拥有不同的 `*frame.Loop`。推荐嵌入 `BaseService`，它已提供身份、Loop、配置以及数字消息 ID 和字符串消息 ID 对应的发送、调用方法。

框架支持 `uint32` 和字符串两种业务消息 ID，并传输 `[]byte` 负载。数字 ID 使用原有的 `Send2Service`、`CallService`、`CallServiceSync`；字符串 ID 使用对应的 `Send2ServiceString`、`CallServiceString`、`CallServiceSyncString`。数字 `0`、空字符串、无效 UTF-8 以及超过 256 字节的字符串 ID 均为无效消息。应用负责使用 Protobuf、JSON 或其他格式编码和解码；空负载是合法消息。框架不会复制业务负载；发送或响应后，调用者不得再修改或复用传入的切片。

单向发送：

```go
payload, err := proto.Marshal(&gamepb.PlayerEnter{PlayerId: 42})
if err == nil {
    err = service.Send2Service("room", 1, 1001, payload)
}
```

字符串消息 ID 的发送方式相同：

```go
err := service.Send2ServiceString("room", 1, "player.enter", payload)
```

异步请求响应：

```go
err := service.CallService(
	"center", 1, 1001, requestPayload,
	func(replyPayload []byte, err error) {
		// 回调在调用方 Service 的 Loop 中执行。
	},
)
```

目标 Service 在 `HandleRPCRequest` 中返回响应负载和错误；响应不携带消息号。处理器 panic 或返回的错误都会转换为调用错误。处理器返回后不得再修改或复用响应负载。`CallService` 不阻塞，回调会投递回调用方 Service 的 Loop；返回错误表示请求未发起，此时不会执行回调。Service 调用超时通过 Node 的 `service_call_timeout` 配置，默认值为 `3s`，也可通过 `WithServiceCallTimeout` 覆盖 YAML。为避免本地高频调用产生定时器开销，异步本地调用不计算超时；`CallServiceSync` 的本地和远端调用都会等待结果或超时，不应在需要保持响应的 Service Loop 中使用。

## 应用层编解码

Service 收到消息号和原始负载后自行解码：

```go
func (s *Room) HandleRPCDirect(ctx xtframework.MessageContext, messageID uint32, payload []byte) error {
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

func (s *Room) HandleRPCRequest(ctx xtframework.MessageContext, messageID uint32, payload []byte) ([]byte, error) {
    switch messageID {
    case 1002:
        var request gamepb.PlayerQuery
		if err := proto.Unmarshal(payload, &request); err != nil {
			return nil, err
		}
		return proto.Marshal(&gamepb.PlayerReply{/* ... */})
    default:
		return nil, fmt.Errorf("unknown message id %d", messageID)
	}
}
```

`Service` 同时定义数字和字符串消息处理方法。嵌入 `BaseService` 后，字符串处理方法已有默认的“不处理”实现，只需按业务需要覆盖：

```go
func (s *Room) HandleRPCDirectString(ctx xtframework.MessageContext, messageID string, payload []byte) error {
	switch messageID {
	case "player.enter":
		// 解码并处理 payload
		return nil
	default:
		return fmt.Errorf("unknown message id %q", messageID)
	}
}

func (s *Room) HandleRPCRequestString(ctx xtframework.MessageContext, messageID string, payload []byte) ([]byte, error) {
	switch messageID {
	case "player.query":
		// 解码请求并返回响应
		return replyPayload, nil
	default:
		return nil, fmt.Errorf("unknown message id %q", messageID)
	}
}
```

Node 间 RPC 信封和框架控制消息仍由框架内部编码，与业务负载格式无关。本地投递直接传递业务字节切片，因此应用必须遵守负载所有权约定。

## 路由与故障语义

- 本地目标：直接投递到目标 Service Loop。
- 远程目标：向主 Node 查询 `ServiceLocation`，然后复用或创建到目标 Node 的 TCP 连接。
- 非主 Node 默认缓存 Service 路由 1 小时；主 Node 会在 Service 注册、注销或所在 Node 断开时推送失效通知。可用 `WithRouteCacheTTL` 调整，设置为 `0` 表示永不过期；使用 `WithRouteCacheDisabled` 可禁用缓存。TTL 是通知丢失时的兜底。
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
