package xtframework

import (
	"fmt"
)

// encodeMessage 为业务负载添加消息号。负载内容由应用层自行编码。
func encodeMessage(id uint32, payload []byte) ([]byte, error) {
	if id == 0 {
		return nil, fmt.Errorf("%w: message id is zero", ErrInvalidMessage)
	}
	data := make([]byte, 4+len(payload))
	byteOrder.PutUint32(data, id)
	copy(data[4:], payload)
	return data, nil
}

// decodeMessage 从传输数据中拆出消息号和应用层负载。
func decodeMessage(data []byte) (uint32, []byte, error) {
	if len(data) < 4 {
		return 0, nil, fmt.Errorf("%w: encoded message is shorter than 4 bytes", ErrInvalidMessage)
	}
	id := byteOrder.Uint32(data)
	if id == 0 {
		return 0, nil, fmt.Errorf("%w: message id is zero", ErrInvalidMessage)
	}
	return id, data[4:], nil
}
