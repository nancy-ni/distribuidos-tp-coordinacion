package messagehandler

import (
	"crypto/rand"
	"encoding/binary"
	"strconv"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type MessageHandler struct {
	clientId     uint64
	messagesSent int
}

func NewMessageHandler() MessageHandler {
	clientId := generateClientId()
	return MessageHandler{clientId: clientId, messagesSent: 0}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	data := []fruititem.FruitItem{fruitRecord}
	messageHandler.messagesSent++
	return inner.SerializeMessage(messageHandler.clientId, data, inner.Data)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	data := []fruititem.FruitItem{}
	messagesSentStr := strconv.Itoa(messageHandler.messagesSent)
	return inner.SerializeMessage(messageHandler.clientId, data, inner.Eof, messagesSentStr)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	clientId, fruitRecords, _, _, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}
	if clientId != messageHandler.clientId {
		return nil, nil
	}
	return fruitRecords, nil
}

func generateClientId() uint64 {
	var b [8]byte
	rand.Read(b[:])
	return binary.BigEndian.Uint64(b[:])
}
