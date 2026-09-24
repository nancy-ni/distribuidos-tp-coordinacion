package messagehandler

import (
	"math/rand/v2"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

const FIXED_SEED = 42

type MessageHandler struct {
	clientId uint64
}

func NewMessageHandler() MessageHandler {
	clientId := generateClientId()
	return MessageHandler{clientId: clientId}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	data := []fruititem.FruitItem{fruitRecord}
	return inner.SerializeMessage(messageHandler.clientId, data)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	data := []fruititem.FruitItem{}
	return inner.SerializeMessage(messageHandler.clientId, data)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	clientId, fruitRecords, _, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}
	if clientId != messageHandler.clientId {
		return nil, nil
	}
	return fruitRecords, nil
}

func generateClientId() uint64 {
	pcg := rand.NewPCG(FIXED_SEED, FIXED_SEED)
	r := rand.New(pcg)
	return r.Uint64()
}
