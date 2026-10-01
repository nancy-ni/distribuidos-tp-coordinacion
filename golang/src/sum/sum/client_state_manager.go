package sum

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

type ClientStateManager struct {
	clientId               uint64
	fruitItems             map[string]fruititem.FruitItem
	coordinatorRoutingKey  string
	totalSentMessages      int
	totalProcessedMessages int
	localProcessedCount    int
}

func NewClientStateManager(clientId uint64) *ClientStateManager {
	return &ClientStateManager{
		clientId:               clientId,
		fruitItems:             map[string]fruititem.FruitItem{},
		coordinatorRoutingKey:  "",
		totalSentMessages:      0,
		totalProcessedMessages: 0,
		localProcessedCount:    0,
	}
}

func (m *ClientStateManager) SetCoordinator(coordRoutingKey string, currentIsCoordinator bool, totalSentMsg ...int) {
	m.coordinatorRoutingKey = coordRoutingKey
	if currentIsCoordinator {
		m.totalSentMessages = totalSentMsg[0]
		m.totalProcessedMessages += m.localProcessedCount
	}
}

func (m *ClientStateManager) HasCoordinator() bool {
	return m.coordinatorRoutingKey != ""
}

func (m *ClientStateManager) GetCoordinator() string {
	return m.coordinatorRoutingKey
}

func (m *ClientStateManager) AllMessagesProcessed() bool {
	return m.totalProcessedMessages >= m.totalSentMessages
}

func (m *ClientStateManager) GetLocalProcessedCount() int {
	return m.localProcessedCount
}

func (m *ClientStateManager) UpdateTotalProcessedMessages(delta int) {
	m.totalProcessedMessages += delta
}

func (m *ClientStateManager) GetFruitItems() map[string]fruititem.FruitItem {
	return m.fruitItems
}

func (m *ClientStateManager) RegisterFruits(fruitRecords []fruititem.FruitItem) {
	clientFruits := m.fruitItems
	for _, fruitRecord := range fruitRecords {
		_, ok := clientFruits[fruitRecord.Fruit]
		if ok {
			clientFruits[fruitRecord.Fruit] = clientFruits[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			clientFruits[fruitRecord.Fruit] = fruitRecord
		}
	}
	m.localProcessedCount++
}

func (m *ClientStateManager) ResetClientState() {
	clear(m.fruitItems)
	m.localProcessedCount = 0
}
