package aggregation

import (
	"fmt"
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	outputQueue           middleware.Middleware
	inputExchange         middleware.Middleware
	fruitItemPerClientMap map[uint64]map[string]fruititem.FruitItem
	topSize               int
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		outputQueue:           outputQueue,
		inputExchange:         inputExchange,
		fruitItemPerClientMap: map[uint64]map[string]fruititem.FruitItem{},
		topSize:               config.TopSize,
	}, nil
}

func (aggregation *Aggregation) Run() {
	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientId, fruitRecords, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		if err := aggregation.handleEndOfRecordsMessage(clientId); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	aggregation.handleDataMessage(clientId, fruitRecords)
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientId uint64) error {
	slog.Info("Received End Of Records message")

	fruitTopRecords := []fruititem.FruitItem{}
	clientFruits, ok := aggregation.fruitItemPerClientMap[clientId]
	if ok {
		fruitTopRecords = aggregation.buildFruitTop(clientFruits)
		delete(aggregation.fruitItemPerClientMap, clientId)
	}
	message, err := inner.SerializeMessage(clientId, fruitTopRecords)
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}

	eofMessage := []fruititem.FruitItem{}
	message, err = inner.SerializeMessage(clientId, eofMessage)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}

func (aggregation *Aggregation) handleDataMessage(clientId uint64, fruitRecords []fruititem.FruitItem) {
	if _, ok := aggregation.fruitItemPerClientMap[clientId]; !ok {
		aggregation.fruitItemPerClientMap[clientId] = map[string]fruititem.FruitItem{}
	}

	clientFruits := aggregation.fruitItemPerClientMap[clientId]
	for _, fruitRecord := range fruitRecords {
		if _, ok := clientFruits[fruitRecord.Fruit]; ok {
			clientFruits[fruitRecord.Fruit] = clientFruits[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			clientFruits[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop(clientFruits map[string]fruititem.FruitItem) []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0, len(clientFruits))
	for _, item := range clientFruits {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(aggregation.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
