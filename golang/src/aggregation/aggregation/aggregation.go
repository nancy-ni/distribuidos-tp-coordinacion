package aggregation

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/utils"
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
	recvEofCountMap       map[uint64]int
	topSize               int
	sumAmount             int
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}
	inputExchangeRoutingKey := []string{utils.GenerateRoutingKey(config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		outputQueue:           outputQueue,
		inputExchange:         inputExchange,
		fruitItemPerClientMap: map[uint64]map[string]fruititem.FruitItem{},
		recvEofCountMap:       map[uint64]int{},
		topSize:               config.TopSize,
		sumAmount:             config.SumAmount,
	}, nil
}

func (aggregation *Aggregation) Run() {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

	go func() {
		aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			aggregation.handleMessage(msg, ack, nack)
		})
	}()

	aggregation.handleShutdown(signalChannel)
}

func (aggregation *Aggregation) handleShutdown(signalChannel chan os.Signal) {
	sig := <-signalChannel
	slog.Info(fmt.Sprintf("Received %v signal for Aggregation", sig))

	aggregation.outputQueue.Close()
	aggregation.inputExchange.Close()
	slog.Info("Aggregation Gracefully Shut Down")
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientId, fruitRecords, messageType, _, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if messageType == inner.Eof {
		if err := aggregation.handleEndOfRecordsMessage(clientId); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	aggregation.handleDataMessage(clientId, fruitRecords)
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientId uint64) error {
	aggregation.recvEofCountMap[clientId]++
	receivedEofs := aggregation.recvEofCountMap[clientId]
	if receivedEofs < aggregation.sumAmount {
		return nil
	}

	fruitTopRecords := aggregation.fetchFruitTopRecords(clientId)
	message, err := inner.SerializeMessage(clientId, fruitTopRecords, inner.Data)
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}

	return aggregation.sendEof(clientId)
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

func (aggregation *Aggregation) fetchFruitTopRecords(clientId uint64) []fruititem.FruitItem {
	fruitTopRecords := []fruititem.FruitItem{}
	clientFruits, ok := aggregation.fruitItemPerClientMap[clientId]
	if ok {
		fruitTopRecords = utils.BuildFruitTop(aggregation.topSize, clientFruits)
		delete(aggregation.fruitItemPerClientMap, clientId)
	}
	delete(aggregation.recvEofCountMap, clientId)
	return fruitTopRecords
}

func (aggregation *Aggregation) sendEof(clientId uint64) error {
	eofMessage := []fruititem.FruitItem{}
	message, err := inner.SerializeMessage(clientId, eofMessage, inner.Eof)
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
