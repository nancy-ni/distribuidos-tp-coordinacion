package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	ControlQueue      string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	inputQueue            middleware.Middleware
	outputExchange        middleware.Middleware
	controlQueue          middleware.Middleware
	controlExchange       middleware.Middleware
	fruitItemPerClientMap map[uint64]map[string]fruititem.FruitItem
	clientChannels        map[uint64]chan struct{}
	lock                  sync.Mutex
	aggregationAmount     int
	aggregationPrefix     string
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}
	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, []string{}, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	controlQueue, err := middleware.CreateQueueMiddleware(config.ControlQueue, connSettings)
	if err != nil {
		return nil, err
	}

	controlExchangeRouteKeys := make([]string, 0, config.SumAmount-1)
	for i := range config.SumAmount {
		if i == config.Id {
			continue
		}
		controlExchangeRouteKeys = append(controlExchangeRouteKeys, fmt.Sprintf("%s_%d", config.SumPrefix, i))
	}
	controlExchange, err := middleware.CreateExchangeMiddleware(config.SumPrefix, controlExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		controlQueue.Close()
		return nil, err
	}

	myRoutingKey := fmt.Sprintf("%s_%d", config.SumPrefix, config.Id)
	if err := controlQueue.Bind(config.SumPrefix, myRoutingKey); err != nil {
		inputQueue.Close()
		outputExchange.Close()
		controlQueue.Close()
		controlExchange.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:            inputQueue,
		outputExchange:        outputExchange,
		controlQueue:          controlQueue,
		controlExchange:       controlExchange,
		fruitItemPerClientMap: map[uint64]map[string]fruititem.FruitItem{},
		clientChannels:        map[uint64]chan struct{}{},
		lock:                  sync.Mutex{},
		aggregationAmount:     config.AggregationAmount,
		aggregationPrefix:     config.AggregationPrefix,
	}, nil
}

func (sum *Sum) Run() {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

	go func() {
		sum.controlQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			sum.handleControlMessage(msg, ack, nack)
		})
	}()
	go func() {
		sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			sum.handleMessage(msg, ack, nack)
		})
	}()

	sig := <-signalChannel
	slog.Info(fmt.Sprintf("Received %v signal for Sum", sig))

	sum.inputQueue.Close()
	sum.controlQueue.Close()
	sum.outputExchange.Close()
	sum.controlExchange.Close()
	slog.Info("Sum Gracefully Shut Down")
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientId, fruitRecords, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		if err := sum.propagateEof(msg); err != nil {
			slog.Error("While propagating end of record message", "err", err)
		}
		if err := sum.handleEndOfRecordMessage(clientId); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	if err := sum.handleDataMessage(clientId, fruitRecords); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

func (sum *Sum) handleEndOfRecordMessage(clientId uint64) error {
	slog.Info("Received End Of Records message")
	outputExchange := sum.outputExchange.(*middleware.ExchangeMiddleware)

	sum.lock.Lock()
	clientFruits, ok := sum.fruitItemPerClientMap[clientId]
	if ok {
		for fruit, item := range clientFruits {
			targetNode := sum.getTargetAggregationNode(clientId, fruit)
			routingKey := fmt.Sprintf("%s_%d", sum.aggregationPrefix, targetNode)

			fruitRecord := []fruititem.FruitItem{item}
			message, err := inner.SerializeMessage(clientId, fruitRecord)
			if err != nil {
				slog.Debug("While serializing message", "err", err)
				return err
			}
			if err := outputExchange.SendWithKey(*message, routingKey); err != nil {
				slog.Debug("While sending message", "err", err)
				return err
			}
		}
		delete(sum.fruitItemPerClientMap, clientId)
	}
	sum.lock.Unlock()

	eofMessage := []fruititem.FruitItem{}
	message, err := inner.SerializeMessage(clientId, eofMessage)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	for i := 0; i < sum.aggregationAmount; i++ {
		routingKey := fmt.Sprintf("%s_%d", sum.aggregationPrefix, i)
		_ = outputExchange.SendWithKey(*message, routingKey)
	}
	return nil
}

func (sum *Sum) handleDataMessage(clientId uint64, fruitRecords []fruititem.FruitItem) error {
	defer sum.lock.Unlock()

	sum.lock.Lock()
	if _, ok := sum.fruitItemPerClientMap[clientId]; !ok {
		sum.fruitItemPerClientMap[clientId] = map[string]fruititem.FruitItem{}
	}

	clientFruits := sum.fruitItemPerClientMap[clientId]
	for _, fruitRecord := range fruitRecords {
		_, ok := clientFruits[fruitRecord.Fruit]
		if ok {
			clientFruits[fruitRecord.Fruit] = clientFruits[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			clientFruits[fruitRecord.Fruit] = fruitRecord
		}
	}

	if clientChannel, ok := sum.clientChannels[clientId]; ok {
		delete(sum.clientChannels, clientId)
		close(clientChannel)
	}
	return nil
}

func (sum *Sum) propagateEof(msg middleware.Message) error {
	if err := sum.controlExchange.Send(msg); err != nil {
		slog.Debug("While propagating EOF message", "err", err)
		return err
	}
	return nil
}

func (sum *Sum) handleControlMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientId, _, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}
	if !isEof {
		slog.Debug("Unexpected Message")
		return
	}

	sum.lock.Lock()
	clientChannel := make(chan struct{})
	sum.clientChannels[clientId] = clientChannel
	sum.lock.Unlock()

	select {
	case <-clientChannel:
		break
	case <-time.After(2 * time.Second):
		sum.lock.Lock()
		close(clientChannel)
		delete(sum.clientChannels, clientId)
		sum.lock.Unlock()
		break
	}

	sum.handleEndOfRecordMessage(clientId)
}

func (sum *Sum) getTargetAggregationNode(clientId uint64, fruitName string) int {
	h := fnv.New64a()
	h.Write([]byte("42"))
	fmt.Fprintf(h, "-%d-%s", clientId, fruitName)

	hashValue := h.Sum64()
	return int(hashValue % uint64(sum.aggregationAmount))
}
