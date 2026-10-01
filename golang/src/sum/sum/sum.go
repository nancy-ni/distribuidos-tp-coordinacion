package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/utils"
)

const FIXED_SEED = "42"

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
	myRoutingKey      string
	inputQueue        middleware.Middleware
	outputExchange    middleware.Middleware
	controlQueue      middleware.Middleware
	controlExchange   middleware.Middleware
	clientStates      map[uint64]*ClientStateManager
	lock              sync.Mutex
	aggregationAmount int
	aggregationPrefix string
	sumAmount         int
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, outputExchange, err := initMiddlewaresForData(config, connSettings)
	if err != nil {
		return nil, err
	}

	controlQueue, controlExchange, err := initMiddlewaresForControl(config, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		return nil, err
	}

	myRoutingKey := utils.GenerateRoutingKey(config.SumPrefix, config.Id)
	return &Sum{
		myRoutingKey:    myRoutingKey,
		inputQueue:      inputQueue,
		outputExchange:  outputExchange,
		controlQueue:    controlQueue,
		controlExchange: controlExchange,

		clientStates: map[uint64]*ClientStateManager{},

		lock:              sync.Mutex{},
		aggregationAmount: config.AggregationAmount,
		aggregationPrefix: config.AggregationPrefix,
		sumAmount:         config.SumAmount,
	}, nil
}

func initMiddlewaresForData(config SumConfig, connSettings middleware.ConnSettings) (middleware.Middleware, middleware.Middleware, error) {
	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, nil, err
	}
	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, []string{}, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, nil, err
	}
	return inputQueue, outputExchange, nil
}

func initMiddlewaresForControl(config SumConfig, connSettings middleware.ConnSettings) (middleware.Middleware, middleware.Middleware, error) {
	controlQueue, err := middleware.CreateQueueMiddleware(config.ControlQueue, connSettings)
	if err != nil {
		return nil, nil, err
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
		controlQueue.Close()
		return nil, nil, err
	}
	myRoutingKey := utils.GenerateRoutingKey(config.SumPrefix, config.Id)
	if err := controlQueue.Bind(config.SumPrefix, myRoutingKey); err != nil {
		controlQueue.Close()
		controlExchange.Close()
		return nil, nil, err
	}
	return controlQueue, controlExchange, nil
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

	sum.handleShutdown(signalChannel)
}

func (sum *Sum) handleShutdown(signalChannel chan os.Signal) {
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

	clientId, fruitRecords, messageType, _, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if messageType == inner.Eof {
		if err := sum.handleOriginalEofMessage(msg, clientId); err != nil {
			slog.Error("While handling original EOF message", "err", err)
		}
		return
	}

	if err := sum.handleDataMessage(clientId, fruitRecords); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

func (sum *Sum) handleDataMessage(clientId uint64, fruitRecords []fruititem.FruitItem) error {
	defer sum.lock.Unlock()
	sum.lock.Lock()

	clientStateManager := sum.getClientStateManager(clientId)
	clientStateManager.RegisterFruits(fruitRecords)

	if clientStateManager.HasCoordinator() {
		sum.flushClientData(clientId)

		coordinatorRoutingKey := clientStateManager.GetCoordinator()
		localProcessed := clientStateManager.GetLocalProcessedCount()
		sum.sendReportToCoordinator(clientId, coordinatorRoutingKey, localProcessed)

		clientStateManager.ResetClientState()
	}
	return nil
}

func (sum *Sum) handleOriginalEofMessage(msg middleware.Message, clientId uint64) error {
	slog.Info("Received End Of Records message")
	defer sum.lock.Unlock()

	_, _, _, extraParam, _ := inner.DeserializeMessage(&msg)
	clientTotalSent, _ := strconv.Atoi(extraParam)

	sum.lock.Lock()
	clientStateManager := sum.getClientStateManager(clientId)
	clientStateManager.SetCoordinator(sum.myRoutingKey, true, clientTotalSent)
	if err := sum.flushClientData(clientId); err != nil {
		return err
	}
	propagatedEofMsg, err := inner.SerializeMessage(clientId, []fruititem.FruitItem{}, inner.Eof, sum.myRoutingKey)
	if err == nil {
		sum.controlExchange.Send(*propagatedEofMsg)
	}

	if clientStateManager.AllMessagesProcessed() {
		sum.sendAckToFollowers(clientId)
		sum.sendAllEofs(clientId)
	}
	return nil
}

func (sum *Sum) handleControlMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientId, _, messageType, extraParam, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	switch messageType {
	case inner.Eof:
		sum.handlePropagatedEof(clientId, extraParam)
	case inner.Report:
		sum.handleReport(clientId, extraParam)
	case inner.Ack:
		sum.handleAck(clientId)
	}
}

func (sum *Sum) handlePropagatedEof(clientId uint64, extraParam string) {
	slog.Info("Received Propagated End Of Records message")
	defer sum.lock.Unlock()

	coordinatorRoutingKey := extraParam
	clientStateManager := sum.getClientStateManager(clientId)

	sum.lock.Lock()
	clientStateManager.SetCoordinator(coordinatorRoutingKey, false)
	sum.flushClientData(clientId)
	localProcessed := clientStateManager.GetLocalProcessedCount()
	sum.sendReportToCoordinator(clientId, coordinatorRoutingKey, localProcessed)

	clientStateManager.ResetClientState()
}

func (sum *Sum) handleReport(clientId uint64, extraParam string) {
	slog.Info("Received Processed Messages Report message")
	defer sum.lock.Unlock()
	processedCount, _ := strconv.Atoi(extraParam)

	sum.lock.Lock()
	clientStateManager := sum.getClientStateManager(clientId)
	clientStateManager.UpdateTotalProcessedMessages(processedCount)

	if clientStateManager.AllMessagesProcessed() {
		sum.sendAckToFollowers(clientId)
		sum.sendAllEofs(clientId)
	}
}

func (sum *Sum) handleAck(clientId uint64) {
	slog.Info("Received Ack message")
	defer sum.lock.Unlock()
	sum.lock.Lock()
	sum.sendAllEofs(clientId)
}

func (sum *Sum) flushClientData(clientId uint64) error {
	if clientStateManager, ok := sum.clientStates[clientId]; ok {
		clientFruits := clientStateManager.GetFruitItems()
		for fruit, item := range clientFruits {
			targetNode := sum.getTargetAggregationNode(clientId, fruit)
			routingKey := utils.GenerateRoutingKey(sum.aggregationPrefix, targetNode)

			fruitRecord := []fruititem.FruitItem{item}
			message, err := inner.SerializeMessage(clientId, fruitRecord, inner.Data)
			if err != nil {
				slog.Debug("While serializing message", "err", err)
				return err
			}
			if err := sum.outputExchange.SendWithKey(*message, routingKey); err != nil {
				slog.Debug("While sending message", "err", err)
				return err
			}
		}
	}
	return nil
}

func (sum *Sum) sendReportToCoordinator(clientId uint64, coordinatorRoutingKey string, localProcessed int) {
	localProcessedStr := strconv.Itoa(localProcessed)
	reportMessage, err := inner.SerializeMessage(clientId, []fruititem.FruitItem{}, inner.Report, localProcessedStr)
	if err != nil {
		slog.Debug("While serializing Report message", "err", err)
		return
	}
	err = sum.controlExchange.SendWithKey(*reportMessage, coordinatorRoutingKey)
	if err != nil {
		slog.Debug("While sending Report message", "err", err)
		return
	}
}

func (sum *Sum) sendAckToFollowers(clientId uint64) {
	ackMessage, err := inner.SerializeMessage(clientId, []fruititem.FruitItem{}, inner.Ack)
	if err != nil {
		slog.Debug("While serializing Ack message", "err", err)
		return
	}
	if err := sum.controlExchange.Send(*ackMessage); err != nil {
		slog.Debug("While sending Ack message", "err", err)
		return
	}
}

func (sum *Sum) sendAllEofs(clientId uint64) error {
	eofMessage := []fruititem.FruitItem{}
	message, err := inner.SerializeMessage(clientId, eofMessage, inner.Eof)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	for i := 0; i < sum.aggregationAmount; i++ {
		routingKey := fmt.Sprintf("%s_%d", sum.aggregationPrefix, i)
		if err := sum.outputExchange.SendWithKey(*message, routingKey); err != nil {
			slog.Debug("While sending EOF message", "err", err)
			return err
		}
	}

	delete(sum.clientStates, clientId)
	return nil
}

func (sum *Sum) getTargetAggregationNode(clientId uint64, fruitName string) int {
	h := fnv.New64a()
	h.Write([]byte(FIXED_SEED))
	fmt.Fprintf(h, "-%d-%s", clientId, fruitName)

	hashValue := h.Sum64()
	return int(hashValue % uint64(sum.aggregationAmount))
}

func (sum *Sum) getClientStateManager(clientId uint64) *ClientStateManager {
	if _, ok := sum.clientStates[clientId]; !ok {
		sum.clientStates[clientId] = NewClientStateManager(clientId)
	}
	return sum.clientStates[clientId]
}
