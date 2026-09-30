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
	myRoutingKey          string
	inputQueue            middleware.Middleware
	outputExchange        middleware.Middleware
	controlQueue          middleware.Middleware
	controlExchange       middleware.Middleware
	fruitItemPerClientMap map[uint64]map[string]fruititem.FruitItem

	clientTotalExpected     map[uint64]int
	clientProcessedCount    map[uint64]int
	clientCoordinators      map[uint64]string
	totalProcessedPerClient map[uint64]int

	lock              sync.Mutex
	aggregationAmount int
	aggregationPrefix string
	sumAmount         int
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
		myRoutingKey:          myRoutingKey,
		inputQueue:            inputQueue,
		outputExchange:        outputExchange,
		controlQueue:          controlQueue,
		controlExchange:       controlExchange,
		fruitItemPerClientMap: map[uint64]map[string]fruititem.FruitItem{},

		clientTotalExpected:     map[uint64]int{},
		clientProcessedCount:    map[uint64]int{},
		clientCoordinators:      map[uint64]string{},
		totalProcessedPerClient: map[uint64]int{},

		lock:              sync.Mutex{},
		aggregationAmount: config.AggregationAmount,
		aggregationPrefix: config.AggregationPrefix,
		sumAmount:         config.SumAmount,
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

	if _, ok := sum.totalProcessedPerClient[clientId]; !ok {
		sum.totalProcessedPerClient[clientId] = 0
	}
	sum.totalProcessedPerClient[clientId]++

	coordinatorRoutingKey, hasCoordinator := sum.clientCoordinators[clientId]
	if hasCoordinator {
		slog.Info(fmt.Sprintf("RECIBI DATA DESPUES DE EOF - CLIENT ID %d", clientId))
		sum.flushClientData(clientId)

		localProcessed := 0
		if processed, ok := sum.totalProcessedPerClient[clientId]; ok {
			localProcessed = processed
		}
		sum.sendReportToCoordinator(clientId, coordinatorRoutingKey, localProcessed)
		slog.Info(fmt.Sprintf("ENVIE REPORT DESPUES DE EOF - CLIENT ID %d", clientId))
	}

	sum.lock.Unlock()
	return nil
}

func (sum *Sum) handleOriginalEofMessage(msg middleware.Message, clientId uint64) error {
	slog.Info(fmt.Sprintf("RECIBI EOF ORIGINAL - CLIENT ID %d", clientId))

	_, _, _, extraParam, _ := inner.DeserializeMessage(&msg)
	clientTotalSent, _ := strconv.Atoi(extraParam)

	sum.lock.Lock()
	sum.clientTotalExpected[clientId] = clientTotalSent
	sum.clientCoordinators[clientId] = sum.myRoutingKey

	if err := sum.flushClientData(clientId); err != nil {
		sum.lock.Unlock()
		return err
	}

	localProcessed := 0
	if processed, ok := sum.totalProcessedPerClient[clientId]; ok {
		localProcessed = processed
	}
	sum.clientProcessedCount[clientId] += localProcessed
	currentTotal := sum.clientProcessedCount[clientId]
	targetTotal := sum.clientTotalExpected[clientId]

	propagatedEofMsg, err := inner.SerializeMessage(clientId, []fruititem.FruitItem{}, inner.Eof, sum.myRoutingKey)
	if err == nil {
		sum.controlExchange.Send(*propagatedEofMsg)
	}

	if currentTotal >= targetTotal {
		slog.Info("Raro que entre aca......")
		sum.sendAckToFollowers(clientId)
		sum.sendAllEofs(clientId)
	}

	sum.lock.Unlock()

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
		slog.Info(fmt.Sprintf("RECIBI EOF PROPAGADO - CLIENT ID %d", clientId))
		coordinatorRoutingKey := extraParam

		sum.lock.Lock()
		sum.clientCoordinators[clientId] = coordinatorRoutingKey
		sum.flushClientData(clientId)

		slog.Info(fmt.Sprintf("ENVIE MI DATA ACTUAL A AGGREGATORS - CLIENT ID %d", clientId))

		localProcessed := 0
		if processed, ok := sum.totalProcessedPerClient[clientId]; ok {
			localProcessed = processed
		}
		sum.sendReportToCoordinator(clientId, coordinatorRoutingKey, localProcessed)
		slog.Info(fmt.Sprintf("ENVIE REPORT A COORDINADOR - CLIENT ID %d", clientId))
		sum.lock.Unlock()

	case inner.Report:
		slog.Info(fmt.Sprintf("RECIBI REPORTE DE OTRO NODO SUM - CLIENT ID %d", clientId))
		processedCount, _ := strconv.Atoi(extraParam)

		sum.lock.Lock()
		sum.clientProcessedCount[clientId] += processedCount
		currentTotal := sum.clientProcessedCount[clientId]
		targetTotal := sum.clientTotalExpected[clientId]

		if currentTotal >= targetTotal {
			slog.Info(fmt.Sprintf("TODO CUADRA, ENVIANDO ACKS - CLIENT ID %d", clientId))
			slog.Info("Entra a este sendAcks......")
			sum.sendAckToFollowers(clientId)
			sum.sendAllEofs(clientId)
		}
		sum.lock.Unlock()
	case inner.Ack:
		sum.lock.Lock()
		slog.Info(fmt.Sprintf("RECIBI ACK - CLIENT ID %d", clientId))
		sum.sendAllEofs(clientId)
		sum.lock.Unlock()
	}
}

func (sum *Sum) flushClientData(clientId uint64) error {
	outputExchange := sum.outputExchange.(*middleware.ExchangeMiddleware)
	clientFruits, ok := sum.fruitItemPerClientMap[clientId]
	if ok {
		for fruit, item := range clientFruits {
			targetNode := sum.getTargetAggregationNode(clientId, fruit)
			routingKey := fmt.Sprintf("%s_%d", sum.aggregationPrefix, targetNode)

			fruitRecord := []fruititem.FruitItem{item}
			message, err := inner.SerializeMessage(clientId, fruitRecord, "DATA")
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
	return nil
}

func (sum *Sum) sendAllEofs(clientId uint64) error {
	outputExchange := sum.outputExchange.(*middleware.ExchangeMiddleware)
	eofMessage := []fruititem.FruitItem{}
	message, err := inner.SerializeMessage(clientId, eofMessage, inner.Eof)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	for i := 0; i < sum.aggregationAmount; i++ {
		routingKey := fmt.Sprintf("%s_%d", sum.aggregationPrefix, i)
		_ = outputExchange.SendWithKey(*message, routingKey)
	}

	delete(sum.clientTotalExpected, clientId)
	delete(sum.clientProcessedCount, clientId)
	delete(sum.clientCoordinators, clientId)
	slog.Info(fmt.Sprintf("ENVIE MIS EOFS - CLIENT ID %d", clientId))

	return nil
}

func (sum *Sum) sendReportToCoordinator(clientId uint64, coordinatorRoutingKey string, localProcessed int) {
	slog.Info(fmt.Sprintf("REPORTANDO %d NUEVOS MENSAJES", localProcessed))
	localProcessedStr := strconv.Itoa(localProcessed)
	reportMessage, err := inner.SerializeMessage(clientId, []fruititem.FruitItem{}, inner.Report, localProcessedStr)
	if err != nil {
		slog.Info(fmt.Sprintf("error REPORT DESPUES DE EOF - CLIENT ID %d", clientId))
		return
	}
	exchange := sum.controlExchange.(*middleware.ExchangeMiddleware)
	err = exchange.SendWithKey(*reportMessage, coordinatorRoutingKey)
	if err != nil {
		slog.Info(fmt.Sprintf("error2 REPORT DESPUES DE EOF - CLIENT ID %d", clientId))
		return
	}
	delete(sum.totalProcessedPerClient, clientId)
}

func (sum *Sum) sendAckToFollowers(clientId uint64) {
	ackMessage, err := inner.SerializeMessage(clientId, []fruititem.FruitItem{}, inner.Ack)
	if err != nil {
		return
	}
	_ = sum.controlExchange.Send(*ackMessage)
}

func (sum *Sum) getTargetAggregationNode(clientId uint64, fruitName string) int {
	h := fnv.New64a()
	h.Write([]byte("42"))
	fmt.Fprintf(h, "-%d-%s", clientId, fruitName)

	hashValue := h.Sum64()
	return int(hashValue % uint64(sum.aggregationAmount))
}
