package join

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	inputQueue            middleware.Middleware
	outputQueue           middleware.Middleware
	fruitItemPerClientMap map[uint64]map[string]fruititem.FruitItem
	recvEofCountMap       map[uint64]int
	aggregationAmount     int
	topSize               int
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		inputQueue:            inputQueue,
		outputQueue:           outputQueue,
		fruitItemPerClientMap: map[uint64]map[string]fruititem.FruitItem{},
		recvEofCountMap:       map[uint64]int{},
		aggregationAmount:     config.AggregationAmount,
		topSize:               config.TopSize,
	}, nil
}

func (join *Join) Run() {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

	go func() {
		join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			join.handleMessage(msg, ack, nack)
		})
	}()

	sig := <-signalChannel
	slog.Info(fmt.Sprintf("Received %v signal for Join", sig))

	join.inputQueue.Close()
	join.outputQueue.Close()
	slog.Info("Join Gracefully Shut Down")
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientId, localTop, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		if err := join.handleEndOfRecordsMessage(clientId); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	join.handleDataMessage(clientId, localTop)
}

func (join *Join) handleDataMessage(clientId uint64, localTop []fruititem.FruitItem) {
	if _, ok := join.fruitItemPerClientMap[clientId]; !ok {
		join.fruitItemPerClientMap[clientId] = map[string]fruititem.FruitItem{}
	}

	clientFruits := join.fruitItemPerClientMap[clientId]
	for _, fruitRecord := range localTop {
		if _, ok := clientFruits[fruitRecord.Fruit]; ok {
			clientFruits[fruitRecord.Fruit] = clientFruits[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			clientFruits[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (join *Join) handleEndOfRecordsMessage(clientId uint64) error {
	join.recvEofCountMap[clientId]++
	receivedEofs := join.recvEofCountMap[clientId]
	if receivedEofs < join.aggregationAmount {
		return nil
	}

	fruitTopRecords := []fruititem.FruitItem{}
	clientFruits, ok := join.fruitItemPerClientMap[clientId]
	if ok {
		fruitTopRecords = join.buildGlobalTop(clientFruits)
		delete(join.fruitItemPerClientMap, clientId)
	}
	delete(join.recvEofCountMap, clientId)

	message, err := inner.SerializeMessage(clientId, fruitTopRecords)
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := join.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}
	return nil
}

func (join *Join) buildGlobalTop(clientFruits map[string]fruititem.FruitItem) []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0, len(clientFruits))
	for _, item := range clientFruits {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(join.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
