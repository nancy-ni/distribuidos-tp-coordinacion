package inner

import (
	"encoding/json"
	"errors"
	"strconv"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

const (
	Data   = "DATA"
	Eof    = "EOF"
	Report = "REPORT"
	Ack    = "ACK"
)

func serializeJson(message []interface{}) ([]byte, error) {
	return json.Marshal(message)
}

func deserializeJson(message []byte) ([]interface{}, error) {
	var data []interface{}
	if err := json.Unmarshal(message, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func SerializeMessage(clientId uint64, fruitRecords []fruititem.FruitItem, messageType string, extraParam ...string) (*middleware.Message, error) {
	data := []interface{}{}
	for _, fruitRecord := range fruitRecords {
		datum := []interface{}{
			fruitRecord.Fruit,
			fruitRecord.Amount,
		}
		data = append(data, datum)
	}

	clientIdStr := strconv.FormatUint(clientId, 10)
	fullMessage := []interface{}{
		clientIdStr,
		messageType,
		data,
	}
	if len(extraParam) > 0 {
		fullMessage = append(fullMessage, extraParam[0])
	}

	body, err := serializeJson(fullMessage)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeMessage(message *middleware.Message) (uint64, []fruititem.FruitItem, string, string, error) {
	data, err := deserializeJson([]byte((*message).Body))
	if err != nil {
		return 0, nil, "", "", err
	}

	if len(data) < 2 {
		return 0, nil, "", "", errors.New("Message is empty array")
	}

	clientIdStr, ok := data[0].(string)
	if !ok {
		return 0, nil, "", "", errors.New("Invalid Client ID")
	}
	clientId, err := strconv.ParseUint(clientIdStr, 10, 64)
	if err != nil {
		return 0, nil, "", "", errors.New("Invalid Client ID")
	}

	messageType, ok := data[1].(string)
	if !ok {
		return 0, nil, "", "", errors.New("Invalid Message Type")
	}

	extraParam := ""
	if len(data) >= 4 {
		if val, ok := data[3].(string); ok {
			extraParam = val
		}
	}

	fruitRecords := []fruititem.FruitItem{}
	if len(data) >= 3 && data[2] != nil {
		recordsData, ok := data[2].([]interface{})
		if ok {
			for _, datum := range recordsData {
				fruitPair, ok := datum.([]interface{})
				if !ok {
					return 0, nil, "", "", errors.New("Datum is not an array")
				}

				fruit, ok := fruitPair[0].(string)
				if !ok {
					return 0, nil, "", "", errors.New("Datum is not a (fruit, amount) pair")
				}

				fruitAmount, ok := fruitPair[1].(float64)
				if !ok {
					return 0, nil, "", "", errors.New("Datum is not a (fruit, amount) pair")
				}

				fruitRecord := fruititem.FruitItem{Fruit: fruit, Amount: uint32(fruitAmount)}
				fruitRecords = append(fruitRecords, fruitRecord)
			}
		}
	}

	return clientId, fruitRecords, messageType, extraParam, nil
}
