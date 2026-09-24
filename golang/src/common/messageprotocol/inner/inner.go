package inner

import (
	"encoding/json"
	"errors"
	"strconv"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
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

func SerializeMessage(clientId uint64, fruitRecords []fruititem.FruitItem) (*middleware.Message, error) {
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
		data,
	}
	body, err := serializeJson(fullMessage)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeMessage(message *middleware.Message) (uint64, []fruititem.FruitItem, bool, error) {
	data, err := deserializeJson([]byte((*message).Body))
	if err != nil {
		return 0, nil, false, err
	}

	if len(data) < 1 {
		return 0, nil, false, errors.New("Message is empty array")
	}

	clientIdStr, ok := data[0].(string)
	if !ok {
		return 0, nil, false, errors.New("Invalid Client ID")
	}
	clientId, err := strconv.ParseUint(clientIdStr, 10, 64)
	if err != nil {
		return 0, nil, false, errors.New("Invalid Client ID")
	}

	if len(data) < 2 {
		return clientId, []fruititem.FruitItem{}, true, nil
	}

	recordsData, ok := data[1].([]interface{})
	if !ok {
		return 0, nil, false, errors.New("Invalid Records Data")
	}

	fruitRecords := []fruititem.FruitItem{}
	for _, datum := range recordsData {
		fruitPair, ok := datum.([]interface{})
		if !ok {
			return 0, nil, false, errors.New("Datum is not an array")
		}

		fruit, ok := fruitPair[0].(string)
		if !ok {
			return 0, nil, false, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitAmount, ok := fruitPair[1].(float64)
		if !ok {
			return 0, nil, false, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitRecord := fruititem.FruitItem{Fruit: fruit, Amount: uint32(fruitAmount)}
		fruitRecords = append(fruitRecords, fruitRecord)
	}

	return clientId, fruitRecords, len(fruitRecords) == 0, nil
}
