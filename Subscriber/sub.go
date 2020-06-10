package main

import (
	"encoding/json"
	"fmt"

	"nats-pubsub/db"
	"nats-pubsub/models"

	"github.com/nats-io/nats.go"
)

func main() {

	db.Init()
	nc, _ := nats.Connect(nats.DefaultURL)

	nc.Subscribe("sensorData", func(msg *nats.Msg) {
		msgObj := []models.Sensor{}
		json.Unmarshal(msg.Data, &msgObj)
		avg := 0.0
		for i, m := range msgObj {
			avg = ((avg * float64(i)) + m.Value) / (float64(i) + 1)
			m.InsertRaw()
			m.InsertAvg()
		}
		models.InsertSensorsAvg(avg, msgObj[0].Timestamp)
		fmt.Printf("Success! Received %d items from publisher.\n\n", len(msgObj))
	})
	nc.Flush()

	fmt.Println("Hello from subscriber")
	fmt.Scanln()

	exit()
}

func exit() {
	db.Close()
}
