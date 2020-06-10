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
		str := ""
		for i, m := range msgObj {
			avg = ((avg * float64(i)) + m.Value) / (float64(i) + 1)
			m.InsertRaw()
			m.InsertAvg()
			str += fmt.Sprintf("%.1f, ", m.Value)
		}
		models.InsertSensorsAvg(avg, msgObj[0].Timestamp)
		fmt.Println("Success! Received :", str, "from publisher.")
	})
	nc.Flush()

	fmt.Println("Hello from subscriber")
	fmt.Scanln()

	exit()
}

func exit() {
	db.Close()
}
