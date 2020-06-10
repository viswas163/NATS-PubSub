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
		for _, m := range msgObj {
			m.InsertRaw()
			m.InsertAvg()
		}
		fmt.Println(msgObj)
	})
	nc.Flush()

	fmt.Println("Hello from subscriber")
	fmt.Scanln()

	exit()
}

func exit() {
	db.Close()
}
