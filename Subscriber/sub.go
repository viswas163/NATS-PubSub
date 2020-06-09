package main

import (
	"encoding/json"
	"fmt"
	"runtime"

	"nats-pubsub/models"

	"github.com/nats-io/nats.go"
)

func main() {

	nc, _ := nats.Connect(nats.DefaultURL)

	nc.Subscribe("sensorData", func(msg *nats.Msg) {
		msgObj := []models.Message{}
		json.Unmarshal(msg.Data, &msgObj)
		fmt.Println(msgObj)
	})
	nc.Flush()

	fmt.Println("Hello from subscriber")

	runtime.Goexit()

}
