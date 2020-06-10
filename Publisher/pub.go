package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"nats-pubsub/models"

	"github.com/nats-io/nats.go"
)

const (
	minWaitTime, maxWaitTime = 8000, 12000
)

var (
	nc *nats.Conn
)

func main() {
	nc, _ = nats.Connect(nats.DefaultURL)

	go startPub()

	fmt.Println("Hello from publisher")
	fmt.Scanln()
}

func startPub() {
	subj := "sensorData"
	for {
		msg := getRandomSensorValues()
		publish(subj, msg)
		time.Sleep(time.Duration(rand.Intn(maxWaitTime-minWaitTime)+minWaitTime) * time.Millisecond)
	}
}

func getRandomSensorValues() []models.Sensor {
	msg := []models.Sensor{}
	for i := 1; i < 4; i++ {
		s := models.CreateSensor("Sensor" + strconv.Itoa(i))
		msg = append(msg, s)
	}
	return msg
}

func publish(subj string, msg []models.Sensor) {

	msgBody, err := json.Marshal(msg)
	if err != nil {
		fmt.Println("Error marshaling to bytea : ", err)
	}

	if err := nc.Publish(subj, msgBody); err != nil {
		log.Fatal(err)
	}
	nc.Flush()
	str := ""
	for _, m := range msg {
		str += fmt.Sprintf("%.1f, ", m.Value)
	}

	log.Println("Published :", str, "to :", subj)
}
