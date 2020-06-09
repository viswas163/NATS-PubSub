package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"runtime"
	"strconv"
	"time"

	"nats-pubsub/models"

	"github.com/nats-io/nats.go"
)

var (
	nc *nats.Conn
)

func main() {
	nc, _ = nats.Connect(nats.DefaultURL)

	go startPub()

	fmt.Println("Hello from publisher")

	runtime.Goexit()
}

func startPub() {
	min, max := 20.0, 60.0
	subj := "sensorData"
	i := 1
	for {
		msg := []models.Message{}
		for i := 1; i < 4; i++ {
			s := models.Message{}
			s.Name = "Sensor" + strconv.Itoa(i)
			s.Timestamp = time.Now().Unix()
			s.Value = min + rand.Float64()*(max-min)
			s.Value = math.Round(s.Value*10) / 10
			msg = append(msg, s)
		}
		publish(subj, msg)
		time.Sleep(time.Duration(rand.Intn(2000)+3000) * time.Millisecond)
		i++
	}
}

func publish(subj string, msg []models.Message) {

	msgBody, err := json.Marshal(msg)
	if err != nil {
		fmt.Println("Error marshaling to bytea : ", err)
	}

	if err := nc.Publish(subj, msgBody); err != nil {
		log.Fatal(err)
	}
	nc.Flush()
	log.Println("Published :", string(msgBody), " to :", subj)
}
