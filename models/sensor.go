package models

import (
	"fmt"
	"math"
	"math/rand"
	"nats-pubsub/db"
	"nats-pubsub/util"
	"time"

	_ "github.com/lib/pq" // pq library for PostgreSQL DB
)

const (
	minSensorValue, maxSensorValue = 20.0, 60.0
)

// Sensor : The entity representation for the message format
type Sensor struct {
	Name      string  `json:"name"`
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

// CreateSensor : Creates the sensor with given name
func CreateSensor(name string) Sensor {
	s := Sensor{}
	s.Name = name
	s.Timestamp = time.Now().Unix()
	s.Value = minSensorValue + rand.Float64()*(maxSensorValue-minSensorValue)
	s.Value = math.Round(s.Value*10) / 10
	return s
}

// InsertRaw : Inserts the sensor into the sensors_raw_values table
func (s *Sensor) InsertRaw() error {
	var lastInsertID int
	err := db.GetInstance().QueryRow(`INSERT INTO sensors_raw_values(sensor_id, value, created_on)
     VALUES ((SELECT id FROM sensors WHERE name = $1), $2, $3) RETURNING id;`,
		s.Name, s.Value, time.Unix(s.Timestamp, 0)).Scan(&lastInsertID)
	util.PanicErr(err)
	fmt.Println("Inserted to raw table in db! Last inserted id =", lastInsertID)
	return err
}

// InsertAvg : Inserts the avg data for each sensor into the sensors_avg_values table
func (s *Sensor) InsertAvg() error {
	var lastInsertID int
	err := db.GetInstance().QueryRow(`
	INSERT INTO sensors_avg_values(sensor_id, avg_value, last_updated) 
		SELECT sensor_id, ROUND(AVG(value)::NUMERIC,2), MAX(created_on) FROM sensors_raw_values 
				WHERE sensor_id = (SELECT id FROM sensors WHERE name = $1) GROUP BY sensor_id
	ON CONFLICT (sensor_id) DO UPDATE SET avg_value = EXCLUDED.avg_value, last_updated = EXCLUDED.last_updated 
	RETURNING id;`, s.Name).Scan(&lastInsertID)
	util.PanicErr(err)
	fmt.Println("Inserted to avg table in db! Last upserted id =", lastInsertID)
	return err
}

// InsertSensorsAvg : Inserts the avg data for all sensors to sensors_avg table
func InsertSensorsAvg(avg float64, createdOn int64) error {
	var lastInsertID int
	err := db.GetInstance().QueryRow(`INSERT INTO sensors_avg(avg_value, created_on) VALUES (ROUND($1::NUMERIC,2), $2)
	RETURNING id;`, avg, time.Unix(createdOn, 0)).Scan(&lastInsertID)
	util.PanicErr(err)
	fmt.Printf("Inserted to sensors avg table! Last inserted avg = %.1f\n", avg)
	return err
}
