package models

import (
	"fmt"
	"nats-pubsub/db"
	"nats-pubsub/util"
	"time"

	_ "github.com/lib/pq" // pq library for PostgreSQL DB
)

// Sensor : The entity representation for the message format
type Sensor struct {
	Name      string  `json:"name"`
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

// InsertRaw : Inserts the sensor into the sensors_raw_values table
func (s *Sensor) InsertRaw() error {
	var lastInsertID int
	err := db.GetInstance().QueryRow(`INSERT INTO sensors_raw_values(sensor_id, value, created_on)
     VALUES ((SELECT id FROM sensors WHERE name = $1), $2, $3) returning id;`,
		s.Name, s.Value, time.Unix(s.Timestamp, 0)).Scan(&lastInsertID)
	util.PanicErr(err)
	fmt.Println("Insert raw success! last inserted id =", lastInsertID)
	return err
}

// InsertAvg : Inserts the sensor avg data into the sensors_avg_values table
func (s *Sensor) InsertAvg() error {
	var lastInsertID int
	err := db.GetInstance().QueryRow(`insert into sensors_avg_values(sensor_id, avg_value, last_updated) 
	select sensor_id, round(avg(value)::numeric,2), MAX(created_on) 
		from sensors_raw_values where sensor_id = (SELECT id FROM sensors WHERE name = $1) group by sensor_id
		ON CONFLICT (sensor_id) 
		DO UPDATE SET avg_value = excluded.avg_value, last_updated = excluded.last_updated
			   returning id;`,
		s.Name).Scan(&lastInsertID)
	util.PanicErr(err)
	fmt.Println("Insert avg success! Last inserted id =", lastInsertID)
	return err
}