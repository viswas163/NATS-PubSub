package main

import (
	"fmt"
	"log"
	"nats-pubsub/db"
	"nats-pubsub/models"
	"strings"
	"time"

	"github.com/lib/pq"
)

const (
	gophers = 10
	entries = 100
)

func main() {
	db.Init()
	testGoBatchDB()
	db.Close()
}

func testGoBatchDB() {
	// create string to pass
	var sStmt string = "insert into sensors_raw_values (sensor_id, value, created_on) values "

	// run the insert function using 10 go routines
	for i := 0; i < gophers; i++ {
		// spin up a gopher
		go gopherSprintf(i, sStmt)
	}

	// this is a simple way to keep a program open
	// the go program will close when a key is pressed
	var input string
	fmt.Scanln(&input)
}

func gopherExec(gopherID int, sStmt string) {

	db := db.GetInstance()

	stmt, err := db.Prepare(sStmt)
	if err != nil {
		log.Fatal(err)
	}
	a := time.Now()
	fmt.Printf("Gopher Id: %v started now\n", gopherID)

	for i := 0; i < entries; i++ {
		res, err := stmt.Exec(23.4, time.Now())
		if err != nil || res == nil {
			log.Fatal(err)
		}
	}

	stmt.Close()

	fmt.Printf("Gopher Id: %v completed! || Took: %v sec\n", gopherID, time.Now().Sub(a).Seconds())
}

func gopherAppend(gopherID int, sStmt string) {

	db := db.GetInstance()

	a := time.Now()
	fmt.Printf("Gopher Id: %v started now\n", gopherID)
	vals := []interface{}{}
	f := 23.4
	for i := 0; i < entries; i++ {
		sStmt += "(1,?,?),"
		vals = append(vals, f, a)
	}
	sStmt = strings.TrimSuffix(sStmt, ",")
	// fmt.Println(sStmt)
	stmt, err := db.Prepare(sStmt)
	if err != nil {
		fmt.Print("prep")
		log.Fatal(err)
	}
	res, err := stmt.Exec(vals...)
	if err != nil || res == nil {
		fmt.Print("exec")
		log.Fatal(err)
	}

	stmt.Close()

	fmt.Printf("Gopher Id: %v completed! || Took: %v sec\n", gopherID, time.Now().Sub(a).Seconds())
}

func gopherSprintf(gopherID int, sStmt string) {
	db := db.GetInstance()

	a := time.Now()
	fmt.Printf("Gopher Id: %v started now\n", gopherID)

	sStmt += "%s"
	valueStrings := make([]string, 0, entries)
	valueArgs := []interface{}{}
	for i := 0; i < entries; i++ {
		valueStrings = append(valueStrings, "(1, ?, ?)")
		valueArgs = append(valueArgs, 23.4, a)
	}
	stmt := fmt.Sprintf(sStmt, strings.Join(valueStrings, ","))
	fmt.Println(len(valueArgs))
	_, err := db.Exec(stmt, valueArgs...)
	if err != nil {
		fmt.Print("exec")
		log.Fatal(err)
	}

	fmt.Printf("Gopher Id: %v completed! || Took: %v sec\n", gopherID, time.Now().Sub(a).Seconds())
}

func testPQCopyDB() {
	dbb := db.GetInstance()
	dbb.SetMaxIdleConns(10)
	dbb.SetMaxOpenConns(10)
	dbb.SetConnMaxLifetime(0)
	a := time.Now()
	txn, err := dbb.Begin()
	if err != nil {
		log.Fatal(err)
	}
	stmt, _ := txn.Prepare(pq.CopyIn("sensors_raw_values", "sensor_id", "value", "created_on")) // MessageDetailRecord is the table name
	m := models.CreateSensor("Sensor1")
	mList := make([]models.Sensor, 0, 100000)
	for i := 0; i < 100000; i++ {
		fmt.Println(i)
		mList = append(mList, m)
	}
	fmt.Println(m)
	for _, m := range mList {
		_, err := stmt.Exec(1, m.Value, time.Unix(m.Timestamp, 0))
		if err != nil {
			log.Fatal(err)
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}
	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}
	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}
	delta := time.Now().Sub(a)
	fmt.Println(delta.Seconds())
	fmt.Println("Program finished successfully")
}
