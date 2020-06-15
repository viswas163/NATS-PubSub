package main

import (
	"database/sql"
	"fmt"
	"log"
	db2 "nats-pubsub/db"
	"nats-pubsub/models"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq"
)

const (
	gophers = 20
	entries = 5000
)

var (
	db      *sql.DB
	maxTime = 0.0
)

func main() {
	db2.Init()
	testGoBatchDB()
	defer db2.Close()
}

func testGoBatchDB() {

	sStmt := "insert into sensors_raw_values (sensor_id, value, created_on) values %s"
	var wg sync.WaitGroup
	db = db2.GetInstance()
	valueStrings := make([]string, 0, entries)
	wg.Add(gophers)

	for i := 1; i <= entries; i++ {
		s := "(1, $" + strconv.Itoa(2*i-1) + ", $" + strconv.Itoa(2*i) + ")"
		valueStrings = append(valueStrings, s)
	}
	sStmt = fmt.Sprintf(sStmt, strings.Join(valueStrings, ","))

	tmt, err := db.Prepare(sStmt)
	if err != nil {
		log.Fatal("prep :", err)
	}
	defer tmt.Close()

	// run the insert function using go routines
	for i := 0; i < gophers; i++ {
		// spin up a gopher
		go gopher(i, &wg, tmt)
	}
	wg.Wait()

	fmt.Printf("Operation took %.2f sec", maxTime)

	fmt.Scanln()
}

func gopher(gopherID int, wg *sync.WaitGroup, stmt *sql.Stmt) {
	defer wg.Done()
	a := time.Now()
	fmt.Printf("Gopher Id: %v started now\n", gopherID)

	// Prep args
	valueArgs := []interface{}{}
	for i := 1; i <= entries; i++ {
		valueArgs = append(valueArgs, 23.4, a)
	}

	// Exec sql stmt with args
	res, err := stmt.Exec(valueArgs...)
	if err != nil || res == nil {
		log.Fatal("exec :", err)
	}

	// Calc  print time taken for gopher
	dur := time.Now().Sub(a).Seconds()
	fmt.Printf("Gopher Id: %v completed! || Took: %v sec\n", gopherID, dur)
	if maxTime < dur {
		maxTime = dur
	}
}

func testPQCopyDB() {
	dbb := db2.GetInstance()
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
