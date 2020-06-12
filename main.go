package main

import (
	"fmt"
	"log"
	"nats-pubsub/db"
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
	maxTime = 0.0
)

func main() {
	db.Init()
	testGoBatchDB()
	db.Close()
}

func testGoBatchDB() {
	// create string to pass
	var sStmt string = "insert into sensors_raw_values (sensor_id, value, created_on) values "
	var wg sync.WaitGroup
	wg.Add(gophers)
	// run the insert function using 10 go routines
	for i := 0; i < gophers; i++ {
		// spin up a gopher
		go gopherSprintf(&wg, i, sStmt)
	}
	wg.Wait()
	fmt.Printf("Operation took %f sec", maxTime)

	// this is a simple way to keep a program open
	// the go program will close when a key is pressed
	var input string
	fmt.Scanln(&input)
}

func gopherAppend(wg *sync.WaitGroup, gopherID int, sStmt string) {
	defer wg.Done()

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

func gopherSprintf(wg *sync.WaitGroup, gopherID int, sStmt string) {
	defer wg.Done()

	db := db.GetInstance()

	a := time.Now()
	fmt.Printf("Gopher Id: %v started now\n", gopherID)

	sStmt += "%s"
	valueStrings := make([]string, 0, entries)
	valueArgs := []interface{}{}
	for i := 1; i <= entries; i++ {
		s := "(1, $" + strconv.Itoa(2*i-1) + ", $" + strconv.Itoa(2*i) + ")"
		valueStrings = append(valueStrings, s)
		valueArgs = append(valueArgs, 23.4, a)
	}
	stmt := fmt.Sprintf(sStmt, strings.Join(valueStrings, ","))
	// fmt.Println(stmt)
	tmt, err := db.Prepare(stmt)
	if err != nil {
		fmt.Print("prep")
		log.Fatal(err)
	}
	defer tmt.Close()

	// fmt.Println(stmt)
	// fmt.Println(valueArgs[:10])
	res, err := tmt.Exec(valueArgs...)
	if err != nil || res == nil {
		fmt.Print("exec")
		log.Fatal(err)
	}
	dur := time.Now().Sub(a).Seconds()
	fmt.Printf("Gopher Id: %v completed! || Took: %v sec\n", gopherID, dur)
	if maxTime < dur {
		maxTime = dur
	}
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
