package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"gopkg.in/mgo.v2"
)

const (
	hosts      = "localhost:27017"
	database   = "sample_databases"
	username   = ""
	password   = ""
	collection = "TempJobs"
)

type Job1 struct {
	Network    string `json:"network"`
	DoctorName string `json:"doctorName"`
	Speciality string `json:"Sepeciality"`
	Startdate  string `json:"startdate"`
	Enddate    string `json:"enddate"`
}

type MongoStore struct {
	session *mgo.Session
}

var mongoStore = MongoStore{}

func main() {
	fmt.Println("Main started")
	go consumer()

	session := initialiseMongo()
	mongoStore.session = session

	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/jobs", jobsPostHandler).Methods("POST")

	log.Fatal(http.ListenAndServe(":9090", router))

}

func jobsPostHandler(w http.ResponseWriter, r *http.Request) {

	//Retrieve body from http request
	b, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		panic(err)
	}

	//Save data into Job struct
	var _job Job1
	err = json.Unmarshal(b, &_job)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	saveJobToKafka(_job)

	//Convert job struct into json
	jsonString, err := json.Marshal(_job)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	//Set content-type http header
	w.Header().Set("content-type", "application/json")

	//Send back data as response
	w.Write(jsonString)

}

func saveJobToKafka(job Job1) {

	fmt.Println("save to kafka")
	jsonString, _ := json.Marshal(job)
	jobString := string(jsonString)
	//fmt.Print(jobString)

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		panic(err)
	}

	topic := "Provider_Info"
	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(jobString),
	}, nil)

	fmt.Println("kafka producer is done with message: ", jobString)
	//p.Flush(5 * 1000)
}

func consumer() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "Provider_Info",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		fmt.Println("Consumer object creation time got some err")
		panic(err)
	}

	c.SubscribeTopics([]string{"Provider_Info", "^aRegex.*[Tt]opic"}, nil)
	for {
		time.Sleep(5 * time.Second)
		msg, err := c.ReadMessage(-1)

		if err == nil {
			fmt.Printf("Consumer Message received %s: %s\n", msg.TopicPartition, string(msg.Value))
			job := string(msg.Value)
			saveJobToMongo(job)
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}

//MongoDB Connection code
func initialiseMongo() (session *mgo.Session) {
	fmt.Println("MongoDB Connection code started")
	info := &mgo.DialInfo{
		Addrs:    []string{hosts},
		Timeout:  60 * time.Second,
		Database: database,
		Username: username,
		Password: password,
	}
	session, err := mgo.DialWithInfo(info)
	fmt.Println("MongoDB Connection session is done")
	if err != nil {
		panic(err)
	}
	return
}

// Saving data to mongodb code
func saveJobToMongo(jobString string) {

	fmt.Println("Save to MongoDB: ", jobString)

	col := mongoStore.session.DB(database).C(collection)
	var _job Job1
	b := []byte(jobString)
	err := json.Unmarshal(b, &_job)
	if err != nil {
		panic(err)
	}

	//Insert job into MongoDB
	errMongo := col.Insert(_job)
	if errMongo != nil {
		panic(errMongo)
	}

	fmt.Printf("Saved to MongoDB : %s", jobString)

}

/*

{
    "network": "100001",
    "doctorName": "tim",
    "Sepeciality": "Heart Surgery",
    "startdate": "2019-11-28",
    "enddate": "2019-11-29"
}

*/
