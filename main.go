package main

import (
	"context"
	"fmt"
	bson2 "go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/mgo.v2/bson"
	"log"
	"strings"
)

var ctx =  context.Background()

type employee struct {
	// bson for customize name field
	Name string `bson:"name"`
	Job_title string `bson:"job_title"`
	Age int `bson:"age"`
}
func connect() (*mongo.Database, error)  {
	clientOptions := options.Client()
	clientOptions.ApplyURI("mongodb://localhost:27017")
	// inisialisasi database from client to server
	client, err := mongo.NewClient(clientOptions)
	if err != nil {
		return nil, err
	}

	err = client.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return client.Database("belajar_golang"), nil
}
func insert() {
	db, err := connect()
	if err != nil {
		log.Fatal(err.Error())
	}

	_, err = db.Collection("employee").InsertOne(ctx, employee{"Sammi","Technical Architect", 19})
	if err != nil {
		log.Fatal(err.Error())
	}

	_, err = db.Collection("employee").InsertOne(ctx, employee{"Ayatullah", "Chief FInancial Officer", 20})
	if err != nil {
		log.Fatal(err.Error())
	}

	fmt.Println("success")
}
func find()  {
	db, err := connect()
	if err != nil {
		log.Fatal(err.Error())
	}
	// bson.M -> alias dari map[string]interface{}
	csr, err := db.Collection("employee").Find(ctx, bson.M{"name": "Gusnur"})
	if err != nil {
		log.Fatal(err.Error())
	}
	defer csr.Close(ctx)

	result := make([]employee, 0)
	for csr.Next(ctx) {
		var row employee
		err := csr.Decode(&row)
		if err != nil {
			log.Fatal(err.Error())
		}
		result = append(result, row)
	}
	if len(result) > 0 {
		fmt.Println("Name : " , result[0].Name)
		fmt.Println("Name : " , result[0].Job_title)
		fmt.Println("Name : " , result[0].Age)
	}
}
func update()  {
	db, err := connect()
	if err != nil {
		log.Fatal(err.Error())
	}

	var selector = bson.M{"name": "Aditya"}
	var changes = employee{"Gusnur","Chief Marketing Officer", 20}
	_, err = db.Collection("employee").UpdateOne(ctx, selector, bson.M{"$set": changes})
	if err != nil {
		log.Fatal(err.Error())
	}

	fmt.Println("Update success!")
}
func remove() {
	db, err := connect()
	if err != nil {
		log.Fatal(err.Error())
	}

	var selector = bson.M{"name": "Gusnur"}
	_, err = db.Collection("employee").DeleteOne(ctx,selector)
	if err != nil {
		log.Fatal(err.Error())
	}

	fmt.Println("Remove success!")
}
func aggregate() {
	db, err := connect()
	if err != nil {
		log.Fatal(err.Error())
	}

	pipeline := make([]bson.M, 0)
	err = bson2.UnmarshalExtJSON([]byte(strings.TrimSpace(
		`
			[
				{ "$group": {
					"_id": null,
					"Total": { "$sum": 1 }
				} },
				{ "$project": {
					"Total": 1,
					"_id": 0
				} }
			]
		`)), true, &pipeline)

	if err != nil {
		log.Fatal(err.Error())
	}

	csr, err := db.Collection("employee").Aggregate(ctx, pipeline)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer csr.Close(ctx)

	result := make([]bson.M, 0)
	for csr.Next(ctx) {
		var row bson.M
		err := csr.Decode(&row)
		if err != nil {
			log.Fatal(err.Error())
		}

		result = append(result, row)
	}

	if len(result) > 0 {
		fmt.Println("Total data :", result[0]["Total"])
	}
}


func main() {
	//insert()
	//update()
	//remove()
	//find()
	aggregate()
}