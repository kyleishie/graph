package main

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"graph"
	"time"
)

type Person struct {
	Email     string
	FirstName string
	LastName  string
}

type Friendship struct {
	Start time.Time
}

func main() {

	var dynamodbClient = dynamodb.New(session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})))

	g := graph.New("graph1", dynamodbClient)

	person1, err := g.AddVertex("PERSON", "person1", Person{
		Email:     "person1",
		FirstName: "Person",
		LastName:  "1",
	})
	if err != nil {
		panic(err.Error())
	}

	person2, err := g.AddVertex("PERSON", "person2", Person{
		Email:     "person2",
		FirstName: "Person",
		LastName:  "2",
	})
	if err != nil {
		panic(err.Error())
	}

	/// person1 -> friendship -> person2
	/// person2 <- friendship <- person1 (Creates Mirror)
	friendship, err := g.AddEdge(person1, "friendship", person2, Friendship{
		Start: time.Now(),
	})
	if err != nil {
		panic(err.Error())
	}

	fmt.Println(friendship)

	v, err := g.GetVertex("PERSON", "person1")
	if err != nil {
		panic(err)
	}

	fmt.Println(v)

	var p1 Person
	if err := v.GetAttributesAs(&p1); err != nil {
		panic(err)
	}

	fmt.Println(p1)

	jsonData, err := json.Marshal(v)
	if err != nil {
		panic(err.Error())
	}

	fmt.Println(string(jsonData))

}
