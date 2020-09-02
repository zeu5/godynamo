package main

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/zeu5/godynamo"
)

type Todo struct {
	ID   string `dynamodb:"paritionkey"`
	Item string `json:"item,omitempty"`
}

func (t *Todo) TableName() string {
	return "todo"
}

func main() {
	sess := session.Must(session.NewSession(&aws.Config{
		Region:   aws.String("ap-southeast-1"),
		Endpoint: aws.String("http://localhost:8000"),
	}))
	c := godynamo.NewClientWithSession(sess, nil)

	// err := c.CreateTable(&Todo{})
	// if err != nil {
	// 	fmt.Println(err)
	// 	os.Exit(1)
	// }

	t := &Todo{
		Item: "Nonsense",
		ID:   "3",
	}

	err := c.Table(t).Put(t)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	t1 := &Todo{
		ID: "3",
	}
	err = c.Table(t1).Get(t1)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println(t1)
}
