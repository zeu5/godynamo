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

func main() {
	sess := session.Must(session.NewSession(&aws.Config{
		Region:   aws.String("ap-southeast-1"),
		Endpoint: aws.String("http://localhost:8000"),
	}))
	c := godynamo.NewClientWithSession(sess, nil)

	err := c.CreateTable("todo", &Todo{})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	t := &Todo{
		Item: "First todo",
		ID:   "1",
	}

	err := c.Table("todo").Put().Bind(t).Execute()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	t1 := &Todo{
		ID: "1",
	}
	err = c.Table("todo").Get().Bind(t1).Execute(t1)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println(t1)
}
