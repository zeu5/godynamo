package godynamo

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// TableItem interface that the model should implement
type TableItem interface {

	// TableName should return the name of the table that the model should store the data in
	TableName() string
}

// Table interface for interactive with dynamo
type Table struct {
	client    *Client
	tableName string
}

// Put function to create a new dynamo document
func (t *Table) Put(i TableItem) error {
	return t.PutWithContext(t.client.Ctx(), i)
}

// PutWithContext function to create a new dynamo document with the given context
func (t *Table) PutWithContext(ctx context.Context, i TableItem) error {
	av, err := dynamodbattribute.MarshalMap(i)
	if err != nil {
		return err
	}
	_, err = t.client.Svc.PutItemWithContext(ctx, &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(t.tableName),
	})
	return err
}

// Get function to fetch an item from dynamo
func (t *Table) Get(i TableItem) error {
	return t.GetWithContext(t.client.Ctx(), i)
}

// GetWithContext function to fetch an item from dynamo with a given context
func (t *Table) GetWithContext(ctx context.Context, i TableItem) error {
	av, err := dynamodbattribute.MarshalMap(i)
	if err != nil {
		return err
	}
	r, err := t.client.Svc.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(t.tableName),
		Key:       av,
	})
	if err != nil {
		return err
	}
	if r.Item == nil {
		return errors.New("Could not find item")
	}
	return dynamodbattribute.UnmarshalMap(r.Item, i)
}
