package godynamo

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Table interface for interactive with dynamo
type Table struct {
	client    *Client
	tableName string
}

func (t *Table) Name() string {
	return t.tableName
}

// PutItem function to create a new dynamo document
func (t *Table) PutItem() *PutItemOp {
	return &PutItemOp{
		t.client,
		&dynamodb.PutItemInput{
			TableName: aws.String(t.tableName),
		},
		nil,
	}
}

// GetItem function to fetch an item from dynamo
func (t *Table) GetItem() *GetItemOp {
	return &GetItemOp{
		t.client,
		&dynamodb.GetItemInput{
			TableName: aws.String(t.tableName),
		},
		nil,
	}
}

func (t *Table) UpdateItem() *UpdateItemOp {
	return &UpdateItemOp{
		t.client,
		&dynamodb.UpdateItemInput{
			TableName: aws.String(t.tableName),
		},
		nil,
	}
}

func (t *Table) DeleteItem() *DeleteItemOp {
	return &DeleteItemOp{
		t.client,
		&dynamodb.DeleteItemInput{
			TableName: aws.String(t.tableName),
		},
		nil,
	}
}
