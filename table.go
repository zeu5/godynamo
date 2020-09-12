package godynamo

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// Table interface for interactive with dynamo
type Table struct {
	client    *Client
	tableName string
}

// Put function to create a new dynamo document
func (t *Table) Put() *PutOp {
	return &PutOp{
		t.client,
		&dynamodb.PutItemInput{
			TableName: aws.String(t.tableName),
		},
		nil,
	}
}

// Get function to fetch an item from dynamo
func (t *Table) Get() *GetOp {
	return &GetOp{
		t.client,
		&dynamodb.GetItemInput{
			TableName: aws.String(t.tableName),
		},
		nil,
	}
}

// PutOp struct that represents a PutItem operation
type PutOp struct {
	client *Client
	input  *dynamodb.PutItemInput
	err    error
}

// Bind function to bind a struct/map as input
func (p *PutOp) Bind(in interface{}) *PutOp {
	av, err := dynamodbattribute.MarshalMap(in)
	if err != nil {
		p.err = fmt.Errorf("bind error: %s", err)
	} else {
		p.input.Item = av
	}
	return p
}

// Execute function to execute the PutItem operation with default context
func (p *PutOp) Execute() error {
	if p.err != nil {
		return p.err
	}
	return p.ExecuteWithCtx(p.client.Ctx())
}

// ExecuteWithCtx function to execute the PutItem operation with the given context
func (p *PutOp) ExecuteWithCtx(ctx context.Context) error {
	if p.err != nil {
		return p.err
	}
	_, err := p.client.Svc.PutItemWithContext(ctx, p.input)
	return err
}

// GetOp struct that represents the GetItem operation
type GetOp struct {
	client *Client
	input  *dynamodb.GetItemInput
	err    error
}

// Bind function to bind a struct/map as input
func (g *GetOp) Bind(in interface{}) *GetOp {
	av, err := dynamodbattribute.MarshalMap(in)
	if err != nil {
		g.err = fmt.Errorf("bind error: %s", err)
	} else {
		g.input.Key = av
	}
	return g
}

// Execute function to execute the GetItem operation with the default context
func (g *GetOp) Execute(out interface{}) error {
	if g.err != nil {
		return g.err
	}
	return g.ExecuteWithCtx(g.client.Ctx(), out)
}

// ExecuteWithCtx function to execute the GetItem operation with the given context
func (g *GetOp) ExecuteWithCtx(ctx context.Context, out interface{}) error {
	r, err := g.client.Svc.GetItemWithContext(ctx, g.input)
	if err != nil {
		return err
	}
	if r.Item == nil {
		return fmt.Errorf("nothing found")
	}
	return dynamodbattribute.UnmarshalMap(r.Item, out)
}
