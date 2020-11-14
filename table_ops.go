package godynamo

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/zeu5/godynamo/internal/util"
)

// PutItemOp struct that represents a PutItem operation
type PutItemOp struct {
	client *Client
	Input  *dynamodb.PutItemInput
	err    error
}

// Bind function to bind a struct/map as input
func (p *PutItemOp) Bind(in interface{}) *PutItemOp {
	av, err := dynamodbattribute.MarshalMap(in)
	if err != nil {
		p.err = fmt.Errorf("bind error: %s", err)
	} else {
		p.Input.Item = av
	}
	return p
}

func (p *PutItemOp) UseExpr(expr expression.Expression) *PutItemOp {
	p.Input.ConditionExpression = expr.Condition()
	p.Input.ExpressionAttributeNames = expr.Names()
	p.Input.ExpressionAttributeValues = expr.Values()
	return p
}

func (p *PutItemOp) Request() (*request.Request, *dynamodb.PutItemOutput) {
	return p.client.Svc.PutItemRequest(p.Input)
}

// Execute function to execute the PutItem operation with default context
func (p *PutItemOp) Execute() error {
	if p.err != nil {
		return p.err
	}
	return p.ExecuteWithCtx(p.client.Ctx())
}

// ExecuteWithCtx function to execute the PutItem operation with the given context
func (p *PutItemOp) ExecuteWithCtx(ctx context.Context) error {
	if p.err != nil {
		return p.err
	}
	_, err := p.client.Svc.PutItemWithContext(ctx, p.Input)
	if err != nil {
		return fmt.Errorf("failed to put item: %s", err)
	}
	return nil
}

// GetItemOp struct that represents the GetItem operation
type GetItemOp struct {
	client *Client
	Input  *dynamodb.GetItemInput
	err    error
}

// Bind function to bind a struct/map as input
func (g *GetItemOp) Bind(in interface{}) *GetItemOp {
	av, err := util.MarshalKey(in, func(k *util.KeyEncoder) {
		k.PartitionKeyTag = g.client.config.PartitionKeyTag
		k.SortKeyTag = g.client.config.SortKeyTag
		k.TagName = g.client.config.TagName
	})
	if err != nil {
		g.err = fmt.Errorf("bind error: %s", err)
	} else {
		g.err = nil
		g.Input.Key = av
	}
	return g
}

func (g *GetItemOp) Request() (*request.Request, *dynamodb.GetItemOutput) {
	return g.client.Svc.GetItemRequest(g.Input)
}

func (g *GetItemOp) UseExpr(expr expression.Expression) *GetItemOp {
	g.Input.ExpressionAttributeNames = expr.Names()
	g.Input.ProjectionExpression = expr.Projection()
	return g
}

// Execute function to execute the GetItem operation with the default context
func (g *GetItemOp) Execute(out interface{}) error {
	if g.err != nil {
		return g.err
	}
	return g.ExecuteWithCtx(g.client.Ctx(), out)
}

// ExecuteWithCtx function to execute the GetItem operation with the given context
func (g *GetItemOp) ExecuteWithCtx(ctx context.Context, out interface{}) error {
	r, err := g.client.Svc.GetItemWithContext(ctx, g.Input)
	if err != nil {
		return err
	}
	if r.Item == nil {
		return fmt.Errorf("nothing found")
	}
	return dynamodbattribute.UnmarshalMap(r.Item, out)
}

type UpdateItemOp struct {
	client *Client
	Input  *dynamodb.UpdateItemInput
	err    error
}

func (u *UpdateItemOp) Bind(in interface{}) *UpdateItemOp {
	av, err := util.MarshalKey(in, func(k *util.KeyEncoder) {
		k.PartitionKeyTag = u.client.config.PartitionKeyTag
		k.SortKeyTag = u.client.config.SortKeyTag
		k.TagName = u.client.config.TagName
	})
	if err != nil {
		u.err = fmt.Errorf("bind error: %s", err)
	} else {
		u.Input.Key = av
	}
	return u
}

func (u *UpdateItemOp) UseExpr(expr expression.Expression) *UpdateItemOp {
	u.Input.ExpressionAttributeNames = expr.Names()
	u.Input.ExpressionAttributeValues = expr.Values()
	u.Input.UpdateExpression = expr.Update()
	return u
}

func (u *UpdateItemOp) Request() (*request.Request, *dynamodb.UpdateItemOutput) {
	return u.client.Svc.UpdateItemRequest(u.Input)
}

func (u *UpdateItemOp) Execute() error {
	if u.err != nil {
		return u.err
	}
	return u.ExecuteWithCtx(u.client.Ctx())
}

func (u *UpdateItemOp) ExecuteWithCtx(ctx context.Context) error {
	if u.err != nil {
		return u.err
	}
	_, err := u.client.Svc.UpdateItemWithContext(ctx, u.Input)
	if err != nil {
		return fmt.Errorf("failed to update: %s", err)
	}
	return nil
}

type DeleteItemOp struct {
	client *Client
	Input  *dynamodb.DeleteItemInput
	err    error
}

func (d *DeleteItemOp) Bind(in interface{}) *DeleteItemOp {
	av, err := dynamodbattribute.MarshalMap(in)
	if err != nil {
		d.err = fmt.Errorf("bind error: %s", err)
	} else {
		d.Input.Key = av
	}
	return d
}

func (d *DeleteItemOp) Request() (*request.Request, *dynamodb.DeleteItemOutput) {
	return d.client.Svc.DeleteItemRequest(d.Input)
}

func (d *DeleteItemOp) UseExpr(expr expression.Expression) *DeleteItemOp {
	d.Input.ExpressionAttributeNames = expr.Names()
	d.Input.ExpressionAttributeValues = expr.Values()
	d.Input.ConditionExpression = expr.Condition()
	return d
}

func (d *DeleteItemOp) Execute() error {
	if d.err != nil {
		return d.err
	}
	return d.ExecuteWithCtx(d.client.Ctx())
}

func (d *DeleteItemOp) ExecuteWithCtx(ctx context.Context) error {
	if d.err != nil {
		return d.err
	}
	_, err := d.client.Svc.DeleteItemWithContext(ctx, d.Input)
	if err != nil {
		return fmt.Errorf("failed to delete: %s", err)
	}
	return nil
}
