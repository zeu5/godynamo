package godynamo

import "github.com/aws/aws-sdk-go/service/dynamodb"

type CreateTableOp struct {
	client *Client
	Input  *dynamodb.CreateTableInput
	err    error
}

