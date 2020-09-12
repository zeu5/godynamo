package godynamo

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const tagName = "dynamodb"

const partitionKeyTag string = "paritionkey"
const sortKeyTag string = "sortkey"

// Config struct to provide initial configuration for the connection
type Config struct {
	Timeout time.Duration
}

var defaultConfig = &Config{
	Timeout: time.Second * 10,
}

// Client struct that holds the connection objects
type Client struct {
	Svc     *dynamodb.DynamoDB
	timeout time.Duration
}

// Table function to fetch the abstract Table interface for further interaction
func (s *Client) Table(t string) *Table {
	return &Table{
		client:    s,
		tableName: t,
	}
}

// CreateTable function to create a new table in Dynamo. Throws error if it already exists
func (s *Client) CreateTable(name string, t interface{}) error {
	var paritionKey string
	var partitionKeyKind reflect.Kind
	var sortKey string
	var sortKeyKind reflect.Kind
	v := valueElem(reflect.TypeOf(t))
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		if tv, ok := f.Tag.Lookup(tagName); ok {
			if tv == partitionKeyTag {
				if paritionKey != "" {
					return errors.New("Multiple parition keys for model")
				}
				paritionKey = f.Name
				partitionKeyKind = f.Type.Kind()
			} else if tv == sortKeyTag {
				if sortKey != "" {
					return errors.New("Multiple sort keys for model")
				}
				sortKey = f.Name
				sortKeyKind = f.Type.Kind()
			}
		}
	}
	if paritionKey == "" {
		return errors.New("Could not find partition key for model")
	}
	attrs := []*dynamodb.AttributeDefinition{
		{
			AttributeName: aws.String(paritionKey),
			AttributeType: aws.String(attributeType(partitionKeyKind)),
		},
	}
	keys := []*dynamodb.KeySchemaElement{
		{
			AttributeName: aws.String(paritionKey),
			KeyType:       aws.String("HASH"),
		},
	}
	if sortKey != "" {
		attrs = append(attrs, &dynamodb.AttributeDefinition{
			AttributeName: aws.String(sortKey),
			AttributeType: aws.String(attributeType(sortKeyKind)),
		})
		keys = append(keys, &dynamodb.KeySchemaElement{
			AttributeName: aws.String(sortKey),
			KeyType:       aws.String("RANGE"),
		})
	}
	_, err := s.Svc.CreateTable(&dynamodb.CreateTableInput{
		AttributeDefinitions: attrs,
		KeySchema:            keys,
		TableName:            aws.String(name),
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
	})
	return err
}

// Ctx function returns the context with the specified Timeout value
func (s *Client) Ctx() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), s.timeout)
	return ctx
}

// NewClient function to create a Client with the given Config
func NewClient(config *Config) *Client {
	if config == nil {
		config = defaultConfig
	}

	return NewClientWithSession(session.Must(session.NewSession()), config)
}

// NewClientWithSession function to create a client with the provided aws sessio object
func NewClientWithSession(s *session.Session, config *Config) *Client {
	if config == nil {
		config = defaultConfig
	}
	return &Client{
		Svc:     dynamodb.New(s),
		timeout: config.Timeout,
	}
}
