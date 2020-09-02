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

type Config struct {
	Timeout time.Duration
}

var defaultConfig = &Config{
	Timeout: time.Second * 10,
}

type Client struct {
	Svc     *dynamodb.DynamoDB
	timeout time.Duration
}

func (s *Client) Table(t TableItem) *Table {
	return &Table{
		client:    s,
		tableName: t.TableName(),
	}
}

func (s *Client) CreateTable(t TableItem) error {
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
		TableName:            aws.String(t.TableName()),
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
	})
	return err
}

func (s *Client) Ctx() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), s.timeout)
	return ctx
}

func NewClient(config *Config) *Client {
	if config == nil {
		config = defaultConfig
	}

	return NewClientWithSession(session.Must(session.NewSession()), config)
}

func NewClientWithSession(s *session.Session, config *Config) *Client {
	if config == nil {
		config = defaultConfig
	}
	return &Client{
		Svc:     dynamodb.New(s),
		timeout: config.Timeout,
	}
}
