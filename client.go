package godynamo

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/zeu5/godynamo/internal/util"
)

// Config struct to provide initial configuration for the connection
type Config struct {
	Timeout         time.Duration
	TagName         string
	PartitionKeyTag string
	SortKeyTag      string
}

var defaultConfig = &Config{
	Timeout:         time.Second * 10,
	TagName:         "dynamodb",
	PartitionKeyTag: "paritionkey",
	SortKeyTag:      "sortkey",
}

// Client struct that holds the connection objects
type Client struct {
	Svc    *dynamodb.DynamoDB
	config *Config
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
	e := util.NewSchemaEncoder(func(e *util.SchemaEncoder) {
		e.PartitionKeyTag = s.config.PartitionKeyTag
		e.SortKeyTag = s.config.SortKeyTag
		e.TagName = s.config.TagName
	})
	e.Encode(t)
	_, err := s.Svc.CreateTable(&dynamodb.CreateTableInput{
		AttributeDefinitions: e.AttributeDefinition(),
		KeySchema:            e.KeySchema(),
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
	ctx, _ := context.WithTimeout(context.Background(), s.config.Timeout)
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
		Svc:    dynamodb.New(s),
		config: config,
	}
}
