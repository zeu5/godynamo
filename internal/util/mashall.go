package util

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

const tagName = "dynamodb"

const partitionKeyTag = "paritionkey"
const sortKeyTag = "sortkey"

type KeyEncoder struct {
	paritionKey     string
	sortKey         string
	TagName         string
	PartitionKeyTag string
	SortKeyTag      string
}

func NewKeyEncoder(opts ...func(*KeyEncoder)) *KeyEncoder {
	k := &KeyEncoder{
		TagName:         tagName,
		PartitionKeyTag: partitionKeyTag,
		SortKeyTag:      sortKeyTag,
	}
	for _, o := range opts {
		o(k)
	}
	return k
}

func (e *KeyEncoder) Encode(in interface{}) (map[string]*dynamodb.AttributeValue, error) {
	v := valueElem(reflect.ValueOf(in))

	switch v.Kind() {
	case reflect.Struct:
		return e.encodeStruct(v)
	default:
		return dynamodbattribute.MarshalMap(in)
	}
}

func (e *KeyEncoder) encodeStruct(v reflect.Value) (map[string]*dynamodb.AttributeValue, error) {
	av := make(map[string]*dynamodb.AttributeValue)

	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if tv, ok := f.Tag.Lookup(e.TagName); ok {
			if tv == e.PartitionKeyTag {
				if e.paritionKey != "" {
					return av, fmt.Errorf("encoder error: multiple partition keys")
				}
				e.paritionKey = f.Name
			} else if tv == e.SortKeyTag {
				if e.sortKey != "" {
					return av, fmt.Errorf("encoder error: multiple sort keys")
				}
				e.sortKey = f.Name
			}
		}
	}
	if e.paritionKey == "" {
		return av, fmt.Errorf("encoder error: no partition key")
	}
	p := &dynamodb.AttributeValue{}
	if err := encodeScaler(p, v.FieldByName(e.paritionKey)); err != nil {
		return av, err
	}
	av[e.paritionKey] = p
	if e.sortKey != "" {
		s := &dynamodb.AttributeValue{}
		if err := encodeScaler(s, v.FieldByName(e.sortKey)); err != nil {
			return av, nil
		}
		av[e.sortKey] = s
	}
	return av, nil
}

func encodeScaler(av *dynamodb.AttributeValue, v reflect.Value) error {
	switch v.Kind() {
	case reflect.Invalid:
		av.NULL = aws.Bool(true)
	case reflect.Bool:
		av.BOOL = aws.Bool(v.Bool())
	case reflect.String:
		s := v.String()
		if len(s) == 0 {
			return fmt.Errorf("encode error: empty string")
		}
		av.S = &s
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		av.N = aws.String(strconv.FormatInt(v.Int(), 10))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		av.N = aws.String(strconv.FormatUint(v.Uint(), 10))
	case reflect.Float32:
		av.N = aws.String(strconv.FormatFloat(v.Float(), 'f', -1, 32))
	case reflect.Float64:
		av.N = aws.String(strconv.FormatFloat(v.Float(), 'f', -1, 64))
	default:
		return fmt.Errorf("encode error: unsupported type")
	}
	return nil
}

func MarshalKey(in interface{}, encoderOpts ...func(*KeyEncoder)) (map[string]*dynamodb.AttributeValue, error) {
	return NewKeyEncoder(encoderOpts...).Encode(in)
}

type SchemaEncoder struct {
	partitionKey     string
	partitionKeyType string
	sortKey          string
	sortKeyType      string
	TagName          string
	PartitionKeyTag  string
	SortKeyTag       string
}

func (s *SchemaEncoder) Encode(in interface{}) error {
	v := typeElem(reflect.TypeOf(in))
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		if tv, ok := f.Tag.Lookup(s.TagName); ok {
			if tv == s.PartitionKeyTag {
				if s.partitionKey != "" {
					return fmt.Errorf("encoder error: multiple partition keys")
				}
				s.partitionKey = f.Name
				s.partitionKeyType = attributeType(f.Type.Kind())
			} else if tv == s.SortKeyTag {
				if s.sortKey != "" {
					return fmt.Errorf("encoder error: multiple sort keys")
				}
				s.sortKey = f.Name
				s.sortKeyType = attributeType(f.Type.Kind())
			}
		}
	}
	if s.partitionKey == "" {
		return fmt.Errorf("encoder error: no partition key")
	}
	return nil
}

func (s *SchemaEncoder) AttributeDefinition() []*dynamodb.AttributeDefinition {
	defs := []*dynamodb.AttributeDefinition{
		{
			AttributeName: aws.String(s.partitionKey),
			AttributeType: aws.String(s.partitionKeyType),
		},
	}
	if s.sortKey != "" {
		defs = append(defs, &dynamodb.AttributeDefinition{
			AttributeName: aws.String(s.sortKey),
			AttributeType: aws.String(s.sortKeyType),
		})
	}
	return defs
}

func (s *SchemaEncoder) KeySchema() []*dynamodb.KeySchemaElement {
	keys := []*dynamodb.KeySchemaElement{
		{
			AttributeName: aws.String(s.partitionKey),
			KeyType:       aws.String("HASH"),
		},
	}
	if s.sortKey != "" {
		keys = append(keys, &dynamodb.KeySchemaElement{
			AttributeName: aws.String(s.sortKey),
			KeyType:       aws.String("RANGE"),
		})
	}
	return keys
}

func NewSchemaEncoder(opts ...func(e *SchemaEncoder)) *SchemaEncoder {
	e := &SchemaEncoder{
		TagName:         tagName,
		SortKeyTag:      sortKeyTag,
		PartitionKeyTag: partitionKeyTag,
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

func attributeType(k reflect.Kind) string {
	switch k {
	case reflect.Uint8:
		return "B"
	case reflect.String:
		return "S"
	default:
		return "N"
	}
}

func typeElem(t reflect.Type) reflect.Type {
	switch t.Kind() {
	case reflect.Interface, reflect.Ptr:
		for t.Kind() == reflect.Interface || t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
	}
	return t
}

func valueElem(v reflect.Value) reflect.Value {
	switch v.Kind() {
	case reflect.Interface, reflect.Ptr:
		for v.Kind() == reflect.Interface || v.Kind() == reflect.Ptr {
			v = v.Elem()
		}
	}
	return v
}
