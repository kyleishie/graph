package graph

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type vertex struct {
	/// DO NOT MODIFY THIS UNLESS YOU ARE BIG BRAIN
	Partition string `json:"__p" csv:"__p" xml:"__p"`
	/// DO NOT MODIFY THIS UNLESS YOU ARE BIG BRAIN
	Sort string `json:"__s" csv:"__s" xml:"__s"`

	Type string `json:"__t" csv:"__t" xml:"__t"`

	Id string `json:"__i" csv:"__i" xml:"__i"`

	attr map[string]*dynamodb.AttributeValue

	g *graph
}

func (v vertex) toMap() (map[string]*dynamodb.AttributeValue, error) {

	vMap, err := dynamodbattribute.MarshalMap(v)
	if err != nil {
		return nil, err
	}

	for key, value := range v.attr {
		///TODO: Detect overlap and return err
		vMap[key] = value
	}

	return vMap, err
}

func (v vertex) AddAttributes(attr interface{}) error {

	//if reflect.ValueOf(attr).Kind() != reflect.Struct {
	//	return fmt.Errorf("attr must be a struct")
	//}

	attrMap, err := dynamodbattribute.MarshalMap(attr)
	if err != nil {
		return err
	}
	v.attr = attrMap
	return nil
}

func (v vertex) GetAttributesAs(out interface{}) error {
	if err := dynamodbattribute.UnmarshalMap(v.attr, out); err != nil {
		return err
	}
	return nil
}

func (g *graph) AddVertex(Type, Id string, Attr interface{}) (*vertex, error) {

	attrMap, err := dynamodbattribute.MarshalMap(Attr)
	if err != nil {
		return nil, err
	}

	v := vertex{
		Partition: Type + keyDelimiter + Id,
		Sort:      Type + keyDelimiter + Id,
		Type:      Type,
		Id:        Id,
		attr:      attrMap,
		g:         g,
	}

	vMap, err := v.toMap()
	if err != nil {
		return nil, err
	}

	_, err = g.dynamodb.PutItem(&dynamodb.PutItemInput{
		Item:      vMap,
		TableName: aws.String(g.tableName),
	})

	if err != nil {
		return nil, err
	}

	return &v, nil
}
