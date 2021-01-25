package graph

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"strings"
)

type Vertex struct {
	Type string      `json:"__t" csv:"__t" xml:"__t"`
	Id   string      `json:"__i" csv:"__i" xml:"__i"`
	Attr interface{} `json:"__a" csv:"__a" xml:"__a"`
	g    *graph
}

func (v *Vertex) graphId() *string {
	p := strings.Join([]string{v.Type, v.Id}, keyDelimiter)
	return &p
}

/*
	partition is only needed for Second Class vertices.
*/
func (v *Vertex) MarshalAttributeValueMap(partition *string) (map[string]*dynamodb.AttributeValue, error) {
	type Alias Vertex
	aliasedV := &struct {
		Partition string `json:"__p" csv:"__p" xml:"__p"`
		Sort      string `json:"__s" csv:"__s" xml:"__s"`
		*Alias
	}{
		Partition: v.Type + keyDelimiter + v.Id,
		Sort:      v.Type + keyDelimiter + v.Id,
		Alias:     (*Alias)(v),
	}
	/// Check if the partition has been overridden.
	if partition != nil {
		aliasedV.Partition = *partition
	}

	vMap, err := dynamodbattribute.MarshalMap(aliasedV)
	if err != nil {
		return nil, err
	}

	return vMap, nil
}

func (v *Vertex) UnmarshalAttributeValueMap(m map[string]*dynamodb.AttributeValue) error {
	type Alias Vertex
	aliasedV := &struct {
		Partition string `json:"__p" csv:"__p" xml:"__p"`
		Sort      string `json:"__s" csv:"__s" xml:"__s"`
		*Alias
	}{
		Alias: (*Alias)(v),
	}

	if err := dynamodbattribute.UnmarshalMap(m, aliasedV); err != nil {
		return err
	}

	return nil
}

func (g *graph) AddVertex(Type, Id string, Attr interface{}) (*Vertex, error) {

	v := Vertex{
		Type: Type,
		Id:   Id,
		Attr: Attr,
		g:    g,
	}

	vMap, err := v.MarshalAttributeValueMap(nil)
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

func (g *graph) GetVertex(Type, Id string) (*Vertex, error) {

	v := Vertex{
		Type: Type,
		Id:   Id,
		Attr: nil,
		g:    g,
	}

	output, err := g.dynamodb.GetItem(&dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			PartitionKeyName: {
				S: v.graphId(),
			},
			SortKeyName: {
				S: v.graphId(),
			},
		},
		TableName: aws.String(g.tableName),
	})
	if err != nil {
		return nil, err
	}

	if err := v.UnmarshalAttributeValueMap(output.Item); err != nil {
		return nil, err
	}

	return &v, nil
}
