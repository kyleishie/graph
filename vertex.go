package graph

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"strings"
)

type Vertex struct {
	Type string `json:"__t" csv:"__t" xml:"__t"`
	Id   string `json:"__i" csv:"__i" xml:"__i"`
	attr json.RawMessage
	g    *graph
}

func (v *Vertex) graphId() *string {
	p := strings.Join([]string{v.Type, v.Id}, keyDelimiter)
	return &p
}

type vertexAlias Vertex
type vertexDDBRepresentation struct {
	Partition string          `json:"__p" csv:"__p" xml:"__p"`
	Sort      string          `json:"__s" csv:"__s" xml:"__s"`
	Attr      json.RawMessage `json:"__a" csv:"__a" xml:"__a"`
	*vertexAlias
}

/*
	partition is only needed for Second Class vertices.
*/
func (v *Vertex) MarshalAttributeValueMap(partition *string) (map[string]*dynamodb.AttributeValue, error) {
	aliasedV := vertexDDBRepresentation{
		Partition:   v.Type + keyDelimiter + v.Id,
		Sort:        v.Type + keyDelimiter + v.Id,
		Attr:        v.attr,
		vertexAlias: (*vertexAlias)(v),
	}
	/// Check if the partition has been overridden.
	if partition != nil {
		aliasedV.Partition = *partition
	}

	//if len(v.attr) > 0 {
	//	if err := json.Unmarshal(v.attr, &aliasedV.Attr); err != nil {
	//		return nil, err
	//	}
	//}

	vMap, err := dynamodbattribute.MarshalMap(&aliasedV)
	if err != nil {
		return nil, err
	}

	return vMap, nil
}

func (v *Vertex) UnmarshalAttributeValueMap(m map[string]*dynamodb.AttributeValue) error {
	type Alias Vertex
	aliasedV := &struct {
		Partition string          `json:"__p" csv:"__p" xml:"__p"`
		Sort      string          `json:"__s" csv:"__s" xml:"__s"`
		Attr      json.RawMessage `json:"__a" csv:"__a" xml:"__a"`
		*Alias
	}{
		Alias: (*Alias)(v),
	}

	if err := dynamodbattribute.UnmarshalMap(m, aliasedV); err != nil {
		return err
	}

	v.attr = aliasedV.Attr

	return nil
}

func (v *Vertex) GetAttributesAs(out interface{}) error {
	return json.Unmarshal(v.attr, out)
}

func (g *graph) AddVertex(Type, Id string, Attr interface{}) (*Vertex, error) {

	v := Vertex{
		Type: Type,
		Id:   Id,
		g:    g,
	}

	if Attr != nil {
		attr, err := json.Marshal(Attr)
		if err != nil {
			return nil, err
		}
		v.attr = attr
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
		attr: nil,
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
