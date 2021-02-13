package graph

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type Vertex struct {
	Type string          `json:"__t" csv:"__t" xml:"__t"`
	Id   string          `json:"__i" csv:"__i" xml:"__i"`
	Attr json.RawMessage `json:"__a" csv:"__a" xml:"__a"`
}

type vertexAlias Vertex
type vertexDDBRepresentation struct {
	Partition string `json:"__p" csv:"__p" xml:"__p"`
	Sort      string `json:"__s" csv:"__s" xml:"__s"`
	*vertexAlias
}

func (v *Vertex) ddbRepresentation() vertexDDBRepresentation {
	var id string
	if v.Type != "" && v.Id != "" {
		id = v.Type + keyDelimiter + v.Id
	}
	return vertexDDBRepresentation{
		Partition:   id,
		Sort:        id,
		vertexAlias: (*vertexAlias)(v),
	}
}

func (v *Vertex) MarshalAttributeValueMap() (map[string]*dynamodb.AttributeValue, error) {
	return v.MarshalAttributeValueMapWithinPartition("")
}

/*
	partition is only needed for Second Class vertices.
*/
func (v *Vertex) MarshalAttributeValueMapWithinPartition(partition string) (map[string]*dynamodb.AttributeValue, error) {
	aliasedV := v.ddbRepresentation()
	/// Check if the partition has been overridden.
	if partition != "" {
		aliasedV.Partition = partition
	}
	vMap, err := dynamodbattribute.MarshalMap(&aliasedV)
	if err != nil {
		return nil, err
	}
	return vMap, nil
}

func NewVertexFromAttributeValueMap(m map[string]*dynamodb.AttributeValue) (*Vertex, error) {
	var v Vertex
	aliasedV := v.ddbRepresentation()
	if err := dynamodbattribute.UnmarshalMap(m, &aliasedV); err != nil {
		return nil, err
	}
	return &v, nil
}

func (v *Vertex) GetAttributesAs(out interface{}) error {
	return json.Unmarshal(v.Attr, out)
}

func (g *graph) AddVertex(Type, Id string, Attr interface{}) (*Vertex, error) {
	return g.AddVertexWithContext(context.Background(), Type, Id, Attr)
}

func (g *graph) AddVertexWithContext(ctx context.Context, Type, Id string, Attr interface{}) (*Vertex, error) {

	v := Vertex{
		Type: Type,
		Id:   Id,
	}

	if Attr != nil {
		attr, err := json.Marshal(Attr)
		if err != nil {
			return nil, err
		}
		v.Attr = attr
	}

	vMap, err := v.MarshalAttributeValueMap()
	if err != nil {
		return nil, err
	}

	_, err = g.dynamodb.PutItemWithContext(ctx, &dynamodb.PutItemInput{
		Item:      vMap,
		TableName: aws.String(g.tableName),
	})
	if err != nil {
		return nil, err
	}

	return &v, nil
}

func (g *graph) GetVertex(Type, Id string) (*Vertex, error) {
	return g.GetVertexWithContext(context.Background(), Type, Id)
}

func (g *graph) GetVertexWithContext(ctx context.Context, Type, Id string) (*Vertex, error) {
	gId := Type + keyDelimiter + Id
	output, err := g.dynamodb.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			PartitionKeyName: {
				S: aws.String(gId),
			},
			SortKeyName: {
				S: aws.String(gId),
			},
		},
		TableName: aws.String(g.tableName),
	})
	if err != nil {
		return nil, err
	}

	v, err := NewVertexFromAttributeValueMap(output.Item)
	if err != nil {
		return nil, err
	}

	return v, nil
}
