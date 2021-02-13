package graph

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"strings"
)

type Edge struct {
	Direction edgeDirection `json:"__d" csv:"__d" xml:"__d"`
	Label     string        `json:"__l" csv:"__l" xml:"__l"`
	Attr      json.RawMessage

	V1 *Vertex `json:"-" csv:"-" xml:"-"`
	V2 *Vertex `json:"-" csv:"-" xml:"-"`
}

type edgeDirection string

const (
	OutgoingEdgeMarker = ">"
	IncomingEdgeMarker = "<"

	OUT = edgeDirection(OutgoingEdgeMarker)
	IN  = edgeDirection(IncomingEdgeMarker)
)

func (e edgeDirection) String() string {
	if e == OUT {
		return OutgoingEdgeMarker
	} else {
		return IncomingEdgeMarker
	}
}

type edgeAlias Edge
type edgeDDBRepresentation struct {
	Partition *string         `json:"__p" csv:"__p" xml:"__p"`
	Sort      *string         `json:"__s" csv:"__s" xml:"__s"`
	Attr      json.RawMessage `json:"__a" csv:"__a" xml:"__a"`

	*edgeAlias
}

func (e *Edge) ddbRepresentation() edgeDDBRepresentation {
	return edgeDDBRepresentation{
		Partition: aws.String(e.V1.ddbRepresentation().Partition),
		Sort:      aws.String(e.Direction.String() + e.Label + keyDelimiter + e.V2.ddbRepresentation().Partition),
		Attr:      e.Attr,
		edgeAlias: (*edgeAlias)(e),
	}
}

func (e *Edge) MarshalAttributeValueMap() (map[string]*dynamodb.AttributeValue, error) {
	ddbRep := e.ddbRepresentation()
	return dynamodbattribute.MarshalMap(&ddbRep)
}

func NewEdgeFromAttributeValueMap(m map[string]*dynamodb.AttributeValue, e *Edge) error {
	alias := edgeDDBRepresentation{
		edgeAlias: (*edgeAlias)(e),
	}
	if err := dynamodbattribute.UnmarshalMap(m, &alias); err != nil {
		return err
	}
	e.Attr = alias.Attr

	par := strings.Split(*alias.Partition, keyDelimiter)
	e.V1 = &Vertex{
		Type: par[0],
		Id:   par[1],
	}

	sort := strings.Split(*alias.Sort, keyDelimiter)
	e.V2 = &Vertex{
		Type: sort[1],
		Id:   sort[2],
	}

	return nil
}

func (e *Edge) GetAttributesAs(out interface{}) error {
	return json.Unmarshal(e.Attr, out)
}

func (e Edge) Mirror() Edge {
	v1 := e.V1
	e.V1 = e.V2
	e.V2 = v1

	if e.Direction == OUT {
		e.Direction = IN
	} else {
		e.Direction = OUT
	}

	return e
}

func (g *graph) AddEdge(V1 *Vertex, Label string, V2 *Vertex, Attr interface{}) (*Edge, error) {
	return g.AddEdgeWithContext(context.Background(), V1, Label, V2, Attr)
}

/*
	Writes an Edge to the tableName.
	NOTE: This call fails if the two vertices have not previously been written.
*/
func (g *graph) AddEdgeWithContext(ctx context.Context, V1 *Vertex, Label string, V2 *Vertex, Attr interface{}) (*Edge, error) {

	/// Create an Edge from V1 out to v2
	v1Outv2 := Edge{
		Label:     Label,
		V1:        V1,
		V2:        V2,
		Direction: OUT,
	}
	if Attr != nil {
		attr, err := json.Marshal(Attr)
		if err != nil {
			return nil, err
		}
		v1Outv2.Attr = attr
	}

	v1Outv2Map, err := v1Outv2.MarshalAttributeValueMap()
	if err != nil {
		return nil, err
	}

	/// Copy V2 so we can write it into the V1 partition
	v2CopyMap, err := V2.MarshalAttributeValueMapWithinPartition(V1.ddbRepresentation().Partition)
	if err != nil {
		return nil, err
	}

	/// MIRROR
	/// This section effectively mirrors the Edge and copied Vertex into V2's partition.
	/// The intention here is to pay storage costs for the sake of faster out vs in queries later.
	/// Create the mirror of the intended Edge for easier queries later.
	v2InV1 := Edge{
		Label:     Label,
		V1:        V2,
		V2:        V1,
		Direction: IN,
	}
	if Attr != nil {
		attr, err := json.Marshal(Attr)
		if err != nil {
			return nil, err
		}
		v2InV1.Attr = attr
	}
	v2InV1Map, err := v2InV1.MarshalAttributeValueMap()
	if err != nil {
		return nil, err
	}

	/// Copy V1 into V2's partition
	v1CopyMap, err := V1.MarshalAttributeValueMapWithinPartition(V2.ddbRepresentation().Partition)
	if err != nil {
		return nil, err
	}

	expr, err := expression.NewBuilder().
		WithCondition(expression.AttributeExists(expression.Name(SortKeyName))).
		Build()
	if err != nil {
		return nil, err
	}

	/// See the comments inline with the individual transact items.
	_, err = g.dynamodb.TransactWriteItemsWithContext(ctx, &dynamodb.TransactWriteItemsInput{
		ClientRequestToken:          nil,
		ReturnConsumedCapacity:      nil,
		ReturnItemCollectionMetrics: nil,
		TransactItems: []*dynamodb.TransactWriteItem{

			/// Check that V1 exists.
			&dynamodb.TransactWriteItem{
				ConditionCheck: &dynamodb.ConditionCheck{
					ConditionExpression:       expr.Condition(),
					ExpressionAttributeNames:  expr.Names(),
					ExpressionAttributeValues: expr.Values(),
					Key: map[string]*dynamodb.AttributeValue{
						PartitionKeyName: {S: aws.String(V1.ddbRepresentation().Partition)},
						SortKeyName:      {S: aws.String(V1.ddbRepresentation().Sort)},
					},
					TableName: &g.tableName,
				},
			},

			/// Check that V2 exists.
			&dynamodb.TransactWriteItem{
				ConditionCheck: &dynamodb.ConditionCheck{
					ConditionExpression:       expr.Condition(),
					ExpressionAttributeNames:  expr.Names(),
					ExpressionAttributeValues: expr.Values(),
					Key: map[string]*dynamodb.AttributeValue{
						PartitionKeyName: {S: aws.String(V2.ddbRepresentation().Partition)},
						SortKeyName:      {S: aws.String(V2.ddbRepresentation().Partition)},
					},
					TableName: &g.tableName,
				},
			},

			/// Write the new Edge into V1's partition.
			&dynamodb.TransactWriteItem{
				Put: &dynamodb.Put{
					Item:      v1Outv2Map,
					TableName: &g.tableName,
				},
			},

			/// Write a copy of v2 into V1's partition.
			&dynamodb.TransactWriteItem{
				Put: &dynamodb.Put{
					Item:      v2CopyMap,
					TableName: &g.tableName,
				},
			},

			/// MIRROR
			/// Write the new Edge into V1's partition.
			&dynamodb.TransactWriteItem{
				Put: &dynamodb.Put{
					Item:      v2InV1Map,
					TableName: &g.tableName,
				},
			},

			/// Write a copy of V1 into V2's partition.
			&dynamodb.TransactWriteItem{
				Put: &dynamodb.Put{
					Item:      v1CopyMap,
					TableName: &g.tableName,
				},
			},
		},
	})

	if err != nil {
		return nil, err
	}

	return &v1Outv2, nil
}
