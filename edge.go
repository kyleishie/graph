package graph

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"strings"
)

type Edge struct {
	Direction edgeDirection `json:"__d" csv:"__d" xml:"__d"`
	Label     string        `json:"__l" csv:"__l" xml:"__l"`
	attr      json.RawMessage

	V1 *Vertex `json:"-" csv:"-" xml:"-"`
	V2 *Vertex `json:"-" csv:"-" xml:"-"`

	g *graph
}

func (e *Edge) graphId() *string {
	p := e.Direction.String() + strings.Join([]string{e.Label, *e.V2.graphId()}, keyDelimiter)
	return &p
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

func (e *Edge) MarshalAttributeValueMap() (map[string]*dynamodb.AttributeValue, error) {
	return dynamodbattribute.MarshalMap(&edgeDDBRepresentation{
		Partition: e.V1.graphId(),
		Sort:      e.graphId(),
		Attr:      e.attr,
		edgeAlias: (*edgeAlias)(e),
	})
}

//type EdgeValidationError int
//
//func (err EdgeValidationError) Error() string {
//	switch err {
//	case NotAnEdge:
//		return "The given map is not an edge representation."
//	}
//
//	return ""
//}
//
//const (
//	NotAnEdge = EdgeValidationError(iota)
//)

func (e *Edge) UnmarshalAttributeValueMap(m map[string]*dynamodb.AttributeValue) error {
	alias := edgeDDBRepresentation{
		edgeAlias: (*edgeAlias)(e),
	}
	if err := dynamodbattribute.UnmarshalMap(m, &alias); err != nil {
		return err
	}
	e.attr = alias.Attr

	par := strings.Split(*alias.Partition, keyDelimiter)
	e.V1 = &Vertex{
		Type: par[0],
		Id:   par[1],
		g:    e.g,
	}

	sort := strings.Split(*alias.Sort, keyDelimiter)
	e.V2 = &Vertex{
		Type: sort[1],
		Id:   sort[2],
		g:    e.g,
	}

	return nil
}

func (e *Edge) GetAttributesAs(out interface{}) error {
	return json.Unmarshal(e.attr, out)
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

/*
	Writes an Edge to the tableName.
	NOTE: This call fails if the two vertices have not previously been written.
*/
func (g *graph) AddEdge(V1 *Vertex, Label string, V2 *Vertex, Attr interface{}) (*Edge, error) {

	/// Create an Edge from V1 out to v2
	v1Outv2 := Edge{
		Label:     Label,
		V1:        V1,
		V2:        V2,
		Direction: OUT,
		g:         V1.g,
	}
	if Attr != nil {
		attr, err := json.Marshal(Attr)
		if err != nil {
			return nil, err
		}
		v1Outv2.attr = attr
	}

	v1Outv2Map, err := v1Outv2.MarshalAttributeValueMap()
	if err != nil {
		return nil, err
	}

	/// Copy V2 so we can write it into the V1 partition
	v2CopyMap, err := V2.MarshalAttributeValueMap(V1.graphId())
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
		v2InV1.attr = attr
	}
	v2InV1Map, err := v2InV1.MarshalAttributeValueMap()
	if err != nil {
		return nil, err
	}

	/// Copy V1 into V2's partition
	v1CopyMap, err := V1.MarshalAttributeValueMap(V2.graphId())
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
	_, err = V1.g.dynamodb.TransactWriteItems(&dynamodb.TransactWriteItemsInput{
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
						PartitionKeyName: {S: V1.graphId()},
						SortKeyName:      {S: V1.graphId()},
					},
					TableName: &V1.g.tableName,
				},
			},

			/// Check that V2 exists.
			&dynamodb.TransactWriteItem{
				ConditionCheck: &dynamodb.ConditionCheck{
					ConditionExpression:       expr.Condition(),
					ExpressionAttributeNames:  expr.Names(),
					ExpressionAttributeValues: expr.Values(),
					Key: map[string]*dynamodb.AttributeValue{
						PartitionKeyName: {S: V2.graphId()},
						SortKeyName:      {S: V2.graphId()},
					},
					TableName: &V1.g.tableName,
				},
			},

			/// Write the new Edge into V1's partition.
			&dynamodb.TransactWriteItem{
				Put: &dynamodb.Put{
					Item:      v1Outv2Map,
					TableName: &V1.g.tableName,
				},
			},

			/// Write a copy of v2 into V1's partition.
			&dynamodb.TransactWriteItem{
				Put: &dynamodb.Put{
					Item:      v2CopyMap,
					TableName: &V1.g.tableName,
				},
			},

			/// MIRROR
			/// Write the new Edge into V1's partition.
			&dynamodb.TransactWriteItem{
				Put: &dynamodb.Put{
					Item:      v2InV1Map,
					TableName: &V1.g.tableName,
				},
			},

			/// Write a copy of V1 into V2's partition.
			&dynamodb.TransactWriteItem{
				Put: &dynamodb.Put{
					Item:      v1CopyMap,
					TableName: &V1.g.tableName,
				},
			},
		},
	})

	if err != nil {
		return nil, err
	}

	return &v1Outv2, nil
}
