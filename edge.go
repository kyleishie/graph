package graph

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

const (
	incomingEdgeMarker = "<"
	outgoingEdgeMarker = ">"
)

type edge struct {
	/// DO NOT MODIFY THIS UNLESS YOU ARE BIG BRAIN
	Partition string `json:"__p" csv:"__p" xml:"__p"`
	/// DO NOT MODIFY THIS UNLESS YOU ARE BIG BRAIN
	Sort string `json:"__s" csv:"__s" xml:"__s"`

	Label string `json:"__l" csv:"__l" xml:"__l"`

	/// The Type of V2 that this edge points to.
	/// This exists to faciliate easier lookup within an in memeory partition map.
	Type string `json:"__t" csv:"__t" xml:"__t"`

	/// The Id of V2 that this edge points to.
	/// This exists to faciliate easier lookup within an in memeory partition map.
	Id string `json:"__i" csv:"__i" xml:"__i"`

	attr map[string]*dynamodb.AttributeValue
}

func (e edge) toMap() (map[string]*dynamodb.AttributeValue, error) {

	eMap, err := dynamodbattribute.MarshalMap(e)
	if err != nil {
		return nil, err
	}

	for key, value := range e.attr {

		///TODO: Detect overlap and return err
		eMap[key] = value
	}

	return eMap, err
}

/*
	Writes an Edge to the tableName.
	NOTE: This call fails if the two vertices have not previously been written.
*/
func (V1 *vertex) AddEdge(Label string, V2 *vertex, Attr interface{}) (*edge, error) {

	attrMap, err := dynamodbattribute.MarshalMap(Attr)
	if err != nil {
		return nil, err
	}

	/// Create an edge from V1 out to v2
	v1Outv2 := edge{
		Partition: V1.Partition,
		Sort:      outgoingEdgeMarker + Label + keyDelimiter + V2.Partition,
		Label:     Label,
		Type:      V2.Type,
		Id:        V2.Id,
		attr:      attrMap,
	}
	v1Outv2Map, err := v1Outv2.toMap()
	if err != nil {
		return nil, err
	}
	/// Copy V2 so we can write it into the V1 partition
	v2CopyMap, err := copyVertex(V2, V1).toMap()
	if err != nil {
		return nil, err
	}

	/// MIRROR
	/// This section effectively mirrors the edge and copied vertex into V2's partition.
	/// The intention here is to pay storage costs for the sake of faster out vs in queries later.
	/// Create the mirror of the intended edge for easier queries later.
	v2Inv1Map, err := edge{
		Partition: V2.Partition,
		Sort:      incomingEdgeMarker + Label + keyDelimiter + V1.Partition,
		Label:     Label,
		Type:      V1.Type,
		Id:        V1.Id,
		attr:      attrMap,
	}.toMap()
	if err != nil {
		return nil, err
	}
	/// Copy V1 into V2's partition
	v1CopyMap, err := copyVertex(V1, V2).toMap()
	if err != nil {
		return nil, err
	}

	expr, err := expression.NewBuilder().
		WithCondition(expression.AttributeExists(expression.Name(sortKeyName))).
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
						partitionKeyName: {S: aws.String(V1.Partition)},
						sortKeyName:      {S: aws.String(V1.Partition)},
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
						partitionKeyName: {S: aws.String(V2.Partition)},
						sortKeyName:      {S: aws.String(V2.Partition)},
					},
					TableName: &V1.g.tableName,
				},
			},

			/// Write the new edge into V1's partition.
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
			/// Write the new edge into V1's partition.
			&dynamodb.TransactWriteItem{
				Put: &dynamodb.Put{
					Item:      v2Inv1Map,
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

/// Copies v2 into v1's partition.
func copyVertex(v2, v1 *vertex) *vertex {
	v2Copy := *v2
	v2Copy.Partition = v1.Partition
	return &v2Copy
}
