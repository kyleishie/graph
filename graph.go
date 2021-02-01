package graph

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type graph struct {
	tableName string
	dynamodb  *dynamodb.DynamoDB
}

func New(tableName string, dynamodb *dynamodb.DynamoDB) *graph {
	return &graph{
		tableName: tableName,
		dynamodb:  dynamodb,
	}
}

/*
	DEFINITIONS

	VertexId: The unique identifier for a given entity.
	GraphId: The combination of a given record's Partition and Sort Keys that make it unique in the entire graph.  AKA the DynamoDB Primary Key.

	First Class Vertex: An Vertex in the graph whose __p and __s are equal.  This represents the original entity.
	Second Class Vertex: A Vertex in the graph whose __p and __s are NOT equal.  This represents the v2 of an Edge.
*/

const (
	keyDelimiter = "#"

	PartitionKeyName = "__p"
	SortKeyName      = "__s"
)
