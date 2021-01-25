package graph

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type traversalContext struct {
	g     *graph
	err   error
	items []map[string]*dynamodb.AttributeValue
	path  []string
}

type traversalVertex struct {
	v Vertex
	e []Edge
}

type traversalEdge struct {
	e Edge
}

//func (g *graph) GetVertices(Type, Id string) *traversalContext {
//
//	graphId := Type + keyDelimiter + Id
//
//	var t = traversalContext{
//		err:   nil,
//		items: nil,
//		g: g,
//		path: []string{
//			graphId,
//		},
//	}
//
//	pCon := expr.Key(PartitionKeyName).Equal(expr.Value(graphId))
//	sCon := expr.Key(SortKeyName).Equal(expr.Value(graphId))
//	expr, err := expr.NewBuilder().
//		WithKeyCondition(pCon.And(sCon)).
//		Build()
//	if err != nil {
//		t.err = err
//		return &t
//	}
//
//	output, err := g.dynamodb.Query(&dynamodb.QueryInput{
//		KeyConditionExpression:    expr.KeyCondition(),
//		ExpressionAttributeNames:  expr.Names(),
//		ExpressionAttributeValues: expr.Values(),
//		TableName:                 aws.String(g.tableName),
//	});
//	if err != nil {
//		t.err = err
//		return &t
//	}
//
//	t.items = output.Items
//	return &t
//}
//
//func (t *traversalContext) As(out interface{}) *traversalContext {
//	switch reflect.ValueOf(out).Elem().Kind() {
//	case reflect.Struct, reflect.Map:
//		if len(t.items) == 0 {
//			t.err = fmt.Errorf("traversalContent.items is empty in call to As(", reflect.ValueOf(out).Kind().String(), ")")
//		}
//		t.err = dynamodbattribute.UnmarshalMap(t.items[0], out)
//	case reflect.Array, reflect.Slice:
//		t.err = dynamodbattribute.UnmarshalListOfMaps(t.items, out)
//	}
//	return t
//}
//
//func (t traversalContext) IsErr() error {
//	return t.err
//}
//
//
//func (t *traversalContext) Out(label string, out interface{}) *traversalContext {
//
//	graphId := Type + keyDelimiter + Id
//	pCon := expr.Key(PartitionKeyName).Equal(expr.Value(graphId))
//	sCon := expr.Key(SortKeyName).Equal(expr.Value(graphId))
//	expr, err := expr.NewBuilder().
//		WithKeyCondition(pCon.And(sCon)).
//		Build()
//	if err != nil {
//		t.err = err
//		return &t
//	}
//
//	output, err := g.dynamodb.Query(&dynamodb.QueryInput{
//		KeyConditionExpression:    expr.KeyCondition(),
//		ExpressionAttributeNames:  expr.Names(),
//		ExpressionAttributeValues: expr.Values(),
//		TableName:                 aws.String(g.tableName),
//	});
//	if err != nil {
//		t.err = err
//		return &t
//	}
//
//	t.items = output.Items
//
//	switch reflect.ValueOf(out).Elem().Kind() {
//	case reflect.Struct, reflect.Map:
//		if len(t.items) == 0 {
//			t.err = fmt.Errorf("traversalContent.items is empty in call to As(", reflect.ValueOf(out).Kind().String(), ")")
//		}
//		t.err = dynamodbattribute.UnmarshalMap(t.items[0], out)
//	case reflect.Array, reflect.Slice:
//		t.err = dynamodbattribute.UnmarshalListOfMaps(t.items, out)
//	}
//	return t
//}
