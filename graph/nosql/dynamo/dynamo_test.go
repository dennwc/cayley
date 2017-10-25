package dynamo

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/nosql"
	"github.com/cayleygraph/cayley/graph/nosql/nosqltest"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var (
	awsAccessKey = os.Getenv("AWS_ACCESS_KEY")
	awsSecretKey = os.Getenv("AWS_SECRET_KEY")
	awsRegion    = os.Getenv("AWS_REGION")
)

func makeDynamo(t testing.TB) (nosql.Database, graph.Options, func()) {
	if awsAccessKey == "" || awsSecretKey == "" || awsRegion == "" {
		t.SkipNow()
	}
	opt := graph.Options{
		"accessKey": awsAccessKey,
		"secretKey": awsSecretKey,
		"region":    awsRegion,
	}
	pref := fmt.Sprintf("cayley_test_%x", rand.Int())

	db, err := newDB(pref, opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("dynamodb table prefix: %q", pref)
	return db, nil, func() {
		req := &dynamodb.ListTablesInput{ExclusiveStartTableName: aws.String(pref)}
		for {
			resp, err := db.db.ListTables(req)
			if err != nil {
				t.Logf("cannot list tables: %v", err)
				return
			} else if len(resp.TableNames) == 0 {
				return
			}
			req.ExclusiveStartTableName = resp.LastEvaluatedTableName
			for _, name := range resp.TableNames {
				if name == nil || !strings.HasPrefix(*name, pref) {
					continue
				}
				_, err = db.db.DeleteTable(&dynamodb.DeleteTableInput{
					TableName: name,
				})
				if err != nil {
					t.Logf("cannot remove table %q: %v", *name, err)
				}
			}
			if resp.LastEvaluatedTableName == nil {
				return
			}
		}
	}
}

func TestDynamo(t *testing.T) {
	nosqltest.TestAll(t, makeDynamo, nil)
}
