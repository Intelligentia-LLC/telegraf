package firehose

import (
	"testing"
	"fmt"
//	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/testutil"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
	//"github.com/influxdata/telegraf/testutil"
	//uuid "github.com/satori/go.uuid"
	//"github.com/stretchr/testify/assert"
)

//req, resp := f.svc.PutRecordBatchRequest(batchInput)
type mockFirehose struct {
	firehoseiface.FirehoseAPI

	// test values
	expectedLines int64
	numOfPuts int64  // tracks the number of times PutRecordBatch was called

	// reaction values
	numErrors int64
}

// Overriding this here so we can do unit testing of firehose puts without
// actually engaging the AWS API
func (m mockFirehose) PutRecordBatch(input *firehose.PutRecordBatchInput) (output *firehose.PutRecordBatchOutput, err error) {
	m.numOfPuts++

	emptyString := ""

	var zero int64 = m.numErrors
	// TODO insert the errors.
	// TODO check for the number of expected lines

	entry := firehose.PutRecordBatchResponseEntry{ErrorCode: &emptyString, ErrorMessage: &emptyString, RecordId: &emptyString}
	batchOutput := firehose.PutRecordBatchOutput{FailedPutCount: &zero, RequestResponses: []*firehose.PutRecordBatchResponseEntry{&entry}}
	return &batchOutput, nil
}

func generateLines(numLines int) (lines []telegraf.Metric, err error) {
	err = nil
	lines = testutil.MockMetrics()

	// generate 1 less line then specified since
	// the MockMetrics line returns a line when generated
	for i := 0; i < (numLines-1); i++ {
		lines = append(lines, testutil.TestMetric(1.0))
	}
	return
}

func TestWriteToFirehoseAllSuccess(t *testing.T) {
	f := FirehoseOutput{}
	f.svc = mockFirehose{numErrors: 0}

	generatedLines, err := generateLines(10)
	if err == nil {
		f.Write(generatedLines)
	}
	fmt.Println(f.svc.numOfPuts)
}

//
//func TestWriteToFirehoseSomeFail(t *testing.T) {
//
//}
//
//func TestWriteToFirehoseSuccess(t *testing.T) {
//
//}
//
//func TestWriteLessThan500Records(t *testing.T) {
//
//}
//
//func TestWrite500Records(t *testing.T) {
//
//}
