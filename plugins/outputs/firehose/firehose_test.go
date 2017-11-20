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
	numErrors int64
}

type mockMetrics struct {
	numLines int64
}

// Overriding this here so we can do unit testing of firehose puts without
// actually engaging the AWS API
func (m mockFirehose) PutRecordBatch(input *firehose.PutRecordBatchInput) (output *firehose.PutRecordBatchOutput, err error) {
	emptyString := ""
	recordId := "ljq33ah4tlkjk34"

	var zero int64 = m.numErrors

	entry := firehose.PutRecordBatchResponseEntry{ErrorCode: &emptyString, ErrorMessage: &emptyString, RecordId: &recordId}
	batchOutput := firehose.PutRecordBatchOutput{FailedPutCount: &zero, RequestResponses: []*firehose.PutRecordBatchResponseEntry{&entry}}
	return &batchOutput, nil
}

func (m mockLines) generateLines() (lines []telegraf.Metric, err error) {
	err = nil
	lines = testutil.MockMetrics()
	lines = append(lines, testutil.TestMetric(1.0))
}

func TestWriteToFirehoseAlliSuccess(t *testing.T) {
	f := FirehoseOutput{}
	f.svc = mockFirehose{numErrors: 0}
	fmt.Println(f.DeliveryStreamName)
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
