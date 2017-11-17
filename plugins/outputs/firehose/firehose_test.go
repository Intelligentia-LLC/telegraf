package firehose

import (
	"testing"
	"fmt"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
	//"github.com/influxdata/telegraf/testutil"
	//uuid "github.com/satori/go.uuid"
	//"github.com/stretchr/testify/assert"
)

//req, resp := f.svc.PutRecordBatchRequest(batchInput)

type mockFirehose struct {
	firehoseiface.FirehoseAPI
	Resp firehose.PutRecordBatchOutput
}

// Overriding this here so we can do unit testing of firehose puts without
// actually engaging the AWS API
func (m *mockFirehose) PutRecordBatch(input *firehose.PutRecordBatchInput) (output *firehose.PutRecordBatchOutput, error) {
	emptyString := ""
	recordId := "ljq33ah4tlkjk34"

	var zero int64 = 0

	entry := firehose.PutRecordBatchResponseEntry{ErrorCode: &emptyString, ErrorMessage: &emptyString, RecordId: &recordId}
	reqOutput := new request.Request{}
	batchOutput := firehose.PutRecordBatchOutput{FailedPutCount: &zero, RequestResponses: []*firehose.PutRecordBatchResponseEntry{&entry}}
	return &reqOutput, &batchOutput
}

//func TestWriteToFirehoseAllFail(t *testing.T) {
//	f := FirehoseOutput{}
//	fmt.Println(f.DeliveryStreamName)
//}
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
