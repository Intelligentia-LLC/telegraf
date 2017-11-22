package firehose

import (
	"testing"
	"fmt"
	//"github.com/aws/aws-sdk-go/aws/request"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/testutil"
  "github.com/influxdata/telegraf/plugins/serializers"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
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
func (m *mockFirehose) PutRecordBatch(input *firehose.PutRecordBatchInput) (output *firehose.PutRecordBatchOutput, err error) {
	m.numOfPuts++

	emptyString := ""

	var zero int64 = m.numErrors
	// TODO insert the errors.

	// TODO check for the number of expected lines

	entry := firehose.PutRecordBatchResponseEntry{ ErrorCode: &emptyString,
                                                 ErrorMessage: &emptyString,
                                                 RecordId: &emptyString }

	batchOutput := firehose.PutRecordBatchOutput{ FailedPutCount: &zero,
                                                RequestResponses: []*firehose.PutRecordBatchResponseEntry{ &entry } }
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

// TODO implement deliberate errors with PutRecordBatch
func mockRun(N int, E int64) error {
  if E > N {
    return errors.New("More errors than records")
  }
  f := FirehoseOutput{}
	f.svc = mockFirehose{ numErrors: E }
  s, err := serializers.NewInfluxSerializer()
  if err == nil {
    f.SetSerializer(s)
  }
  else {
    t.Fail(err)
    return err
  }
  generatedLines, err := generateLines(N)
	if err == nil {
		err = f.Write(generatedLines)
    if err != nil {
      t.Fail(err)
      return err
    }
	}
  else {
    t.Fail(err)
    return err
  }
  return nil
}

// to be removed; reimplemented in suite function below
func TestWriteToFirehoseAllSuccess(t *testing.T) {
	err := mockRun(10,0)
  if err != nil {
    t.Fail(err)
  }
}

// func TestWriteRecords(t *testing.T) {
//   t.Run("writes"), func(t *testing.T) {
//     t.Run("N=1",    func(t *testing.T) { mockRun(1,0) })      // one record, no failures
//     t.Run("N=500",  func(t *testing.T) { mockRun(500,0) })    // 500 records, no failures
//     t.Run("N=1000", func(t *testing.T) { mockRun(1000,0) })   // >500 records, no failures
//   }
//   t.Run("retries"), func(t *testing.T) {
//     t.Run("E=1",    func(t *testing.T) { mockRun(1,1) })      // 1 record, all failures
//     t.Run("E=2",    func(t *testing.T) { mockRun(2,1) })      // multiple records, one failure
//     t.Run("E=N",    func(t *testing.T) { mockRun(2,2) })      // multiple records, all failures
//   }
//}
