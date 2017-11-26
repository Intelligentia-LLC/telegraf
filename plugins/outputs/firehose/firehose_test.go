package firehose

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"testing"
	//"github.com/aws/aws-sdk-go/aws/request"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/testutil"
	"github.com/influxdata/telegraf/plugins/serializers"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
	//"github.com/stretchr/testify/assert"
	//uuid "github.com/satori/go.uuid"
)

// primary key spoof
var recordID int = 0

//req, resp := f.svc.PutRecordBatchRequest(batchInput)
type mockFirehose struct {
	firehoseiface.FirehoseAPI

	// test values
	expectedLines int64
	numErrors int64

	// reaction values
	errorsRemain bool  // tracks when the errors have finished being injected
	numOfPuts int64    // tracks the number of times PutRecordBatch was called
}

// Overriding this here so we can do unit testing of firehose puts without
// actually engaging the AWS API
// TODO check for only <=500 records. This doesn't have access to t *testing.T
// so this check may need to happen higher up the execution chain via m.numOfPuts
// since errors (err) are eaten by the caller writeToFirehose
func (m *mockFirehose) PutRecordBatch(input *firehose.PutRecordBatchInput) (output *firehose.PutRecordBatchOutput, err error) {
	if len(input.Records) > 500 {
		log.Printf("E! firehose_test: got more than 500 in a batch")
	}
	m.numOfPuts++

	code := "42"
	message := "Deliberate Error!"
	errCount := m.numErrors
	// insert errors
	var entry firehose.PutRecordBatchResponseEntry
	var responses []*firehose.PutRecordBatchResponseEntry


	// create responses for output and process errors
	for index, element := range input.Records {
		recordID++
		if m.errorsRemain && int64(index) < errCount {
			// inserting errors at first for specified error count
			entry = firehose.PutRecordBatchResponseEntry{
				ErrorCode: &code,
				ErrorMessage: &message }
		} else {
			m.errorsRemain = false
			idString := strconv.Itoa(recordID)
			entry = firehose.PutRecordBatchResponseEntry{ RecordId: &idString }
		}
		responses = append(responses, &entry)
	}

	batchOutput := firehose.PutRecordBatchOutput{ FailedPutCount: &errCount, RequestResponses: responses }

	if errCount - (500 * (m.numOfPuts - 1)) >= 500 {
		// simulated error, eaten by writeToFirehose (calling function)
		err = errors.New("Total Failure Simulated")
	}

	return &batchOutput, err
}

// TODO is err needed? check MockMetrics?
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

//
func mockRun(f *FirehoseOutput, N int, E int64) (err error) {
	if E > int64(N) {
		return errors.New("More errors than records")
	}
	f.svc = mockFirehose{ numErrors: E, errorsRemain: true } // want bool(E)
	s, err := serializers.NewInfluxSerializer()
	if err != nil {
		return
	}
	f.SetSerializer(s)
	generatedLines, err := generateLines(N)
	if err != nil {
		return
	}
	err = f.Write(generatedLines)
}

func checkBuffer(f *FirehoseOutput, E int64) (err error) {
	count := len(f.errorBuffer)
	if count != E {
		err = errors.New(fmt.Sprintf("Got buffer length of %d, expected %d", count, E))
	}
}

func testWrapper(t *testing.T, f *FirehoseOutput, N int, E int64) {
	err = mockRun(f, N, E) // run one and total fail
	if err != nil {
		t.Fail(err)
	}
	err = checkBuffer(f, E)
	if err != nil {
		t.Fail(err)
	}
}

// to be removed; reimplemented in suite function below
func TestWriteToFirehoseAllSuccess(t *testing.T) {
	f := FirehoseOutput{}
	testWrapper(t, f, 10, 0)
}

//
// func TestWriteRecords(t *testing.T) {
// 	// create shared firehose output
// 	//f := FirehoseOutput{}
// 	// TODO create tests that share f (via errorBuffer)
// 	t.Run("simple_total", func(t *testing.T) {
// 		f := FirehoseOutput{}
// 		testWrapper(t, f, 1, 1)
// 		testWrapper(t, f, 2, 1)
// 		testWrapper(t, f, 0, 0)
// 	}
//
// }
