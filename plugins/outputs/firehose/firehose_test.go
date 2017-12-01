package firehose

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	//"log"
	"strconv"
	"testing"
	//"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/serializers"
	"github.com/influxdata/telegraf/testutil"
	//"github.com/stretchr/testify/assert"
	//uuid "github.com/satori/go.uuid"
)

// primary key spoof
var recordID int64 = 0

//req, resp := f.svc.PutRecordBatchRequest(batchInput)
type mockFirehose struct {
	firehoseiface.FirehoseAPI

	// a link to the test interface to quickly fail tests
	t *testing.T

	// test values
	expectedLines int64
	numErrors     int64

	// reaction values
	errorsRemain bool  // tracks when the errors have finished being injected
	numOfPuts    int64 // tracks the number of times PutRecordBatch was called
}

// Overriding this here so we can do unit testing of firehose puts without
// actually engaging the AWS API
// TODO check for only <=500 records. This doesn't have access to t *testing.T
// so this check may need to happen higher up the execution chain via m.numOfPuts
// since errors (err) are eaten by the caller writeToFirehose
func (m *mockFirehose) PutRecordBatch(input *firehose.PutRecordBatchInput) (output *firehose.PutRecordBatchOutput, err error) {
	if len(input.Records) > 500 {
		m.t.Log("E! firehose_test: got more than 500 in a batch")
		m.t.Fail()
	}
	m.numOfPuts++

	code := "42"
	message := "Deliberate Error!"
	errCount := m.numErrors

	// insert errors
	var entry firehose.PutRecordBatchResponseEntry
	var responses []*firehose.PutRecordBatchResponseEntry

	// create responses for output and process errors
	for index, _ := range input.Records { // TODO maybe something other than a range? range returns an int32
		recordID++
		if m.errorsRemain && int64(index) < errCount {
			// inserting errors at first for specified error count
			entry = firehose.PutRecordBatchResponseEntry{
				ErrorCode:    &code,
				ErrorMessage: &message}
		} else {
			m.errorsRemain = false
			idString := strconv.FormatInt(recordID, 10)
			entry = firehose.PutRecordBatchResponseEntry{RecordId: &idString}
		}
		responses = append(responses, &entry)
	}

	batchOutput := firehose.PutRecordBatchOutput{FailedPutCount: &errCount, RequestResponses: responses}

	// TODO should this be at the top of the function to short-circuit other logic?
	if errCount-(500*(m.numOfPuts-1)) >= 500 {
		// simulated error, eaten by writeToFirehose (calling function)
		err = errors.New("Total Failure Simulated")
	}

	return &batchOutput, err
}

// generateLines simply uses the built-in telegraf testing functions to
// create some metrics in the correct format and structure.  We did this 
// little wrapper to quickly spin up any number of metrics.
func generateLines(numLines int64) (lines []telegraf.Metric, err error) {
	err = nil
	lines = testutil.MockMetrics()

	// generate 1 less line then specified since
	// the MockMetrics line returns a line when generated
	for i := int64(0); i < (numLines - 1); i++ {
		lines = append(lines, testutil.TestMetric(1.0))
	}
	return
}

// attachSerializer quickly creates a new serializer for handling 
// metrics from telegraf. Nothing fancy here, just a new serializer
// and added it into our firehose output struct.
func attachSerializer(t *testing.T, f *FirehoseOutput) (err error) {
	s, err := serializers.NewInfluxSerializer()
	if err != nil {
		t.Fail()
		return
	}
	f.SetSerializer(s)
	return
}

// checkBuffer takes a quick look at how many firehose record errors were
// handled by the code.  It will error out if the number of errors found
// are not equal to the number of errors expected.
func checkBuffer(t *testing.T, f *FirehoseOutput, numErrors int64) (err error) {
	count := int64(len(f.errorBuffer))
	if count != numErrors {
		err = errors.New(fmt.Sprintf("Got buffer length of %d, expected %d", count, numErrors))
		t.Fail()
	}
	return
}

// testWrapper is a simple wrapper to add some test cases we always want to
// check when executing the code.
func testWrapper(t *testing.T, f *FirehoseOutput, numMetrics int64, numErrors int64) {
	if numErrors > numMetrics {
		errors.New("More errors than records")
		t.Fail()
	}

	//err := mockRun(t, f, numMetrics, numErrors) // run one and total fail
	//if err != nil {
	//	t.Fail()
	//}

	//assert.Equal(t, f.errorsRemain, 0)
	checkBuffer(t, f, numErrors)
}

func initFirehose(t *testing.T, m *mockFirehose) (f *FirehoseOutput) {
	f = &FirehoseOutput{}
	f.svc = m
	attachSerializer(t, f)
	return f
}

// TestWriteToFirehoseAllSuccess tests the case when no error is returned
// from our mock AWS firehose function.
func TestWriteToFirehoseAllSuccess(t *testing.T) {
	m := &mockFirehose{
		numErrors:    0,
		errorsRemain: false,
		t:            t,
	}
	f := initFirehose(t, m)

	generatedLines, err := generateLines(10)
	if err != nil {
		t.FailNow()
	}

	err = f.Write(generatedLines)
	assert.Equal(t, m.numOfPuts, int64(1))
}


// TestWriteToFirehoseAllSuccess tests the case when no error is returned
// from our mock AWS firehose function.
func TestWriteToFirehose500AllSuccess(t *testing.T) {
	m := &mockFirehose{
		numErrors:    0,
		errorsRemain: false,
		t:            t,
	}
	f := initFirehose(t, m)

	generatedLines, err := generateLines(500)
	if err != nil {
		t.FailNow()
	}

	err = f.Write(generatedLines)
	assert.Equal(t, m.numOfPuts, int64(1))
}


// TestWriteToFirehoseAllSuccess tests the case when no error is returned
// from our mock AWS firehose function.
func TestWriteToFirehose550AllSuccess(t *testing.T) {
	m := &mockFirehose{
		numErrors:    0,
		errorsRemain: false,
		t:            t,
	}
	f := initFirehose(t, m)

	generatedLines, err := generateLines(550)
	if err != nil {
		t.FailNow()
	}

	err = f.Write(generatedLines)
	assert.Equal(t, m.numOfPuts, int64(2))
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
