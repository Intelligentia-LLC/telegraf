package firehose

import (
	"testing"
	"fmt"

	//"github.com/influxdata/telegraf/testutil"
	//uuid "github.com/satori/go.uuid"
	//"github.com/stretchr/testify/assert"
)
g
func TestWriteToFirehoseAllFail(t *testing.T) {
	f := FirehoseOutput{}
	fmt.Println(f.DeliveryStreamName)
}

func TestWriteToFirehoseSomeFail(t *testing.T) {

}

func TestWriteToFirehoseSuccess(t *testing.T) {

}

func TestWriteLessThan500Records(t *testing.T) {

}

func TestWrite500Records(t *testing.T) {

}
