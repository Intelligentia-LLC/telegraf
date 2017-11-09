package firehose

import (
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/satori/go.uuid"

	"github.com/influxdata/telegraf"
	internalaws "github.com/influxdata/telegraf/internal/config/aws"
	"github.com/influxdata/telegraf/plugins/outputs"
	"github.com/influxdata/telegraf/plugins/serializers"
)

type (
	FirehoseOutput struct {
		Region    string `toml:"region"`
		AccessKey string `toml:"access_key"`
		SecretKey string `toml:"secret_key"`
		RoleARN   string `toml:"role_arn"`
		Profile   string `toml:"profile"`
		Filename  string `toml:"shared_credential_file"`
		Token     string `toml:"token"`

		FirehoseName       string     `toml:"firehosename"`
		Debug              bool       `toml:"debug"`
		svc                *firehose.Firehose
		// TODO add our error/retry array/slice/thing here.

		serializer serializers.Serializer
	}
)

var sampleConfig = `
  ## Amazon REGION of the AWS firehose endpoint.
  region = "us-east-2"

  ## Amazon Credentials
  ## Credentials are loaded in the following order
  ## 1) Assumed credentials via STS if role_arn is specified
  ## 2) explicit credentials from 'access_key' and 'secret_key'
  ## 3) shared profile from 'profile'
  ## 4) environment variables
  ## 5) shared credentials file
  ## 6) EC2 Instance Profile
  #access_key = ""
  #secret_key = ""
  #token = ""
  #role_arn = ""
  #profile = ""
  #shared_credential_file = ""

  ## Firehose StreamName must exist prior to starting telegraf.
  firehosename = "FirehoseName"

  ## Data format to output.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md
  data_format = "influx"

  ## debug will show upstream aws messages.
  debug = false
`

func (k *FirehoseOutput) SampleConfig() string {
	return sampleConfig
}

func (k *FirehoseOutput) Description() string {
	return "Configuration for the AWS Firehose output."
}

// Commenting this out because it would be best not to need the list firehoses permission. -Doran
//
//func checkstream(l []*string, s string) bool {
	// Check if the StreamName exists in the slice returned from the ListStreams API request.
//	for _, stream := range l {
//		if *stream == s {
//			return true
//		}
//	}
//	return false
//}

func (k *FirehoseOutput) Connect() error {
	// We attempt first to create a session to Firehose using an IAMS role, if that fails it will fall through to using
	// environment variables, and then Shared Credentials.
	if k.Debug {
		log.Printf("E! kinesis: Building a session for connection to Firehose in %+v", k.Region)
	}

	credentialConfig := &internalaws.CredentialConfig{
		Region:    k.Region,
		AccessKey: k.AccessKey,
		SecretKey: k.SecretKey,
		RoleARN:   k.RoleARN,
		Profile:   k.Profile,
		Filename:  k.Filename,
		Token:     k.Token,
	}
	configProvider := credentialConfig.Credentials()

	// we simply create the skeleton here. AWS doesn't attempt
	// any connection yet, so we don't know if an error will happen.
	svc := firehose.New(configProvider)
	k.svc = svc
	return nil
}

func (k *FirehoseOutput) Close() error {
	return nil
}

func (k *FirehoseOutput) SetSerializer(serializer serializers.Serializer) {
	k.serializer = serializer
}

// TODO this function is where the actual upload happens.
//      do we also want to do our error handling here? -Doran
func writefirehose(k *FirehoseOutput, r []*kinesis.PutRecordsRequestEntry) time.Duration {
	start := time.Now()
	payload := &kinesis.PutRecordsInput{
		Records:    r,
		StreamName: aws.String(k.StreamName),
	}

	if k.Debug {
		resp, err := k.svc.PutRecords(payload)
		if err != nil {
			log.Printf("E! kinesis: Unable to write to Kinesis : %+v \n", err.Error())
		}
		log.Printf("E! %+v \n", resp)

	} else {
		_, err := k.svc.PutRecords(payload)
		if err != nil {
			log.Printf("E! kinesis: Unable to write to Kinesis : %+v \n", err.Error())
		}
	}
	return time.Since(start)
}



// send( errorCount int, metrics []telegraf.Metric )
//  if there are errors, a new []telegraf.Metric that failed, increase error count by 1, and re-send as batch later.

// errorList[
//   { errorCount: 1, metrics: [..., ..., ...] },
//   { errorCount: 2, metrics: [..., ..., ..., ...]} 
// ]


func (k *FirehoseOutput) Write(metrics []telegraf.Metric) error {
	var sz uint32

	if len(metrics) == 0 {
		return nil
	}

	r := []*kinesis.PutRecordsRequestEntry{}
//Doran is here.

	for _, metric := range metrics {
		sz++

		values, err := k.serializer.Serialize(metric)
		if err != nil {
			return err
		}

		d := kinesis.PutRecordsRequestEntry{
			Data:         values,
			PartitionKey: aws.String(partitionKey),
		}

		r = append(r, &d)

		if sz == 500 {
			// Max Messages Per PutRecordRequest is 500
			elapsed := writekinesis(k, r)
			log.Printf("E! Wrote a %+v point batch to Kinesis in %+v.\n", sz, elapsed)
			sz = 0
			r = nil
		}

	}
	if sz > 0 {
		elapsed := writekinesis(k, r)
		log.Printf("E! Wrote a %+v point batch to Kinesis in %+v.\n", sz, elapsed)
	}

	return nil
}

func init() {
	outputs.Add("kinesis", func() telegraf.Output {
		return &KinesisOutput{}
	})
}
