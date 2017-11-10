package firehose

import (
	"log"
	//"os"
	"time"

	"github.com/aws/aws-sdk-go/service/firehose"
	//"github.com/satori/go.uuid"

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

		DeliveryStreamName       string     `toml:"delivery_stream_name"`
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
  delivery_stream_name = "FirehoseName"

  ## Data format to output.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md
  data_format = "influx"

  ## debug will show upstream aws messages.
  debug = false
`

func (f *FirehoseOutput) SampleConfig() string {
	return sampleConfig
}

func (f *FirehoseOutput) Description() string {
	return "Configuration for the AWS Firehose output."
}


func (f *FirehoseOutput) Connect() error {
	// We attempt first to create a session to Firehose using an IAMS role, if that fails it will fall through to using
	// environment variables, and then Shared Credentials.
	if f.Debug {
		log.Printf("E! firehose: Building a session for connection to Firehose in %+v", f.Region)
	}

	credentialConfig := &internalaws.CredentialConfig{
		Region:    f.Region,
		AccessKey: f.AccessKey,
		SecretKey: f.SecretKey,
		RoleARN:   f.RoleARN,
		Profile:   f.Profile,
		Filename:  f.Filename,
		Token:     f.Token,
	}
	configProvider := credentialConfig.Credentials()

	// we simply create the skeleton here. AWS doesn't attempt
	// any connection yet, so we don't know if an error will happen.
	svc := firehose.New(configProvider)
	f.svc = svc
	return nil
}

func (f *FirehoseOutput) Close() error {
	return nil
}

func (f *FirehoseOutput) SetSerializer(serializer serializers.Serializer) {
	f.serializer = serializer
}

// TODO this function is where the actual upload happens.
// do we also want to do our error handling here? -Doran
func writeToFirehose(f *FirehoseOutput, r []*firehose.Record) time.Duration {
	start := time.Now()
	batchInput := &firehose.PutRecordBatchInput{}
	batchInput.SetDeliveryStreamName(f.DeliveryStreamName)
	batchInput.SetRecords(r)

	req, resp := f.svc.PutRecordBatchRequest(batchInput)
	err := req.Send()

	if err != nil {
		log.Printf("E! firehose: Unable to write to Firehose : %+v \n", err.Error())
	}
	if f.Debug {
		log.Printf("E! %+v \n", resp)

	}
	return time.Since(start)
}

// send( errorCount int, metrics []telegraf.Metric )
//  if there are errors, a new []telegraf.Metric that failed, increase error count by 1, and re-send as batch later.

// errorList[
//   { errorCount: 1, metrics: [..., ..., ...] },
//   { errorCount: 2, metrics: [..., ..., ..., ...]} 
// ]

func (f *FirehoseOutput) Write(metrics []telegraf.Metric) error {
	var sz uint32

	if len(metrics) == 0 {
		return nil
	}

	r := []*firehose.Record{}
	for _, metric := range metrics {
		sz++

		values, err := f.serializer.Serialize(metric)
		if err != nil {
			return err
		}

		d := firehose.Record{
			Data:         values,
		}

		r = append(r, &d)

		if sz == 500 {
			// Max Messages Per PutRecordRequest is 500
			elapsed := writeToFirehose(f, r)
			log.Printf("E! Wrote a %+v point batch to Firehose in %+v.\n", sz, elapsed)
			sz = 0
			r = nil
		}

	}
	if sz > 0 {
		elapsed := writeToFirehose(f, r)
		log.Printf("E! Wrote a %+v point batch to Firehose in %+v.\n", sz, elapsed)
	}

	return nil
}

func init() {
	outputs.Add("firehose", func() telegraf.Output {
		return &FirehoseOutput{}
	})
}
