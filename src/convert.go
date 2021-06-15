package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/moov-io/ach"

	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

// logic to be executed when lambda starts goes here
func Handler(ctx context.Context, s3Event events.S3Event) {
	region := s3Event.Records[0].AWSRegion
	bucket := s3Event.Records[0].S3.Bucket.Name
	key := s3Event.Records[0].S3.Object.Key
	newKey := key + ".json"

	ifile, err := ioutil.TempFile("", "ach-ifile-")
	check(err)
	defer os.Remove(ifile.Name())
	defer ifile.Close()

	ofile, err := ioutil.TempFile("", "ach-ofile-")
	check(err)
	defer ofile.Close()
	defer os.Remove(ofile.Name())

	sess, _ := session.NewSession(
		&aws.Config{
			Region: aws.String(region),
	})

	downloader := s3manager.NewDownloader(sess)
	numBytes, err := downloader.Download(ifile,
		&s3.GetObjectInput {
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
	})
	check(err)

	fmt.Printf("Downloaded file=%s bytes=%d\n", ifile.Name(), uint64(numBytes))

	rfile, err := os.Open(ifile.Name())
	check(err)

	r := ach.NewReader(rfile)
	achFile, err := r.Read()
	check(err)

	err = achFile.Create();
	check(err)

	bs, err := json.Marshal(achFile)
	check(err)

	err = ioutil.WriteFile(ofile.Name(), bs, 0644)
	check(err)

	fmt.Printf("Converted from=%s to=%s\n", ifile.Name(), ofile.Name())

	uploader := s3manager.NewUploader(sess)
	_, err = uploader.Upload(
		&s3manager.UploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(newKey),
			Body:   ofile,
	})
	check(err)

	fmt.Printf("Uploaded file=%s bucket=%s key=%s\n", ofile.Name(), bucket, newKey)
}

func check(e error) {
    if e != nil {
        panic(e)
    }
}

func main() {
	lambda.Start(Handler)
}
