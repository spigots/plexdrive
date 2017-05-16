package main

import (
	"fmt"
	"io"
	"io/ioutil"

	. "github.com/claudetech/loggo/default"
)

// DownloadRequest represents exactly one download request
type DownloadRequest struct {
	reader   io.ReadCloser
	response chan []byte
	errors   chan error
}

// Downloader is a global download handler for rate limiting
type Downloader struct {
	concurrentDownloads int64
	speedLimit          int64
	queue               chan DownloadRequest
}

var downloader *Downloader

// InitDownloader initializes the downloader
func InitDownloader(concurrentDownloads, speedLimit int64) error {
	downloader = &Downloader{
		concurrentDownloads: concurrentDownloads,
		speedLimit:          speedLimit,
	}
	go downloader.processQueue()
	return nil
}

// GetDownloader gets the downloader singleton
func GetDownloader() *Downloader {
	return downloader
}

// Download queues a download in the downloading queue
func (d *Downloader) Download(reader io.ReadCloser) (chan []byte, chan error) {
	byteChannel := make(chan []byte)
	errorChannel := make(chan error)

	d.queue <- DownloadRequest{
		reader:   reader,
		response: byteChannel,
		errors:   errorChannel,
	}

	return byteChannel, errorChannel
}

func (d *Downloader) processQueue() {
	for {
		request := <-d.queue

		bytes, err := ioutil.ReadAll(request.reader)
		if nil != err {
			Log.Debugf("%v", err)
			sendResponse(&request, nil, fmt.Errorf("Could not download chunk"))
		}
		sendResponse(&request, bytes, nil)
	}
}

func sendResponse(request *DownloadRequest, response []byte, err error) {
	request.response <- response
	request.errors <- err
}
