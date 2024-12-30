package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type DownloadSegment struct {
	start, end, index int64
	retries           int
}

type DownloadResult struct {
	index    int64
	duration float64
	err      error
}

const (
	segments   = 16          // Increased number of segments
	bufferSize = 1024 * 1024 // 1MB buffer size
	maxWorkers = 32          // Maximum concurrent workers
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: goload-manager <download_url>")
		return
	}

	urlPath := os.Args[1]

	resp, err := http.Head(urlPath)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	if resp.StatusCode != http.StatusOK {
		fmt.Println("Error: unable to access file")
		return
	}

	size, err := strconv.Atoi(resp.Header.Get("Content-Length"))
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Extract the file name from the URL
	fileName := path.Base(strings.Split(urlPath, "?")[0])
	fileName, err = url.QueryUnescape(fileName)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("Decoded file name:", fileName)

	fmt.Printf("Starting download of %.2f MB\n", float64(size)/1024/1024)

	startTime := time.Now() // Start time for the entire download

	// Generate download segments
	partSize := int64(size) / segments
	var downloadSegments []DownloadSegment
	for i := 0; i < segments; i++ {
		start := int64(i) * partSize
		end := start + partSize - 1
		if i == segments-1 {
			end = int64(size) - 1
		}
		downloadSegments = append(downloadSegments, DownloadSegment{start: start, end: end, index: int64(i)})
	}

	downloadWithWorkerPool(downloadSegments, urlPath)

	totalDuration := time.Since(startTime).Seconds() // Calculate total duration
	fmt.Printf("Total download time: %.2f seconds\n", totalDuration)

	mergeFiles(segments, fileName)
	fmt.Println("Download completed")
}

func downloadWithWorkerPool(segments []DownloadSegment, urlPath string) {
	jobs := make(chan DownloadSegment, len(segments))
	results := make(chan DownloadResult, len(segments))
	progress := make(chan int64, bufferSize)

	// Dynamic worker pool size based on CPU cores
	workerCount := min(runtime.NumCPU()*2, maxWorkers)

	// Start progress tracker
	go trackProgress(progress, segments)

	// Start worker pool
	for w := 1; w <= workerCount; w++ {
		go worker(jobs, results, progress, urlPath)
	}

	// Send jobs to workers
	for _, segment := range segments {
		jobs <- segment
	}
	close(jobs)

	// Collect results
	for range segments {
		result := <-results
		if result.err != nil {
			log.Printf("Segment %d failed: %v, retrying...", result.index, result.err)
			if segments[result.index].retries < 3 {
				segments[result.index].retries++
				jobs <- segments[result.index]
			}
		}
	}
}

func worker(jobs <-chan DownloadSegment, results chan<- DownloadResult, progress chan<- int64, urlPath string) {
	for job := range jobs {
		startTime := time.Now()

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		// Download with context and retry
		err := downloadSegmentWithContext(ctx, job.start, job.end, job.index, progress, urlPath)
		duration := time.Since(startTime).Seconds()
		cancel()

		results <- DownloadResult{
			index:    job.index,
			duration: duration,
			err:      err,
		}
	}
}

func downloadSegmentWithContext(ctx context.Context, start, end, index int64, progress chan<- int64, urlPath string) error {
	req, err := http.NewRequestWithContext(ctx, "GET", urlPath, nil)
	if err != nil {
		return err
	}

	// Add range header
	rangeHeader := fmt.Sprintf("bytes=%d-%d", start, end)
	req.Header.Add("Range", rangeHeader)

	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:       100,
			IdleConnTimeout:    90 * time.Second,
			DisableCompression: true, // For range requests
		},
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Write to file with buffered writer
	f, err := os.Create(fmt.Sprintf("segment_%d", index))
	if err != nil {
		return err
	}
	defer f.Close()

	// Use buffered reader/writer for better performance
	reader := bufio.NewReaderSize(resp.Body, bufferSize)
	writer := bufio.NewWriterSize(f, bufferSize)

	buf := make([]byte, 32*1024) // 32KB chunks
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			_, werr := writer.Write(buf[:n])
			if werr != nil {
				return werr
			}
			progress <- int64(n) // Update progress
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	return writer.Flush()
}

func mergeFiles(parts int, fileName string) error {
	out, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer out.Close()

	writer := bufio.NewWriterSize(out, bufferSize)

	// Use worker pool for concurrent merging
	errChan := make(chan error, parts)
	sem := make(chan struct{}, runtime.NumCPU())

	for i := 0; i < parts; i++ {
		sem <- struct{}{} // Acquire semaphore
		go func(index int) {
			defer func() { <-sem }() // Release semaphore

			partFile, err := os.Open(fmt.Sprintf("segment_%d", index))
			if err != nil {
				errChan <- err
				return
			}
			defer partFile.Close()

			reader := bufio.NewReaderSize(partFile, bufferSize)
			buf := make([]byte, 32*1024)
			_, err = io.CopyBuffer(writer, reader, buf)
			if err != nil {
				errChan <- err
				return
			}

			os.Remove(fmt.Sprintf("segment_%d", index))
			errChan <- nil
		}(i)
	}

	// Wait for all merges to complete
	for i := 0; i < parts; i++ {
		if err := <-errChan; err != nil {
			return err
		}
	}

	return writer.Flush()
}

func trackProgress(progress chan int64, segments []DownloadSegment) {
	var totalBytes int64
	for _, seg := range segments {
		totalBytes += seg.end - seg.start + 1
	}

	var downloadedBytes int64
	var lastDownloadedBytes int64
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case bytes := <-progress:
			downloadedBytes += bytes
		case <-ticker.C:
			percentage := float64(downloadedBytes) / float64(totalBytes) * 100
			speed := float64(downloadedBytes-lastDownloadedBytes) / 1024 // KB/s
			lastDownloadedBytes = downloadedBytes
			fmt.Printf("\rProgress: %.2f%% [%s] Speed: %.2f KB/s", percentage, getProgressBar(percentage, 50), speed)
		}
	}
}

func getProgressBar(percentage float64, width int) string {
	filled := int((percentage / 100) * float64(width))
	bar := strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
	return bar
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
