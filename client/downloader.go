package client

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

const defaultMockFilePrefix = "gocaves-"
const defaultMockUrlPrefix = "https://github.com/couchbaselabs/gocaves/releases/download/"

func downloadMock(version string) (path string, err error) {
	var mockUrlPrefix string
	if version == "" {
		mockUrlPrefix = "https://github.com/couchbaselabs/gocaves/releases/latest/download/"
	} else {
		mockUrlPrefix = defaultMockUrlPrefix + version + "/"
	}
	var binary string
	switch runtime.GOOS {
	case "darwin":
		binary = defaultMockFilePrefix + "macos"
	case "linux":
		switch runtime.GOARCH {
		case "amd64":
			binary = defaultMockFilePrefix + "linux-amd64"
		case "arm64":
			binary = defaultMockFilePrefix + "linux-arm64"
		}
	case "windows":
		binary = defaultMockFilePrefix + "windows.exe"
	}
	var url string
	if path = os.Getenv("GOCB_MOCK_PATH"); path == "" {
		path = strings.Join([]string{os.TempDir(), binary}, string(os.PathSeparator))
	} else {
		path = strings.Join([]string{path, binary}, string(os.PathSeparator))
	}
	path, err = filepath.Abs(path)
	if err != nil {
		throwMockError("Couldn't get absolute path (!)", err)
	}
	info, err := os.Stat(path)
	if err == nil && info.Size() > 0 {
		return path, nil
	} else if err != nil && !os.IsNotExist(err) {
		throwMockError("Couldn't resolve existing path", err)
	}
	if err := os.Remove(path); err != nil {
		log.Printf("Couldn't remove existing mock: %v", err)
	}
	log.Printf("Downloading %s to %s", url, path)
	if url = os.Getenv("GOCB_MOCK_URL"); url == "" {
		url = mockUrlPrefix + binary
	}
	// #nosec:G107
	resp, err := http.Get(url)
	if err != nil {
		throwMockError("Couldn't create HTTP request (or other error)", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Failed to close response body: %v", err)
		}
	}()
	if resp.StatusCode != 200 {
		throwMockError(fmt.Sprintf("Got HTTP %d from URL", resp.StatusCode), errors.New("non-200 response"))
	}
	out, err := os.Create(path)
	if err != nil {
		throwMockError("Couldn't open output file", err)
	}
	defer func() {
		if err := out.Close(); err != nil {
			log.Printf("Failed to close file: %v", err)
		}
	}()
	err = os.Chmod(path, 0755)
	if err != nil {
		throwMockError("Couldn't open output file", err)
	}
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		throwMockError("Couldn't write response", err)
	}
	return path, nil
}

type mockError struct {
	cause   error
	message string
}

func (e mockError) Error() string {
	return fmt.Sprintf("Mock Error: %s (caused by %s)", e.message, e.cause.Error())
}
func throwMockError(msg string, cause error) {
	if cause == nil {
		cause = errors.New("no cause")
	}
	panic(mockError{message: msg, cause: cause})
}
