package collections

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"
)

func formatNumber(number int64) string {
	output := fmt.Sprintf("%v", number)
	startOffset := 3
	if number < 0 {
		startOffset++
	}
	for outputIndex := len(output); outputIndex > startOffset; {
		outputIndex -= 3
		output = output[:outputIndex] + "," + output[outputIndex:]
	}
	return output
}

// convertStringToTime --
// dateTimeString => 2020-06-22T10:20:38
// on error it returns: 0001-01-01T00:00:00.000Z
func convertStringToTime(dateTimeString string) (time.Time, error) {

	var s string

	dateTimeString = strings.Trim(dateTimeString, " ")

	if strings.Contains(dateTimeString, " ") {
		// Missing T
		v := strings.Split(dateTimeString, " ")
		v[1] = fmt.Sprintf("T%s", v[1])
		dateTimeString = strings.Join(v, "")
	}

	tEmpty, err := time.Parse(time.RFC3339, "0001-01-01T00:00:00.000Z")
	if err != nil {
		return tEmpty, err
	}
	s = strings.Replace(dateTimeString, " ", "T", 1)
	v := strings.Split(dateTimeString, ".")

	if len(v) > 1 {
		z := ""
		if len(v[1]) >= 4 {
			z = v[1][:3]
		} else {
			z = "000"
		}
		// check again
		if len(z) < 3 {
			z = "000"
		}
		s = fmt.Sprintf("%s.%sZ", v[0], z)
	} else {
		s = fmt.Sprintf("%s.000Z", s)
	}

	t, err := time.Parse(time.RFC3339, s)

	if err != nil {
		return tEmpty, err
	}

	return t, err
}

func removeItemFromArray(e []interface{}, i int) []interface{} {
	e[len(e)-1], e[i] = e[i], e[len(e)-1]
	return e[:len(e)-1]
}

// fileOrDirExists checks to see if a file or directory exists.
func fileOrDirExists(path string) bool {
	if path == "" {
		return false
	}
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}

	return true
}

// getFiles returns an array of files names found in a directory.
// To get all files leave the ext blank.
func getFiles(dirPath string, ext string) ([]string, error) {

	var fileNames []string

	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return fileNames, err
	}

	for i := 0; i < len(files); i++ {

		fn := files[i].Name()

		if files[i].IsDir() {
			continue
		}

		if ext != "" {
			v := strings.Split(fn, ".")
			if ext[1:] != v[len(v)-1] {
				continue
			}
		}

		fileNames = append(fileNames, fn)
	}

	return fileNames, nil
}
