package collections

import (
	"io/ioutil"
	"os"
	"strings"
)

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
