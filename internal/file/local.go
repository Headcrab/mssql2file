package file

import "os"

type HDDLocation string

func (h HDDLocation) Open() (File, error) {
	file, err := os.Open(string(h))
	return file, err
}

func (h HDDLocation) Create() (File, error) {
	file, err := os.Create(string(h))
	return file, err
}

func (h HDDLocation) Type() int {
	return LOC_HDD
}
