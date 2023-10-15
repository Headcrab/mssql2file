package file

import (
	"mssql2file/internal/apperrors"
)

const (
	LOC_HDD = iota
	LOC_FTP
	LOC_SMB
)

type File interface {
	Close() error
	Name() string
}

type FileLocation interface {
	Open() (File, error)
	Create() (File, error)
	Type() int
}

var fileLocations = make(map[string]func() FileLocation)

func RegisterFileLocation(locType string, loc func() FileLocation) {
	fileLocations[locType] = loc
}

func NewFileLocation(locType string) (FileLocation, error) {
	loc, ok := fileLocations[locType]
	if !ok {
		return nil, apperrors.New(apperrors.UnsupportedLocationType, locType)
	}
	return loc(), nil
}
