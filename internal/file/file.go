package file

import (
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
	"mssql2file/internal/apperrors"
=======
	"mssql2file/internal/errors"
>>>>>>> e66dc11 (*ref)
=======
	apperrors "mssql2file/internal/errors"
>>>>>>> 252be83 (+ apperrors)
=======
	"mssql2file/internal/apperrors"
>>>>>>> 448a933 (app.ver added)
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
<<<<<<< HEAD
<<<<<<< HEAD
		return nil, apperrors.New(apperrors.UnsupportedLocationType, locType)
	}
	return loc(), nil
}
<<<<<<< HEAD
=======
		return nil, errors.New(errors.UnsupportedLocationType, locType)
=======
		return nil, apperrors.New(apperrors.UnsupportedLocationType, locType)
>>>>>>> 252be83 (+ apperrors)
	}
	return loc(), nil
}

// func main() {
// 	locations := []FileLocation{
// 		HDDLocation("/path/to/local/file"),
// 		FTPLocation{
// 			Addr: "ftp.example.com",
// 			User: "user",
// 			Pass: "pass",
// 			Path: "/path/to/remote/file",
// 		},
// 		SMBLocation{
// 			Addr:   "smb.example.com",
// 			User:   "user",
// 			Pass:   "pass",
// 			Path:   "/path/to/remote/file",
// 			IsIPv6: false,
// 		},
// 	}

// 	for _, loc := range locations {
// 		file, err := loc.Open()
// 		if err != nil {
// 			fmt.Println(err)
// 			continue
// 		}
// 		defer file.Close()

// 		fmt.Println("Opened file:", file.Name())
// 		fmt.Println("File location type:", loc.Type())
// 	}
// }
>>>>>>> e66dc11 (*ref)
=======
>>>>>>> aa201e5 (go-mssqldb moved)
