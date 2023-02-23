package main

import (
	"fmt"
	"io"
	"os"

	"github.com/jlaffaye/ftp"
)

const (
	LOC_HDD = iota
	LOC_FTP
	LOC_SMB
)

type FileLocation interface {
	Open() (*os.File, error)
	Create() (*os.File, error)
	Type() int
}

type HDDLocation string

func (h HDDLocation) Open() (*os.File, error) {
	file, err := os.Open(string(h))
	return file, err
}

func (h HDDLocation) Create() (*os.File, error) {
	file, err := os.Create(string(h))
	return file, err
}

func (h HDDLocation) Type() int {
	return LOC_HDD
}

type FTPLocation struct {
	Addr string
	User string
	Pass string
	Path string
}

func (f FTPLocation) Open() (*os.File, error) {
	conn, err := ftp.Connect(f.Addr)
	if err != nil {
		return nil, err
	}

	err = conn.Login(f.User, f.Pass)
	if err != nil {
		return nil, err
	}

	res, err := conn.Retr(f.Path)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(f.Path)
	if err != nil {
		return nil, err
	}

	_, err = io.Copy(file, res)
	if err != nil {
		return nil, err
	}

	err = conn.Quit()
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (f FTPLocation) Create() (r *os.File, err error) {
	conn, err := ftp.Connect(f.Addr)
	if err != nil {
		return nil, err
	}

	err = conn.Login(f.User, f.Pass)
	if err != nil {
		return nil, err
	}

	err = conn.Stor(f.Path, r)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (f FTPLocation) Type() int {
	return LOC_FTP
}

type SMBLocation struct {
	Addr   string
	User   string
	Pass   string
	Path   string
	IsIPv6 bool
}

func (s SMBLocation) Open() (*os.File, error) {
	// TODO: Implement SMBLocation.Open()
	return nil, nil
}

func (s SMBLocation) Create() (*os.File, error) {
	// TODO: Implement SMBLocation.Create()
	return nil, nil
}

func (s SMBLocation) Type() int {
	return LOC_SMB
}

func main() {
	locations := []FileLocation{
		HDDLocation("/path/to/local/file"),
		FTPLocation{
			Addr: "ftp.example.com",
			User: "user",
			Pass: "pass",
			Path: "/path/to/remote/file",
		},
		SMBLocation{
			Addr:   "smb.example.com",
			User:   "user",
			Pass:   "pass",
			Path:   "/path/to/remote/file",
			IsIPv6: false,
		},
	}

	for _, loc := range locations {
		file, err := loc.Open()
		if err != nil {
			fmt.Println(err)
			continue
		}
		defer file.Close()

		fmt.Println("Opened file:", file.Name())
		fmt.Println("File location type:", loc.Type())
	}
}
