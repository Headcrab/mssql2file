package file

import (
	"io"
	"io/ioutil"
	"os"

	"github.com/jlaffaye/ftp"
)

type FTPLocation struct {
	Addr string
	User string
	Pass string
	Path string
}

func connectAndLoginFTP(addr, user, pass string) (*ftp.ServerConn, error) {
	conn, err := ftp.Connect(addr)
	if err != nil {
		return nil, err
	}

	err = conn.Login(user, pass)
	if err != nil {
		conn.Quit()
		return nil, err
	}

	return conn, nil
}

func (f FTPLocation) Open() (File, error) {
	conn, err := connectAndLoginFTP(f.Addr, f.User, f.Pass)
	if err != nil {
		return nil, err
	}
	defer conn.Quit()

	res, err := conn.Retr(f.Path)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	tmpfile, err := ioutil.TempFile("", "ftptmp")
	if err != nil {
		return nil, err
	}

	_, err = io.Copy(tmpfile, res)
	if err != nil {
		tmpfile.Close()
		os.Remove(tmpfile.Name())
		return nil, err
	}

	return tmpfile, nil
}

func (f FTPLocation) Create() (File, error) {
	conn, err := connectAndLoginFTP(f.Addr, f.User, f.Pass)
	if err != nil {
		return nil, err
	}

	r := os.NewFile(0, f.Path)
	err = conn.Stor(f.Path, r)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (f FTPLocation) Type() int {
	return LOC_FTP
}
