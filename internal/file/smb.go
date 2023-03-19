package file

import (
	"net"

	"github.com/hirochachacha/go-smb2"
)

type SMBLocation struct {
	Addr   string
	User   string
	Pass   string
	Path   string
	IsIPv6 bool
}

func connectAndLoginSMB(addr, user, pass string) (*smb2.Session, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	d := &smb2.Dialer{
		Initiator: &smb2.NTLMInitiator{
			User:     user,
			Password: pass,
		},
	}

	session, err := d.Dial(conn)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return session, nil
}

func (s SMBLocation) Open() (File, error) {
	session, err := connectAndLoginSMB(s.Addr, s.User, s.Pass)
	if err != nil {
		return nil, err
	}
	defer session.Logoff()

	fs, err := session.Mount(s.Path)
	if err != nil {
		return nil, err
	}
	defer fs.Umount()

	file, err := fs.Open(s.Path)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (s SMBLocation) Create() (File, error) {
	session, err := connectAndLoginSMB(s.Addr, s.User, s.Pass)
	if err != nil {
		return nil, err
	}
	defer session.Logoff()

	fs, err := session.Mount(s.Path)
	if err != nil {
		return nil, err
	}
	defer fs.Umount()

	file, err := fs.Create(s.Path)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (s SMBLocation) Type() int {
	return LOC_SMB
}
