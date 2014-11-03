package user

import (
	"bytes"
	"errors"
	"os/exec"
	osuser "os/user"

	"github.com/square/p2/pkg/util"
)

var AlreadyExists error

func init() {
	AlreadyExists = errors.New("The user already exists")
}

func CreateUser(username string, homedir string) (*osuser.User, error) {
	user, err := osuser.Lookup(username)
	if err == nil {
		return user, AlreadyExists
	}
	add := exec.Command("adduser", "-d", homedir, username)
	errout := bytes.Buffer{}
	add.Stderr = &errout
	err = add.Run()
	if err != nil {
		return nil, util.Errorf("Couldn't add new user %s: %s: %s", username, err, errout.String())
	}
	return osuser.Lookup(username)
}
