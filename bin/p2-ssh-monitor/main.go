package main

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/square/p2/pkg/config"
	"github.com/square/p2/pkg/logging"

	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"
	"github.com/square/p2/Godeps/_workspace/src/golang.org/x/crypto/ssh"
)

func main() {
	conf, err := config.LoadFromEnvironment()
	fatalize(err)
	sshConf, err := conf.ReadMap("ssh_monitor")
	fatalize(err)

	sshKeyPath, err := sshConf.ReadString("ssh_key")
	fatalize(err)
	sshUser, err := sshConf.ReadString("ssh_user")
	fatalize(err)
	sshAddr, err := sshConf.ReadString("ssh_address")
	fatalize(err)
	monitorPort, err := sshConf.ReadString("port")
	fatalize(err)
	if sshKeyPath == "" || sshUser == "" || sshAddr == "" || monitorPort == "" {
		logging.DefaultLogger.NoFields().Fatalln("Must set all of ssh_key, ssh_user, ssh_address and port")
	}

	privateKey, err := ioutil.ReadFile(sshKeyPath)
	if err != nil {
		logging.DefaultLogger.WithErrorAndFields(err, logrus.Fields{
			"key": sshKeyPath,
		}).Fatalln("Could not read SSH private key")
	}
	authSigner, err := ssh.ParsePrivateKey(privateKey)
	if err != nil {
		logging.DefaultLogger.WithErrorAndFields(err, logrus.Fields{
			"key": sshKeyPath,
		}).Fatalln("Could not parse SSH private key")
	}
	clientConf := &ssh.ClientConfig{
		User: sshUser,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(authSigner),
		},
	}

	http.HandleFunc("/_status", func(w http.ResponseWriter, r *http.Request) {
		c, err := ssh.Dial("tcp", sshAddr, clientConf)
		defer c.Close()

		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			logging.DefaultLogger.WithErrorAndFields(err, logrus.Fields{
				"user":    sshUser,
				"key":     sshKeyPath,
				"address": sshAddr,
			}).Errorln("Could not connect to SSHd")
		} else {
			w.Write([]byte("ok"))
			logging.DefaultLogger.NoFields().Debugln("Connected to SSHd successfully")
		}
	})
	if err := http.ListenAndServe(fmt.Sprintf(":%d", monitorPort), nil); err != nil {
		logging.DefaultLogger.WithError(err).Fatalln("Monitor HTTP server crashed")
	}
}

func fatalize(err error) {
	if err != nil {
		logging.DefaultLogger.WithError(err).Fatalln("Error while loading configuration")
	}
}
