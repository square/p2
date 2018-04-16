// package container_exec contains functionality that is useful for configuring
// an opencontainer's config.json file with P2-specific settings.
package container_exec

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/square/p2/pkg/opencontainer"
	"github.com/square/p2/pkg/util"
)

// We need a type that will catch any JSON so we don't modify parts of the
// file we didn't want to modify
type runcConfigWriter struct {
	runcConfig *opencontainer.Spec

	// baseDir is the directory to which the final config.json should be written
	baseDir string

	// uid and gid are used to validate that the config will run as the correct
	// user before writing it to disk
	uid uint32
	gid uint32
}

type RuncConfigWriter interface {
	AddBindMounts(paths []string) error
	Write() error
}

// NewRuncConfigWriter returns an interface that can be used to craft a runc config.json
// file by starting with an existing template. The UID and GID will be written inside
// the constructor because that behavior is always desired.
func NewRuncConfigWriter(baseDir string, templateFilename string, uid int, gid int) (RuncConfigWriter, error) {
	configJSONPath := filepath.Join(baseDir, opencontainer.SpecFilename)
	err := os.Remove(configJSONPath)
	switch {
	case os.IsNotExist(err):
		// good, the file isn't supposed to exist until we write it
	case err == nil:
		// good, we removed it
	case err != nil:
		return nil, util.Errorf("could not delete %s before rewriting it: %s", opencontainer.SpecFilename, err)
	}

	templatePath := filepath.Join(baseDir, templateFilename)

	// start runcConfigWriter from the data in the config.json.template. The caller
	// will then make the desired mutations and eventually write it out to config.json
	runcConfig, err := getRuncConfigTemplate(templatePath)
	if err != nil {
		return nil, err
	}

	var uid32, gid32 uint32
	uid32 = uint32(uid)
	gid32 = uint32(gid)

	// make sure that the cast did what we wanted
	if int(uid32) != uid {
		return nil, util.Errorf("could not safely cast uid %d to a uint32: got %d", uid, uid32)
	}
	if int(gid32) != gid {
		return nil, util.Errorf("could not safely cast gid %d to a uint32: got %d", gid, gid32)
	}

	runcConfig, err = rewriteUIDAndGID(runcConfig, uid32, gid32)
	if err != nil {
		return nil, err
	}

	return &runcConfigWriter{
		runcConfig: runcConfig,
		baseDir:    baseDir,
		uid:        uid32,
		gid:        gid32,
	}, nil
}

// rewriteUIDAndGID rewrites the uid/gid in the runc config which controls the
// user/group that the process it starts will run as.
func rewriteUIDAndGID(runcConfig *opencontainer.Spec, uid uint32, gid uint32) (*opencontainer.Spec, error) {
	runcConfig.Process.User.UID = uid
	runcConfig.Process.User.GID = gid

	return runcConfig, nil
}

// AddBindMounts takes a slice of paths as arguments. It will add a bind mount to the runc
// config with each path as both the source and destination.
func (r *runcConfigWriter) AddBindMounts(paths []string) error {
	for _, path := range paths {
		if !filepath.IsAbs(path) {
			return util.Errorf("%s is not an absolute path", path)
		}
	}

	mounts := r.runcConfig.Mounts

	for _, path := range paths {
		// add the mount
		mounts = append(mounts, opencontainer.Mount{
			Destination: path,
			Source:      path,
			Type:        "bind",
			Options: []string{
				"bind",
				"nosuid",
				"nodev",
				"strictatime",
			},
		})
	}
	r.runcConfig.Mounts = mounts

	return nil
}

func (r *runcConfigWriter) Write() error {
	err := opencontainer.ValidateSpec(r.runcConfig, int(r.uid), int(r.gid))
	if err != nil {
		return err
	}

	jsonBytes, err := json.MarshalIndent(r.runcConfig, "", "        ")
	if err != nil {
		return util.Errorf("could not marshal rewritten config.json template as JSON: %s", err)
	}

	configJSONPath := filepath.Join(r.baseDir, opencontainer.SpecFilename)
	err = ioutil.WriteFile(configJSONPath, jsonBytes, 0744)
	if err != nil {
		return util.Errorf("could not write rewritten JSON template to config.json: %s", err)
	}

	return nil
}

// getRuncConfigTemplate() locates the config.json.template file and returns a
// configWriter loaded with the unmarshaled JSON values.
func getRuncConfigTemplate(configTemplatePath string) (*opencontainer.Spec, error) {
	var config opencontainer.Spec

	templateConfigBytes, err := ioutil.ReadFile(configTemplatePath)
	if err != nil {
		return nil, util.Errorf("could not read %s: %s", configTemplatePath, err)
	}

	err = json.Unmarshal(templateConfigBytes, &config)
	if err != nil {
		return nil, util.Errorf("could not parse %s as JSON: %s", configTemplatePath, err)
	}

	return &config, nil
}
