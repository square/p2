package pods

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"os"
	"path"

	curl "github.com/andelf/go-curl"
)

type easy interface {
	Cleanup()
	Perform() error
	Setopt(int, interface{}) error
}

type HoistLaunchable struct {
	location string
	id       string
	podId    string
	fetcher  easy
}

func (hoistLaunchable *HoistLaunchable) Install(dirPrefix string) error {
	installDir := hoistLaunchable.InstallDir(dirPrefix)
	if _, err := os.Stat(installDir); err == nil {
		return nil
	}

	easy := hoistLaunchable.fetcher
	defer easy.Cleanup()

	// follow redirects
	easy.Setopt(curl.OPT_FOLLOWLOCATION, true)

	// fail on HTTP 4xx errors
	easy.Setopt(curl.OPT_FAILONERROR, true)
	easy.Setopt(curl.OPT_URL, hoistLaunchable.location)

	easy.Setopt(curl.OPT_WRITEFUNCTION, func(ptr []byte, userdata interface{}) bool {
		file := userdata.(*os.File)
		if _, err := file.Write(ptr); err != nil {
			return false
		}
		return true
	})

	fp, err := os.Create(path.Join(os.TempDir(), hoistLaunchable.Name()))
	if err != nil {
		return err
	}
	defer fp.Close()

	easy.Setopt(curl.OPT_WRITEDATA, fp)

	if err := easy.Perform(); err != nil {
		return err
	}
	fp.Seek(0, 0)

	err = extractTarGz(fp, installDir)
	if err != nil {
		return err
	}
	return nil
}

func (*HoistLaunchable) Launch() error {
	return nil
}

func (hoistLaunchable *HoistLaunchable) Name() string {
	_, fileName := path.Split(hoistLaunchable.location)
	return fileName
}

func (*HoistLaunchable) Type() string {
	return "hoist"
}

func (hoistLaunchable *HoistLaunchable) InstallDir(dirPrefix string) string {
	launchableFileName := hoistLaunchable.Name()
	launchableName := launchableFileName[:len(launchableFileName)-len(".tar.gz")]
	return path.Join(dirPrefix, hoistLaunchable.id, "installs", launchableName) // need to generalize this (no /data/app assumption)
}

func extractTarGz(fp *os.File, dest string) (err error) {
	fz, err := gzip.NewReader(fp)
	if err != nil {
		return err
	}
	defer fz.Close()

	tr := tar.NewReader(fz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		fpath := path.Join(dest, hdr.Name)
		if hdr.FileInfo().IsDir() {
			continue
		} else {
			dir := path.Dir(fpath)
			os.MkdirAll(dir, 0755)
			f, err := os.OpenFile(
				fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, hdr.FileInfo().Mode())
			if err != nil {
				return err
			}
			defer f.Close()

			_, err = io.Copy(f, tr)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
