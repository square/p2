package hoist

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/util"
)

// ArtifactCacher keeps a directory of Hoist artifacts that can be reused for future downloads.
// This allows artifacts to be cheaply shared amongst different pods, especially in cases where
// the artifacts contain base libraries and runtimes. For example, a pod may include a Java 8
// artifact that is reused across other pods.
//
// The cache assumes that Hoist artifact URIs are unique at the basename, ie. comply with the
// convention of {artifact_name}_{artifact_SHA}.tar.gz pattern. No attempt is made to distinguish
// artifacts that are named the same but contain different content. (Checksumming is presumed to
// be done by the Launchable itself)
//
// Once a cached copy of an artifact is created, it will be hardlinked into the final destination
// URI provided by the caller. This allows the ArtifactCacher to safely prune without worrying about
// whether the artifact is still in use by a particular Launchable (say, as the Last link). However,
// as a result of this strategy, pruning the cache dir will not be sufficient to free system disk
// resources as other links to the same tars will also need to be removed.
type ArtifactCacher struct {
	cacheDir      string
	nestedFetcher Fetcher
	Logger        *logging.Logger
}

// Complies with the Fetcher interface
func (a *ArtifactCacher) Fetch(fromURI, toURI string) error {
	base := filepath.Base(toURI)
	if base == "." || base == string(filepath.Separator) {
		return util.Errorf("%s is not a valid toURI", toURI)
	}
	cachePath := filepath.Join(a.cacheDir, base)
	if _, err := os.Stat(cachePath); os.IsNotExist(err) {
		a.nestedFetcher(fromURI, cachePath)
	} else if err != nil {
		return util.Errorf("Error stating the cache path: %s", err)
	} else {
		err = a.touch(cachePath)
		if err != nil {
			a.Logger.WithField("err", err).Warnln("Error when updating mtime on cached artifact")
		}
	}
	// hardlink so we can safely prune without breaking Last/Current links
	linkErr := os.Link(cachePath, toURI)
	if linkErr != nil {
		// we may be on different mounts, just copy
		err := uri.URICopy(cachePath, toURI)
		if err != nil {
			return util.Errorf("Could not link or copy file from %s to %s: %s - %s", cachePath, toURI, err, linkErr)
		}
	}
	return nil
}

type byMtime []os.FileInfo

func (b byMtime) Len() int           { return len(b) }
func (b byMtime) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byMtime) Less(i, j int) bool { return b[i].ModTime().Before(b[j].ModTime()) }

// Prune will remove all but `keep` # of artifacts
// from the cache dir. Prune will favor more recently modified artifacts
func (a *ArtifactCacher) Prune(keep int) {
	infos, err := ioutil.ReadDir(a.cacheDir)
	if err != nil {
		err = util.Errorf("cache dir unlistable: %s", err)
		a.Logger.WithField("err", err).Errorln("Pruning failed")
		return
	}
	byMtimeInfos := byMtime(infos)
	sort.Sort(&byMtimeInfos)
	for i := len(infos) - 1; i >= keep; i-- {
		info := infos[i]
		err = os.Remove(filepath.Join(a.cacheDir, info.Name()))
		if err != nil {
			a.Logger.WithFields(logrus.Fields{
				"path": info.Name(),
				"err":  err,
			}).Errorln("Could not prune")
		}
	}
}

func (a *ArtifactCacher) touch(cachePath string) error {
	err := os.Chtimes(cachePath, time.Now(), time.Now())
	if err != nil {
		return util.Errorf("Couldn't update %s mtime: %s", cachePath, err)
	}
	return nil
}

func WithCacher(f Fetcher, cacheDir string) (*ArtifactCacher, error) {
	if _, err := os.Stat(cacheDir); os.IsNotExist(err) {
		err = os.MkdirAll(cacheDir, 0644)
		if err != nil {
			return nil, util.Errorf("Could not initialize cacheDir %s: %s", cacheDir, err)
		}
	}
	logger := logging.DefaultLogger.SubLogger(logrus.Fields{"source": "artifact_cache"})
	cacher := &ArtifactCacher{
		cacheDir:      cacheDir,
		nestedFetcher: f,
		Logger:        &logger,
	}
	return cacher, nil
}
