package main

import (
	// #include <stdlib.h>
	// #include <unistd.h>
	// #include <sys/resource.h>
	// #include <sys/types.h>
	// #include <pwd.h>
	// #include <grp.h>
	"C"
	"io/ioutil"
	"strconv"
	"strings"
	"unsafe"

	"github.com/square/p2/pkg/user"
	"github.com/square/p2/pkg/util"
)

func nolimit() error {
	nrOpen, err := ioutil.ReadFile("/proc/sys/fs/nr_open")
	if err != nil {
		return util.Errorf("Could not read \"/proc/sys/fs/nr_open\": %s", err)
	}
	maxFDs, err := strconv.Atoi(strings.TrimSpace(string(nrOpen)))
	if err != nil {
		return util.Errorf("Could not convert %q (from \"/proc/sys/fs/nr_open\") into int: %s", nrOpen, err)
	}

	ret, err := C.setrlimit(C.RLIMIT_NOFILE, &C.struct_rlimit{C.rlim_t(maxFDs), C.rlim_t(maxFDs)})
	if ret != 0 && err != nil {
		return util.Errorf("Could not set RLIMIT_NOFILE (max FDs %v): %s", maxFDs, err)
	}

	inf := C.RLIM_INFINITY
	unlimit := &C.struct_rlimit{
		C.rlim_t(inf),
		C.rlim_t(inf),
	}
	ret, err = C.setrlimit(C.RLIMIT_CPU, unlimit)
	if ret != 0 && err != nil {
		return util.Errorf("Could not set RLIMIT_CPU: %s", err)
	}
	ret, err = C.setrlimit(C.RLIMIT_DATA, unlimit)
	if ret != 0 && err != nil {
		return util.Errorf("Could not set RLIMIT_DATA: %s", err)
	}
	ret, err = C.setrlimit(C.RLIMIT_FSIZE, unlimit)
	if ret != 0 && err != nil {
		return util.Errorf("Could not set RLIMIT_FSIZE: %s", err)
	}

	ret, err = C.setrlimit(C.RLIMIT_MEMLOCK, unlimit)
	if ret != 0 && err != nil {
		return util.Errorf("Could not set RLIMIT_MEMLOCK: %s", err)
	}
	ret, err = C.setrlimit(C.RLIMIT_NPROC, unlimit)
	if ret != 0 && err != nil {
		return util.Errorf("Could not set RLIMIT_NPROC: %s", err)
	}
	ret, err = C.setrlimit(C.RLIMIT_RSS, unlimit)
	if ret != 0 && err != nil {
		return util.Errorf("Could not set RLIMIT_RSS: %s", err)
	}
	return nil
}

func changeUser(username string) error {
	uid, gid, err := user.IDs(username)
	if err != nil {
		return util.Errorf("Could not retrieve uid/gid for %q: %s", username, err)
	}

	userCstring := C.CString(username)
	defer C.free(unsafe.Pointer(userCstring))

	ret, err := C.initgroups(userCstring, C.__gid_t(gid))
	if ret != 0 && err != nil {
		return util.Errorf("Could not initgroups for %q (primary gid %v): %s", username, gid, err)
	}
	ret, err = C.setgid(C.__gid_t(gid))
	if ret != 0 && err != nil {
		return util.Errorf("Could not setgid %v: %s", gid, err)
	}
	ret, err = C.setuid(C.__uid_t(uid))
	if ret != 0 && err != nil {
		return util.Errorf("Could not setuid %v: %s", uid, err)
	}
	return nil
}
