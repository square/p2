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
	"os/user"
	"strconv"
	"strings"
	"unsafe"

	"github.com/square/p2/pkg/util"
)

func sysMaxFDs() (*C.struct_rlimit, error) {
	nrOpen, err := ioutil.ReadFile("/proc/sys/fs/nr_open")
	if err != nil {
		return nil, util.Errorf("Could not read \"/proc/sys/fs/nr_open\": %s", err)
	}
	maxFDs, err := strconv.Atoi(strings.TrimSpace(string(nrOpen)))
	if err != nil {
		return nil, util.Errorf("Could not convert %q (from \"/proc/sys/fs/nr_open\") into int: %s", nrOpen, err)
	}

	return &C.struct_rlimit{
		C.rlim_t(maxFDs),
		C.rlim_t(maxFDs),
	}, nil
}

func sysUnRlimit() *C.struct_rlimit {
	return &C.struct_rlimit{
		C.rlim_t(C.RLIM_INFINITY),
		C.rlim_t(C.RLIM_INFINITY),
	}
}

func changeUser(username string, uid int, gid int) error {
	currentUser, err := user.Current()
	if err != nil {
		return util.Errorf("Could not determine current user: %s", err)
	}

	if strconv.Itoa(uid) == currentUser.Uid && strconv.Itoa(gid) == currentUser.Gid {
		return nil
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
