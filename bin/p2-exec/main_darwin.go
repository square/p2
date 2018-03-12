package main

import (
	// #include <stdlib.h>
	// #include <unistd.h>
	// #include <sys/resource.h>
	// #include <sys/types.h>
	// #include <sys/syslimits.h>
	// #include <pwd.h>
	// #include <grp.h>
	"C"
	"unsafe"

	"github.com/square/p2/pkg/util"
)

func sysMaxFDs() (*C.struct_rlimit, error) {
	return &C.struct_rlimit{
		C.rlim_t(C.OPEN_MAX),
		C.rlim_t(C.OPEN_MAX),
	}, nil
}

func sysUnRlimit() *C.struct_rlimit {
	return &C.struct_rlimit{
		C.rlim_t(C.RLIM_INFINITY),
		C.rlim_t(C.RLIM_INFINITY),
	}
}

// changeUser sets the uid and gid of the current process to the passed uid and gid
func changeUser(username string, uid int, gid int) error {
	userCstring := C.CString(username)
	defer C.free(unsafe.Pointer(userCstring))

	ret, err := C.initgroups(userCstring, C.int(gid))
	if ret != 0 && err != nil {
		return util.Errorf("Could not initgroups for %q (primary gid %v): %s", username, gid, err)
	}
	ret, err = C.setgid(C.gid_t(gid))
	if ret != 0 && err != nil {
		return util.Errorf("Could not setgid %v: %s", gid, err)
	}
	ret, err = C.setuid(C.uid_t(uid))
	if ret != 0 && err != nil {
		return util.Errorf("Could not setuid %v: %s", uid, err)
	}
	return nil
}
