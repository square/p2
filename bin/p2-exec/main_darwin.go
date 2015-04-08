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

	"github.com/square/p2/pkg/user"
)

func nolimit() error {
	maxFDs := C.OPEN_MAX
	_, err := C.setrlimit(C.RLIMIT_NOFILE, &C.struct_rlimit{C.rlim_t(maxFDs), C.rlim_t(maxFDs)})
	if err != nil {
		return err
	}

	unlimit := &C.struct_rlimit{
		C.rlim_t(C.RLIM_INFINITY),
		C.rlim_t(C.RLIM_INFINITY),
	}
	_, err = C.setrlimit(C.RLIMIT_CPU, unlimit)
	if err != nil {
		return err
	}
	_, err = C.setrlimit(C.RLIMIT_DATA, unlimit)
	if err != nil {
		return err
	}
	_, err = C.setrlimit(C.RLIMIT_FSIZE, unlimit)
	if err != nil {
		return err
	}
	_, err = C.setrlimit(C.RLIMIT_MEMLOCK, unlimit)
	if err != nil {
		return err
	}
	_, err = C.setrlimit(C.RLIMIT_NPROC, unlimit)
	if err != nil {
		return err
	}
	_, err = C.setrlimit(C.RLIMIT_RSS, unlimit)
	if err != nil {
		return err
	}
	return nil
}

func changeUser(username string) error {
	uid, gid, err := user.IDs(username)
	if err != nil {
		return err
	}

	userCstring := C.CString(username)
	defer C.free(unsafe.Pointer(userCstring))

	_, err = C.initgroups(userCstring, C.int(gid))
	if err != nil {
		return err
	}
	_, err = C.setgid(C.gid_t(gid))
	if err != nil {
		return err
	}
	_, err = C.setuid(C.uid_t(uid))
	if err != nil {
		return err
	}
	return nil
}
