package main

import (
	// #include <stdlib.h>
	// #include <stdio.h>
	// #include <unistd.h>
	// #include <sys/resource.h>
	// #include <sys/types.h>
	// #include <pwd.h>
	// #include <grp.h>
	// #include <sys/syslimits.h>
	//
	// char** stringSlice(int size) {
	//   char** ret = calloc(sizeof(char*), size+1);
	//   ret[size] = NULL;
	//   return ret;
	// }
	// void setStringSlice(char **a, char *s, int n) {
	//   a[n] = s;
	// }
	// void freeStringSlice(char **a, int size) {
	//   int i;
	//   for (i = 0; i < size; i++) { free(a[i]); }
	//   free(a);
	// }
	//
	// char* rlimDropExec(char* username, char* arg0, char** argv) {
	//   struct rlimit lim;
	//   struct passwd* pw;
	//
	//   lim.rlim_cur = OPEN_MAX;
	//   lim.rlim_max = OPEN_MAX;
	//   if (setrlimit(RLIMIT_NOFILE, &lim)) { return "Could not set RLIMIT_NOFILE"; }
	//   lim.rlim_cur = RLIM_INFINITY;
	//   lim.rlim_max = RLIM_INFINITY;
	//   if (setrlimit(RLIMIT_CPU, &lim)) { return "Could not set RLIMIT_CPU"; }
	//   if (setrlimit(RLIMIT_DATA, &lim)) { return "Could not set RLIMIT_DATA"; }
	//   if (setrlimit(RLIMIT_FSIZE, &lim)) { return "Could not set RLIMIT_FSIZE"; }
	//   if (setrlimit(RLIMIT_MEMLOCK, &lim)) { return "Could not set RLIMIT_MEMLOCK"; }
	//   if (setrlimit(RLIMIT_NPROC, &lim)) { return "Could not set RLIMIT_NPROC"; }
	//   if (setrlimit(RLIMIT_RSS, &lim)) { return "Could not set RLIMIT_RSS"; }
	//
	//   pw = getpwnam(username);
	//   if (!pw) { return "Could not getpwnam"; }
	//   if (initgroups(username, pw->pw_gid)) { return "Could not initgroups"; }
	//   if (setgid(pw->pw_gid)) { return "Could not setgid"; }
	//   if (setuid(pw->pw_uid)) { return "Could not setuid"; }
	//
	//   execv(arg0, argv);
	//   return "Could not execvp";
	// }
	"C"
	"unsafe"

	"github.com/square/p2/pkg/util"
)

func rlimDropExec(username string, cmd0 string, cmd []string) error {
	userC := C.CString(username)
	defer C.free(unsafe.Pointer(userC))
	cmd0C := C.CString(cmd0)
	defer C.free(unsafe.Pointer(cmd0C))

	cmdC := C.stringSlice(C.int(len(cmd)))
	defer C.freeStringSlice(cmdC, C.int(len(cmd)))
	for i, s := range cmd {
		C.setStringSlice(cmdC, C.CString(s), C.int(i))
	}

	errcode, err := C.rlimDropExec(userC, cmd0C, cmdC)
	errString := C.GoString(errcode)
	return util.Errorf("%s: %s", errString, err)
}
