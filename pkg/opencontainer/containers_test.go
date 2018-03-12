package opencontainer

import (
	"testing"
)

const (
	uid = 1234
	gid = 1234
)

// this is just the minimal fields required for validation for brevity, it
// certainly wouldn't run a functional container
func validSpec() Spec {
	return Spec{
		Process: &Process{
			NoNewPrivileges: true,
			User: User{
				UID: 1234,
				GID: 1234,
			},
			Capabilities: &LinuxCapabilities{},
		},
		Root: &Root{
			Readonly: true,
			Path:     "rootfs",
		},
	}
}

func validRootSpec() Spec {
	spec := validSpec()
	spec.Process.User.GID = 0
	spec.Process.User.UID = 0
	return spec
}

var launchable = &Launchable{}

type validateSpecCase struct {
	specMutator    func(Spec) *Spec
	shouldValidate bool
}

func TestValidateSpecNonRoot(t *testing.T) {
	validateSpecCases := []validateSpecCase{
		{ // normal case
			specMutator:    func(s Spec) *Spec { return &s },
			shouldValidate: true,
		},
		{ // nil spec
			specMutator:    func(s Spec) *Spec { return nil },
			shouldValidate: false,
		},
		{ // Solaris set
			specMutator:    func(s Spec) *Spec { s.Solaris = &Solaris{}; return &s },
			shouldValidate: false,
		},
		{ // Windows set
			specMutator:    func(s Spec) *Spec { s.Windows = &Windows{}; return &s },
			shouldValidate: false,
		},
		{ // nil root
			specMutator:    func(s Spec) *Spec { s.Root = nil; return &s },
			shouldValidate: false,
		},
		{ // root has a path separator
			specMutator:    func(s Spec) *Spec { s.Root.Path = "foo/rootfs"; return &s },
			shouldValidate: false,
		},
		{ // root isn't read only
			specMutator:    func(s Spec) *Spec { s.Root.Readonly = false; return &s },
			shouldValidate: false,
		},
		{ // uid doesn't match the pod user
			specMutator:    func(s Spec) *Spec { s.Process.User.UID = 0; return &s },
			shouldValidate: false,
		},
		{ // gid doesn't match the pod user
			specMutator:    func(s Spec) *Spec { s.Process.User.GID = 0; return &s },
			shouldValidate: false,
		},
		{ // bounding capability set
			specMutator:    func(s Spec) *Spec { s.Process.Capabilities.Bounding = []string{"a capability"}; return &s },
			shouldValidate: false,
		},
		{ // effective capability set
			specMutator:    func(s Spec) *Spec { s.Process.Capabilities.Effective = []string{"a capability"}; return &s },
			shouldValidate: false,
		},
		{ // inheritable capability set
			specMutator:    func(s Spec) *Spec { s.Process.Capabilities.Inheritable = []string{"a capability"}; return &s },
			shouldValidate: false,
		},
		{ // permitted capability set
			specMutator:    func(s Spec) *Spec { s.Process.Capabilities.Permitted = []string{"a capability"}; return &s },
			shouldValidate: false,
		},
		{ // ambient capability set
			specMutator:    func(s Spec) *Spec { s.Process.Capabilities.Ambient = []string{"a capability"}; return &s },
			shouldValidate: false,
		},
		{ // no new privileges off
			specMutator:    func(s Spec) *Spec { s.Process.NoNewPrivileges = false; return &s },
			shouldValidate: false,
		},
	}
	for i, tc := range validateSpecCases {
		specToTry := tc.specMutator(validSpec())
		err := ValidateSpec(specToTry, uid, gid)
		if err != nil && tc.shouldValidate {
			t.Errorf("case %d: %s", i+1, err)
		}

		if err == nil && !tc.shouldValidate {
			t.Errorf("case %d: expected an error but didn't get one", i+1)
		}
	}
}

func TestValidateSpecRoot(t *testing.T) {
	validateSpecCases := []validateSpecCase{
		{ // normal case
			specMutator:    func(s Spec) *Spec { return &s },
			shouldValidate: true,
		},
		{ // nil spec
			specMutator:    func(s Spec) *Spec { return nil },
			shouldValidate: false,
		},
		{ // Solaris set
			specMutator:    func(s Spec) *Spec { s.Solaris = &Solaris{}; return &s },
			shouldValidate: false,
		},
		{ // Windows set
			specMutator:    func(s Spec) *Spec { s.Windows = &Windows{}; return &s },
			shouldValidate: false,
		},
		{ // nil root
			specMutator:    func(s Spec) *Spec { s.Root = nil; return &s },
			shouldValidate: false,
		},
		{ // root has a path separator
			specMutator:    func(s Spec) *Spec { s.Root.Path = "foo/rootfs"; return &s },
			shouldValidate: false,
		},
		{ // root isn't read only
			specMutator:    func(s Spec) *Spec { s.Root.Readonly = false; return &s },
			shouldValidate: false,
		},
		{ // uid doesn't match the pod user
			specMutator:    func(s Spec) *Spec { s.Process.User.UID = 1234; return &s },
			shouldValidate: false,
		},
		{ // gid doesn't match the pod user
			specMutator:    func(s Spec) *Spec { s.Process.User.GID = 1234; return &s },
			shouldValidate: false,
		},
		{ // bounding capability set
			specMutator:    func(s Spec) *Spec { s.Process.Capabilities.Bounding = []string{"a capability"}; return &s },
			shouldValidate: true, // because we're root anyway
		},
		{ // effective capability set
			specMutator:    func(s Spec) *Spec { s.Process.Capabilities.Effective = []string{"a capability"}; return &s },
			shouldValidate: true, // because we're root anyway
		},
		{ // inheritable capability set
			specMutator:    func(s Spec) *Spec { s.Process.Capabilities.Inheritable = []string{"a capability"}; return &s },
			shouldValidate: true, // because we're root anyway
		},
		{ // permitted capability set
			specMutator:    func(s Spec) *Spec { s.Process.Capabilities.Permitted = []string{"a capability"}; return &s },
			shouldValidate: true, // because we're root anyway
		},
		{ // ambient capability set
			specMutator:    func(s Spec) *Spec { s.Process.Capabilities.Ambient = []string{"a capability"}; return &s },
			shouldValidate: true, // because we're root anyway
		},
		{ // no new privileges off
			specMutator:    func(s Spec) *Spec { s.Process.NoNewPrivileges = false; return &s },
			shouldValidate: false,
		},
	}
	for i, tc := range validateSpecCases {
		specToTry := tc.specMutator(validRootSpec())
		err := ValidateSpec(specToTry, 0, 0)
		if err != nil && tc.shouldValidate {
			t.Errorf("case %d: %s", i+1, err)
		}

		if err == nil && !tc.shouldValidate {
			t.Errorf("case %d: expected an error but didn't get one", i+1)
		}
	}
}
