package kp

import (
	"strings"
)

const (
	INTENT_TREE  string = "intent"
	REALITY_TREE string = "reality"
	HOOK_TREE    string = "hooks"
	LOCK_TREE    string = "lock"
	RC_TREE      string = "replication_controllers"
	ROLL_TREE    string = "rolls"
)

func IntentPath(args ...string) string {
	return strings.Join(append([]string{INTENT_TREE}, args...), "/")
}

func RealityPath(args ...string) string {
	return strings.Join(append([]string{REALITY_TREE}, args...), "/")
}

func HookPath(args ...string) string {
	return strings.Join(append([]string{HOOK_TREE}, args...), "/")
}

func LockPath(args ...string) string {
	return strings.Join(append([]string{LOCK_TREE}, args...), "/")
}

func RCPath(args ...string) string {
	return strings.Join(append([]string{RC_TREE}, args...), "/")
}

func RollPath(args ...string) string {
	return strings.Join(append([]string{ROLL_TREE}, args...), "/")
}
