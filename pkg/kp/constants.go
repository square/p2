package kp

import (
	"strings"

	"github.com/square/p2/pkg/hooks"
)

const (
	INTENT_TREE  string = "intent"
	REALITY_TREE string = "reality"
	HOOK_TREE    string = "hooks"
)

func IntentPath(args ...string) string {
	return strings.Join(append([]string{INTENT_TREE}, args...), "/")
}

func RealityPath(args ...string) string {
	return strings.Join(append([]string{REALITY_TREE}, args...), "/")
}

func HookPath(hookType hooks.HookType, args ...string) string {
	return strings.Join(append([]string{HOOK_TREE, hookType.String()}, args...), "/")
}
