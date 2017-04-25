package hooks

import "github.com/square/p2/pkg/logging"

// Returns a FileAuditLogger using the given logger
func NewFileAuditLogger(logger *logging.Logger) *FileAuditLogger {
	return &FileAuditLogger{
		logger: logger,
	}
}

type FileAuditLogger struct {
	logger *logging.Logger
}

func (al *FileAuditLogger) LogSuccess(env *HookExecContext) {
	al.logger.WithFields(envToFields(env)).Infoln("<hook audit log>")
}

func (al *FileAuditLogger) LogFailure(env *HookExecContext, err error) {
	lFields := envToFields(env)
	if err != nil {
		al.logger.WithError(err).WithFields(lFields).Errorln("<hook audit log>")
		return
	}
	al.logger.WithFields(lFields).Errorln("<hook audit log>")
}

func (al *FileAuditLogger) Close() error { return nil }

func envToFields(ctx *HookExecContext) map[string]interface{} {
	env := ctx.env
	return map[string]interface{}{
		"HookName":                  ctx.Name,
		"HookEnvVar":                env.HookEnvVar,
		"HookEventEnvVar":           env.HookEventEnvVar,
		"HookedNodeEnvVar":          env.HookedNodeEnvVar,
		"HookedPodIDEnvVar":         env.HookedPodIDEnvVar,
		"HookedPodHomeEnvVar":       env.HookedPodHomeEnvVar,
		"HookedPodManifestEnvVar":   env.HookedPodManifestEnvVar,
		"HookedConfigPathEnvVar":    env.HookedConfigPathEnvVar,
		"HookedEnvPathEnvVar":       env.HookedEnvPathEnvVar,
		"HookedConfigDirPathEnvVar": env.HookedConfigDirPathEnvVar,
		"HookedSystemPodRootEnvVar": env.HookedSystemPodRootEnvVar,
		"HookedPodUniqueKeyEnvVar":  env.HookedPodUniqueKeyEnvVar,
	}
}
