package pp

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/platypus-platform/pp/pkg/logging"

	"testing"
)

func init() {
	logger.SetLogLevel("FATAL")
}

func TestApp(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "pp-store")
}
