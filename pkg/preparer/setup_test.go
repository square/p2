package preparer

import (
	"runtime"
	"testing"

	. "github.com/anthonybishopric/gotcha"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/util"
)

func TestLoadConfigWillMarshalYaml(t *testing.T) {
	configPath := util.From(runtime.Caller(0)).ExpandPath("test_preparer_config.yaml")
	preparerConfig, err := LoadConfig(configPath)
	Assert(t).IsNil(err, "should have read config correctly")

	Assert(t).AreEqual("foohost", preparerConfig.NodeName.String(), "did not read the node name correctly")
	Assert(t).AreEqual("0.0.0.0", preparerConfig.ConsulAddress, "did not read the consul address correctly")
	Assert(t).IsTrue(preparerConfig.ConsulHttps, "did not read consul HTTPS correctly (should be true)")
	Assert(t).AreEqual("/etc/p2/hooks", preparerConfig.HooksDirectory, "did not read the hooks directory correctly")
	Assert(t).AreEqual("/etc/p2.keyring", preparerConfig.Auth["keyring"], "did not read the keyring path correctly")
	Assert(t).AreEqual(1, len(preparerConfig.ExtraLogDestinations), "should have picked up 1 log destination")

	destination := preparerConfig.ExtraLogDestinations[0]
	Assert(t).AreEqual(logging.OutSocket, destination.Type, "should have been the socket type")
	Assert(t).AreEqual("/var/log/p2-socket.out", destination.Path, "should have parsed path correctly")
}
