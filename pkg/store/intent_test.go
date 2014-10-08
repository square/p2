package pp

import (
	"bytes"
	"github.com/platypus-platform/pp/pkg/kv-consul"
	"github.com/platypus-platform/pp/pkg/logging"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func prepareStore() *ppkv.Client {
	var result interface{}
	kv, _ := ppkv.NewClient()
	if err := kv.Get("test", &result); err != nil {
		return nil
	}

	kv.DeleteTree("nodes/testhost")
	kv.DeleteTree("clusters/testapp")

	return kv
}

var _ = Describe("Reading spec from KV store", func() {
	It("Works", func() {
		kv := prepareStore()
		if kv == nil {
			return
		}

		kv.Put("nodes/testhost/testapp", map[string]string{
			"cluster": "test",
		})
		kv.Put("clusters/testapp/test/config", AppConfig{
			Versions: map[string]string{
				"abc123": "prep",
				"def456": "active",
			},
		})
		kv.Put("clusters/testapp/test/deploy_config", map[string]string{
			"basedir": "/sometmp",
		})

		s := make([]IntentNode, 0)

		err := PollIntent("testhost", func(intent IntentNode) {
			s = append(s, intent)
		})
		if err != nil {
			Fail(err.Error())
			return
		}

		expected := []IntentNode{
			IntentNode{
				Apps: map[string]IntentApp{
					"testapp": IntentApp{
						Name: "testapp",
						DeployConfig: DeployConfig{
							Basedir: "/sometmp",
						},
						AppConfig: AppConfig{
							Versions: map[string]string{
								"abc123": "prep",
								"def456": "active",
							},
						},
					},
				},
			},
		}

		Expect(s).To(Equal(expected))
	})

	It("Gracefully handles no data", func() {
		kv := prepareStore()
		if kv == nil {
			return
		}

		PollIntent("testhost", Sink())
	})

	It("Gracefully handles invalid node data", func() {
		var buf bytes.Buffer
		logger.SetOut(&buf)
		defer logger.SetOut(logger.DefaultOut())

		kv := prepareStore()
		if kv == nil {
			return
		}

		kv.Put("nodes/testhost/testapp", 34)

		PollIntent("testhost", Sink())

		Expect(buf.String()).To(ContainSubstring("Invalid node data"))
		Expect(buf.String()).To(ContainSubstring("testapp"))
	})

	It("Gracefully handles missing cluster data", func() {
		var buf bytes.Buffer
		logger.SetOut(&buf)
		defer logger.SetOut(logger.DefaultOut())

		kv := prepareStore()
		if kv == nil {
			return
		}

		kv.Put("nodes/testhost/testapp", map[string]string{
			"bogus": "test",
		})

		PollIntent("testhost", Sink())

		Expect(buf.String()).To(ContainSubstring("No cluster key"))
		Expect(buf.String()).To(ContainSubstring("testapp"))
	})

	It("Gracefully handles missing or invalid version data", func() {
		var buf bytes.Buffer
		logger.SetOut(&buf)
		defer logger.SetOut(logger.DefaultOut())

		kv := prepareStore()
		if kv == nil {
			return
		}

		kv.Put("nodes/testhost/testapp", map[string]string{
			"cluster": "test",
		})

		PollIntent("testhost", Sink())

		Expect(buf.String()).To(ContainSubstring("No or invalid data"))
		Expect(buf.String()).To(ContainSubstring("testapp"))

		buf.Reset()

		kv.Put("clusters/testapp/test/config", "bogus")

		PollIntent("testhost", Sink())

		Expect(buf.String()).To(ContainSubstring("No or invalid data"))
		Expect(buf.String()).To(ContainSubstring("testapp"))
	})

	It("Gracefully handles missing or invalid config data", func() {
		var buf bytes.Buffer
		logger.SetOut(&buf)
		defer logger.SetOut(logger.DefaultOut())

		kv := prepareStore()
		if kv == nil {
			return
		}

		kv.Put("nodes/testhost/testapp", map[string]string{
			"cluster": "test",
		})
		kv.Put("clusters/testapp/test/config", AppConfig{
			Versions: map[string]string{
				"abc123": "active",
			},
		})

		PollIntent("testhost", Sink())

		Expect(buf.String()).To(ContainSubstring("No or invalid data"))
		Expect(buf.String()).To(ContainSubstring("testapp"))

		buf.Reset()
		kv.Put("clusters/testapp/test/deploy_config", "bogus")
		PollIntent("testhost", Sink())

		Expect(buf.String()).To(ContainSubstring("No or invalid data"))
		Expect(buf.String()).To(ContainSubstring("testapp"))
	})

	It("Gracefully handles non-absolute basedir", func() {
		var buf bytes.Buffer
		logger.SetOut(&buf)
		defer logger.SetOut(logger.DefaultOut())

		kv := prepareStore()
		if kv == nil {
			return
		}

		kv.Put("nodes/testhost/testapp", map[string]string{
			"cluster": "test",
		})
		kv.Put("clusters/testapp/test/config", AppConfig{
			Versions: map[string]string{
				"abc123": "active",
			},
		})
		kv.Put("clusters/testapp/test/deploy_config", map[string]string{
			"basedir": "relative",
		})

		PollIntent("testhost", Sink())

		Expect(buf.String()).To(ContainSubstring("Not allowing relative basedir"))
		Expect(buf.String()).To(ContainSubstring("testapp"))
	})
})

func Sink() func(IntentNode) {
	return func(_ IntentNode) {
	}
}
