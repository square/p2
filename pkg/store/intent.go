package pp

import (
	"github.com/platypus-platform/pp/pkg/kv-consul"
	. "github.com/platypus-platform/pp/pkg/logging"
	"path"
)

type DeployConfig struct {
	Basedir string
	Ports   []int
}

type AppConfig struct {
	Versions   map[string]string
	MinNodes   int
	UserConfig map[string]interface{} `json:"user_config"`
}

// The intended state for an application on a node.
type IntentApp struct {
	Name string
	AppConfig
	DeployConfig
}

// The intended state for a particular node.
type IntentNode struct {
	Apps map[string]IntentApp
}

// An app may have many versions ready to go, but only zero or one will be
// active.
func (app *IntentApp) ActiveVersion() string {
	for id, status := range app.Versions {
		if status == "active" {
			return id
		}
	}
	return ""
}

// Fetches data from the intent store and emits it on the channel.
// It skips any malformed data. The only error condition is if the store is not
// available in the first place.
func PollIntent(hostname string, callback func(IntentNode)) error {
	Info("Polling intent store")

	kv, _ := ppkv.NewClient()
	apps, err := kv.List(path.Join("nodes", hostname))
	if err != nil {
		return err
	}

	intent := IntentNode{
		Apps: map[string]IntentApp{},
	}

	for appName, data := range apps {
		Info("Checking spec for %s", appName)

		appData, worked := stringMap(data)
		if !worked {
			Fatal("Invalid node data for %s", appName)
			continue
		}

		cluster := appData["cluster"]
		if cluster == "" {
			Fatal("No cluster key in node data for %s", appName)
			continue
		}

		clusterKey := path.Join("clusters", appName, cluster, "config")
		configKey := path.Join("clusters", appName, cluster, "deploy_config")

		var appConfig AppConfig
		var deployConfig DeployConfig

		if err := kv.Get(clusterKey, &appConfig); err != nil {
			Fatal("No or invalid data for %s: %s", clusterKey, err)
			continue
		}

		err = kv.Get(configKey, &deployConfig)

		if err != nil {
			Fatal("No or invalid data for %s: %s", configKey, err)
			continue
		}

		basedir := deployConfig.Basedir
		if !path.IsAbs(basedir) {
			Fatal("Not allowing relative basedir in %s", configKey)
			continue
		}

		intent.Apps[appName] = IntentApp{
			Name:         appName,
			AppConfig:    appConfig,
			DeployConfig: deployConfig,
		}
	}

	callback(intent)

	return nil
}

func stringMap(raw interface{}) (map[string]string, bool) {
	mapped, worked := raw.(map[string]interface{})
	if !worked {
		return nil, false
	}
	ret := map[string]string{}
	for k, v := range mapped {
		str, worked := v.(string)
		if !worked {
			return nil, false
		}
		ret[k] = str
	}
	return ret, true
}
