package ppkv

import (
	"encoding/json"
	"errors"
	"path"

	"github.com/armon/consul-api"
)

type Client struct {
	kv *consulapi.KV
}

func NewClient() (*Client, error) {
	client, err := consulapi.NewClient(consulapi.DefaultConfig())

	if err != nil {
		return nil, err
	}

	return &Client{
		kv: client.KV(),
	}, nil
}

func (c *Client) List(query string) (map[string]interface{}, error) {
	xs, _, err := c.kv.List(query, nil)
	if err != nil {
		return nil, err
	}

	ret := map[string]interface{}{}

	for _, x := range xs {
		var value interface{}
		keyName := path.Base(x.Key)
		err := json.Unmarshal(x.Value, &value)
		if err != nil {
			return nil, err
		}
		ret[keyName] = value
	}
	return ret, nil
}

func (c *Client) Get(query string, ret interface{}) error {
	data, _, err := c.kv.Get(query, nil)

	if err != nil {
		return err
	}

	if data == nil {
		return errors.New("key not present")
	}

	if err := json.Unmarshal(data.Value, &ret); err != nil {
		return err
	}

	return nil
}

func (c *Client) DeleteTree(query string) error {
	_, err := c.kv.DeleteTree(query, nil)
	return err
}

func (c *Client) Put(key string, value interface{}) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}

	node := &consulapi.KVPair{
		Key:   key,
		Value: body,
	}
	_, err = c.kv.Put(node, nil)
	return err
}
