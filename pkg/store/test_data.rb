require 'json'

app = "slug"
node = {
  "cluster" => "development"
}
hostname = `hostname`.chomp

cmd = "curl -X PUT localhost:8500/v1/kv/nodes/#{hostname}/#{app} -d '#{node.to_json}'"
puts cmd
`#{cmd}`

config = {
  "min_nodes" => 0,
  "user_config" => {
    "some_value" => "hello",
  },
  "versions" => {
    "e928e5ad8814441e7c503d7f6c9e55d72584c006" => 'prep',
    '56d459e7c581b913ad5afa627064d62aea2cfac3' => 'active'
  }
}

# Deploy config cannot be changed by developers. It requires priviliged access
# to change, since it allows you to run stuff as root in arbitrary locations.
deploy_config = {
  'basedir' => '/tmp/slug',
  'ports'   => [1212, 1231],
}

cmd = "curl -X PUT localhost:8500/v1/kv/clusters/#{app}/#{node["cluster"]}/config -d '#{config.to_json}'"
puts cmd
`#{cmd}`

cmd = "curl -X PUT localhost:8500/v1/kv/clusters/#{app}/#{node["cluster"]}/deploy_config -d '#{deploy_config.to_json}'"
puts cmd
`#{cmd}`
