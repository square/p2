require "json"

def e(cmd)
  puts cmd
  system(cmd) || raise("Error running `#{cmd}`")
end

def target(expand="")
  root = File.dirname(__FILE__)
  File.expand_path(expand, File.join(root, 'target'))
end

task :godep_check do
  system("which godep") || abort("You do not have godep installed. Run `go get github.com/tools/godep` and ensure that it's on your PATH")
end

desc 'Get deps for all projects.'
task :deps => :godep_check do
  e "go get -v -t ./..."
  e "godep save ./..."
end

desc 'Build all projects'
task :build do
  e "go build -v ./..."
end

desc 'Test all projects (short only)'
task :test => [:build] do
  e "go test -short -timeout 10s ./..."
end

desc 'Test all projects'
task :test_all => [:build] do
  e "go test -timeout 120s ./..."
end

desc 'Update all dependencies'
task :update => :godep_check do
  e "go get -u -t -v ./..."
  e "godep update -r .../..."
end

desc 'Install all built binaries'
task :install do
  e "go install -a -ldflags \"-X github.com/square/p2/pkg/version.VERSION=$(git describe --tags)\" ./..."
end

task :errcheck do
  e "exit $(errcheck -ignoretests github.com/square/p2/pkg/... | grep -v defer | wc -l || 1)"
end

desc 'Package the installed P2 binaries into a Hoist artifact that runs as the preparer. The output tar is symlinked to builds/p2.tar.gz'
task :package => :install do
  root = File.dirname(__FILE__)
  os = `uname`.downcase.chomp
  arch = `uname -p`.downcase.chomp
  builds_dir = File.join(root, "builds", os)
  version_tag = `git describe --tags`.chomp
  build_base = "p2-#{version_tag}-#{os}-#{arch}"

  abort("Could not get version_tag") unless version_tag && version_tag != ""

  e "mkdir -p #{builds_dir}/#{build_base}/bin"
  Dir.glob(File.join(File.dirname(`which p2-preparer`.chomp), 'p2*')).each do |f|
    e "cp #{f} #{builds_dir}/#{build_base}/bin"
  end
  e "mv #{builds_dir}/#{build_base}/bin/p2-preparer #{builds_dir}/#{build_base}/bin/launch"

  e "tar -czf #{builds_dir}/#{build_base}.tar.gz -C #{builds_dir}/#{build_base} ."

  e "rm -f #{builds_dir}/p2.tar.gz"
  e "ln -s #{builds_dir}/#{build_base}.tar.gz #{builds_dir}/p2.tar.gz"
end

desc 'Run the vagrant integration tests. Will attempt to build first to save you some time.'
task :integration => :build do
  root = File.dirname(__FILE__)
  # suite.rb has a number of options, this one just runs the default one.
  script = File.expand_path("integration/suite.rb", root)
  exec "ruby #{script}"
end

desc 'By default, gather dependencies, build and test'
task :default => [:deps, :test, :install]

task :sync_consul_deps do
  p2_repo_dir = File.join(ENV["GOPATH"], "src", "github.com", "square", "p2")

  consul_vendor_contents = JSON.parse(File.read(File.join(ENV["GOPATH"], "src", "github.com", "hashicorp", "consul", "vendor", "vendor.json")))
  p2_godep_contents = JSON.parse(File.read(File.join(p2_repo_dir, "Godeps", "Godeps.json")))

  # build a map of package names to revisions from consul's vendor.json
  consul_deps = {}
  consul_vendor_contents["package"].each do |package|
    consul_deps[package["path"]] = package["revision"]
  end

  success_count = 0
  failed_count = 0
  skipped_count = 0

  # now iterate over our Godeps.json and find any versions that mismatch and print them
  p2_godep_contents["Deps"].each do |package|
    package_name = package["ImportPath"]
    consul_rev = consul_deps[package_name]
    next unless consul_rev

    our_rev = package["Rev"]
    if consul_rev != our_rev
      # grab just the first three items e.g. github.com/hashicorp/consul not github.com/hashicorp/consul/api/inner/package/thing
      first_three = package_name.split("/")[0..2]
      first_three_joined = first_three.join("/")

      # in case the consul dependency is new and not in $GOPATH, do a "go get" first
      e "go get #{first_three_joined}/..."
      begin
        Dir.chdir(File.join(ENV["GOPATH"], "src", *first_three)) do
          e "git fetch"
          older_commit = `git merge-base #{our_rev} #{consul_rev}`.chomp
          raise "could not determine which commit is newer for #{first_three_joined}" unless $?.exitstatus == 0
          if older_commit == our_rev
            e "git checkout #{consul_rev}"
          else
            puts "No action taken for #{package_name}: consul's version #{consul_rev} is older than P2's #{our_rev}"
            skipped_count += 1
            next
          end
        end

        Dir.chdir(p2_repo_dir) do
          e "godep update #{first_three.join("/")}/..."
        end

        puts "Successfully repaired mismatch in #{package_name}: consul has #{consul_rev} p2 had #{our_rev}"
        success_count += 1
      rescue => e
        puts "Failed to repair mismatch in #{package_name}: consul has #{consul_rev} p2 had #{our_rev}: #{e.message}"
        failed_count += 1
      end

    end
  end

  puts "Successfully updated #{success_count} packages"
  puts "Failed to updated #{failed_count} packages"
  puts "Skipped updating #{skipped_count} packages (because consul had an older version vendored)"
end
