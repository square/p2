def e(cmd)
  puts cmd
  system(cmd) || abort("Error running `#{cmd}`")
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

desc 'Test all projects'
task :test => [:build] do
  e "go test -timeout 10s ./..."
end

desc 'Update all dependencies'
task :update => :godep_check do
  e "go get -u -t -v ./..."
  e "godep update -r .../..."
end

desc 'Install all built binaries'
task :install do
  e "go install -a -ldflags \"-X github.com/square/p2/pkg/version.VERSION $(git describe --tags)\" ./..."
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
