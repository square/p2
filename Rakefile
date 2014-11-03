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
task :build => :godep_check do
  e "godep go build -v ./..."
end

namespace :artifact do

  desc 'Build the hoist artifact for the preparer'
  task :preparer => :godep_check do
    e "godep go build ./bin/preparer" # this will put a copy of preparer in the root
    preparer_bin = target('preparer/bin')
    preparer_launch = target('preparer/bin/launch')
    preparer_enable = target('preparer/bin/enable')
    preparer_disable = target('preparer/bin/disable')
    e "rm -rf #{preparer_bin}"
    e "mkdir -p #{preparer_bin}"
    e "mv preparer #{preparer_launch}"
    # e "touch #{preparer_enable}"
    # e "chmod 744 #{preparer_enable}"
    # e "touch #{preparer_disable}"
    # e "chmod 744 #{preparer_disable}"
    sha = `git rev-parse HEAD`.strip
    tar_out = target("preparer_#{sha}.tar.gz")
    e "tar -cvzf #{tar_out} -C #{target('preparer')} ."
  end

end

desc 'Test all projects'
task :test => [:godep_check, :build] do
  e "godep go test -timeout 10s -v ./..."
end

desc 'Update all dependencies'
task :update => :godep_check do
  e "go get -u -t -v ./..."
  e "godep update .../..."
end

desc 'Install all built binaries'
task :install => :godep_check do
  e "godep go install ./..."
end

desc 'By default, gather dependencies, build and test'
task :default => [:deps, :test, :install]
