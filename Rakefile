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

desc 'Test all projects'
task :test => [:godep_check, :build] do
  e "godep go test -timeout 10s ./..."
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

desc 'Run the vagrant integration tests. Will attempt to build first to save you some time.'
task :integration => :build do
  root = File.dirname(__FILE__)
  # suite.rb has a number of options, this one just runs the default one.
  script = File.expand_path("integration/suite.rb", root)
  exec "ruby #{script}"
end

desc 'By default, gather dependencies, build and test'
task :default => [:deps, :test, :install]
