def e(cmd)
  puts cmd
  system(cmd) || abort("Error running `#{cmd}`")
end

task :godep_check do
  system("which godep") || abort("You do not have godep installed. Run `go get github.com/tools/godep` and ensure that it's on your PATH")
end

desc 'Get deps for all projects.'
task :deps => :godep_check do
  e "go get -d -v ./..."
  e "godep save ./..."
end

desc 'Build all projects'
task :build do
  e "godep go build -v ./..."
end

desc 'Test all projects'
task :test => :build do
  e "godep go test -v ./..."
end

desc 'Update all dependencies'
task :update => :godep_check do
  e "go get -u ./..."
  e "godep update .../..."
end

desc 'By default, gather dependencies, build and test'
task :default => [:deps, :test]