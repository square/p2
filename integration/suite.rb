#!/usr/bin/env ruby
require 'optparse'
require 'shellwords'

class String
  # colorization
  def colorize(color_code)
    "\e[#{color_code}m#{self}\e[0m"
  end

  def red
    colorize(31)
  end

  def green
    colorize(32)
  end

  def yellow
    colorize(33)
  end

  def pink
    colorize(35)
  end
end

# TODO read a conf file in the test dir specifying what commands to run on what VMs

options = {:regex => /.*/, :test_command => 'go run %s/*.go'}
parser = OptionParser.new do |opts|
  opts.on('-p', '--pattern=PATTERN', 'Only run tests that match the given regex') do |p|
    options[:regex] = Regexp.new(p)
  end
  opts.on('-t', '--test-command=COMMAND', 'Run the given test command on the VM. The command will expand the first %s as the test directory.') do |t|
    options[:test_command] = t
  end
end

parser.parse!

abort("You must have vagrant installed") unless system('which vagrant')

path = File.dirname(__FILE__)
Dir.glob(File.join(path, '*/')).each do |test_dir|
  test_name = File.basename(test_dir)
  Dir.chdir(test_dir) do
    puts "Launching test environment in #{test_name}".yellow
    unless system('stat Vagrantfile')
      $stderr.puts "No Vagrantfile found, is this test set up properly?".red
      next
    end
    unless system('vagrant up')
      $stderr.puts "Couldn't start test #{test_name}, skipping".red
      next
    end
    begin
      to_execute = sprintf(options[:test_command], sprintf("%s/%s", "$GOPATH/src/github.com/square/p2", test_dir))
      puts "Starting test #{test_name}: #{Shellwords.escape(to_execute)}".yellow
      unless system("vagrant ssh -c '#{to_execute}'")
        puts "#{test_name} #{'FAILED'.red}"
        next
      end
      puts "#{test_name} #{'SUCCEEDED'.green}"
    ensure
      puts "Halting VM".yellow
      unless system("vagrant halt")
        puts "Tried to halt #{test_name} but it failed".red
      end
    end
  end if options[:regex].match(test_name)
end
