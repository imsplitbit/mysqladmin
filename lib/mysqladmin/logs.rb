module Mysqladmin
  class Logs
    include Mysqladmin::System
    include Mysqladmin::Arguments
    include Mysqladmin::ServerInfo
    
    attr_accessor :time
    attr_reader :binLogs, :slowLog, :genLog, :relayLog
    
    def initialize(args={})
      req(:required => [:connectionName], :argsObject => args)
      @start = Time.now
      @binLogs = {}
      @slowLog = nil
      @genLog = nil
      @relayLogs = {}
      @vars = serverVariables(:connectionName => args[:connectionName], :type => "variables")
      @status = serverVariables(:connectionName => args[:connectionName], :type => "status")
      parseMyCnf
    end
    
    def parseMyCnf
      myCnfFiles = ["/etc/my.cnf", "#{@vars[:datadir]}/my.cnf"]
      myCnfFiles.each do |file|
        if line.include?("!include")
          myCnfFiles << line.split(" ")[1]
        end
      end
      myCnfFiles.each do |file|
        if File.exist?(file)
          File.readlines(file).each do |line|
            var = line.split("=")[0].gsub("-", "_")
            val = line.split("=")[1]
            if var[/log_bin/]
              Dir.glob("#{val}*").delete_if{|x| x[/index$/]}.each do |binLog|
                binLogName = File.basename(binlog)
                @binLogs[binLogName] = {}
                @binLogs[binLogName][:path] = binLog
              end
            elsif var[/relay_log/]
              Dir.glob("#{val}*").delete_if{|x| x[/index/]}.each do |relayLog|
                relayLogName = File.basename(relayLog)
                @relayLogs[relayLog] = {}
                @relayLogs[relayLog][:path] = relayLog
              end
            elsif var[/log_slow_queries/]
              @slowLog = val
            elsif var[/^log$/]
              @genLog = val
            end
          end
        end
      end
    end
    
  end
end