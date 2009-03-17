module Mysqladmin
  class Logs
    include Mysqladmin::System
    include Mysqladmin::Arguments
    include Mysqladmin::ServerInfo
    
    attr_accessor :time
    attr_reader :binaryLogs, :slowLog, :genLog, :relayLog, :lastEntry, :lastDb, :tables
    
    def initialize(args={})
      req(:required => [:connectionName], :argsObject => args)
      @start = Time.now
      @binaryLogs = {}
      @slowLog = nil
      @generalLog = nil
      @relayLogs = {}
      @tables = []
      @vars = serverVariables(:connectionName => args[:connectionName], :type => "variables")
      @status = serverVariables(:connectionName => args[:connectionName], :type => "status")
      parseMyCnf
    end
    
    def parseMyCnf
      myCnfFiles = ["/etc/my.cnf", "#{@vars[:datadir]}my.cnf"]
      myCnfFiles.each do |file|
        if File.exist?(file)
          File.readlines(file).each do |line|
            if line.include?("!include")
              myCnfFiles << file.split(" ")[1].strip
            end
          end
        end
      end
      myCnfFiles.each do |file|
        if File.exist?(file)
          File.readlines(file).each do |line|
            if line.include?("=")
              var = line.split("=")[0].gsub("-", "_").strip
              val = line.split("=")[1].strip
              if var[/^log_bin$/i]
                Dir.glob("#{val}*").delete_if{|x| x[/index$/]}.each do |binLog|
                  binLogName = File.basename(binLog)
                  @binaryLogs[binLogName] = {}
                  @binaryLogs[binLogName][:path] = binLog
                end
              elsif var[/^relay_log$/i]
                Dir.glob("#{val}*").delete_if{|x| x[/index$/]}.each do |relayLog|
                  relayLogName = File.basename(relayLog)
                  @relayLogs[relayLog] = {}
                  @relayLogs[relayLog][:path] = relayLog
                end
              elsif var[/^log_slow_queries$/i]
                @slowLog = val
              elsif var[/^log$/i]
                @generalLog = val
              end
            end
          end
        end
      end
    end
    
    def listEntry(args = {})
      req(:required => [:type, :fileName, :position], :argsObject => args)
      validTypes = [:binary, :relay]
      if validTypes.include?(args[:type])
        if args[:type] == :relay 
          logFile = @relayLogs[args[:fileName]][:path]
        elsif args[:type] == :binary
          logFile = @binaryLogs[args[:fileName]][:path]
        end
        
        @lastEntry = `#{coreReqs(:binary => "/usr/local/mysql/bin/mysqlbinlogs")} --start-position=#{args[:position]} --stop-position=#{args[:position].to_i + 1} #{args[:fileName]}`.split(";").map{|x| x.strip}
        @lastEntry.each do |entry|
          dbName = entry.match(/use (.+)/i)
          unless dbName == nil
            @lastDb = dbName[1]
          end
        end
        @lastEntry.each do |entry|
          entry = entry.gsub("\n", "").gsub(/\s+/, " ")
          if entry[/^insert/i]
            x = entry.match(/into (.+?)(value|\(| )/i)
          elsif entry[/^update/i]
            x = entry.match(/update (.+?) set/i)
          elsif entry[/^delete/i]
            x = entry.match(/from (.+?) (where|order|limit|$).*/)
          end
          if x[1].include?(",")
            @tables + x[1].split(",").map{ |x| x.strip! }
          else
            @tables + x[1].strip.to_a
          end
        end
      end
    end
    
  end
end