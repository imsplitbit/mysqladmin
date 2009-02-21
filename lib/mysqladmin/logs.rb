module Mysqladmin
  class Logs
    include Mysqladmin::System
    include Mysqladmin::Arguments
    include Mysqladmin::ServerInfo
    
    attr_accessor :time, :foo
    
    def initialize(args={})
      req(:required => [:connectionName], :argsObject => args)
      @start = Time.now
      @binLogs = {}
      @slowLog = nil
      @genLog = nil
      @relayLog = nil
      @vars = serverVariables(:connectionName => args[:connectionName], :type => "variables")
      @status = serverVariables(:connectionName => args[:connectionName], :type => "status")
    end
    
    def findLogs(args = {})
      
    end
    
    def parseMyCnf
      myCnfFiles = ["/etc/my.cnf", "#{@vars[:datadir]}/my.cnf"]
      myCnfFiles.each do |file|
        if File.exist?
      end
    end
  end
end