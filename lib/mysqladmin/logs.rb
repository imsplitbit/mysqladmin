module Mysqladmin
  class Logs
    include Mysqladmin::System
    include Mysqladmin::Arguments
    
    attr_accessor :time
    
    def initialize(args={})
      @start = Time.now
      @binLogs = {}
      @slowLog = nil
      @genLog = nil
      @relayLog = nil
      
    end
  end
end