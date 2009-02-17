require "mysqladmin/arguments"
require "mysqladmin/serialize"
require "mysqladmin/system"

module Mysqladmin
  class ClusterMgmt
    include Mysqladmin::Arguments
    include Mysqladmin::Serialize
    include Mysqladmin::System
    
    # :mgmtServer => IP or hostname of the management server
    def initialize(args = {})
      @mgmtServer = args[:mgmtServer] || "localhost"
      
    end
    
    # :id => Custom backup id, defaults to Time.now.strftime("%m%d%H%M")
    def startBackup(args = {})
      args[:id] = Time.now.strftime("%m%d%H%M") unless args.has_key?(:id)
      results = `#{coreReqs(:binary => "ndb_mgm")} #{@mgmtServer} -e "START BACKUP #{args[:id]} WAIT COMPLETED"`
      if results.split("\n")[3].split.last == "completed"
        true
      else
        false
      end
    end
  end
end