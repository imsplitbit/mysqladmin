require "mysqladmin/arguments"
require "mysqladmin/serialize"
require "mysqladmin/system"

module Mysqladmin
  class ClusterMgmt
    include Mysqladmin::Arguments
    include Mysqladmin::Serialize
    include Mysqladmin::System
    
    # :mgmt_server => IP or hostname of the management server
    def initialize(args = {})
      @mgmt_server = args[:mgmt_server] || "localhost"
      
    end
    
    # :id => Custom backup id, defaults to Time.now.strftime("%m%d%H%M")
    def start_backup(args = {})
      args[:id] = Time.now.strftime("%m%d%H%M") unless args.has_key?(:id)
      results = `#{core_reqs(:binary => "ndb_mgm")} #{@mgmt_server} -e "START BACKUP #{args[:id]} WAIT COMPLETED"`
      if results.split("\n")[3].split.last == "completed"
        true
      else
        false
      end
    end
  end
end