module Mysqladmin
  class Replication
    include Mysqladmin::Arguments
    include Mysqladmin::Serialize
    
    class Status
      attr_accessor :replStatus
      # Valid arguments:
      #       {
      #         :source => The master server in the replication pair
      #         :replica => The slave server in the replication pair,
      #         :sbmLimit => The number of seconds behind the master the slave can be and still be considered "usably in sync"
      #       }
      def initialize(args={})
        @source = args[:source] || nil
        @replica = args[:replica] || nil
        @sbmLimit = args[:sbmLimit] || 600
        @replStatus = {}
      end
      
      # Valid arguments:
      #       {
      #         :slave => The slave server in the replication pair
      #       }
      def repStatusHost(args={})
        req(:required => [:slave],
            :argsObject => args)
        args.delete(:sql) if args.has_key?(:sql)
        dbh = Mysqladmin::Exec.new(:connectionName => args[:slave])
        dbh.go(:sql => "SHOW SLAVE STATUS")
        res = dbh.fetch_hash
        sbm = res["Seconds_Behind_Master"].to_i
        io = res["Slave_IO_Running"].upcase
        sql = res["Slave_SQL_Running"].upcase
        if((io != "YES") || (sql != "YES"))
          return false
        elsif(sbm > @sbmLimit)
          return false
        else
          return true
        end
      end
      
      # Valid arguments:
      #       {
      #         :poolName => the pool of servers on which you wish to gather replication status
      #       }
      def repStatusPool(args={})
        req(:required => [:poolName],
            :argsObject => args)
        threadPoolSize = args[:threadPoolSize] || 16
        pool = ThreadPool::Threadpool.new(threadPoolSize)
        Mysqladmin::Pool.connectionPools[args[:poolName]].each do |connectionName|
          pool.process {
            @replStatus[connectionName] = repStatusHost(:slave = connectionName)
          }
        end
        pool.join
      end
      
      def masterStatus(args={})
        args[:source] = @source unless args.has_key?(:source)
        req(:required => [:source],
            :argsObject => args)
        dbh = Mysqladmin::Exec.new(:connectionName = args[:source])
        dbh.go(:sql => "SHOW MASTER STATUS")
        res = dbh.fetch_hash
        return {
          :file => res["File"],
          :position => res["Position"],
          :binlogDoDb => res["Binlog_Do_DB"],
          :binlogIgnoreDb => res["Binlog_Ignore_DB"]
        }
      end
    end
  end
end
