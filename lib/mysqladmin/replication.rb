module Mysqladmin
  class Replication
    include Mysqladmin::Arguments
    include Mysqladmin::Serialize
    
    class Status
      attr_accessor :replStatus
      # Valid arguments:
      #       {
      #         :connectionName => The slave server in the replication pair,
      #         :sbmLimit => The number of seconds behind the master the slave can be and still be considered "usably in sync"
      #       }
      def initialize(args={})
        @connectionName = args[:slave] || nil
        @sbmLimit = args[:sbmLimit] || 600
        @replStatus = {}
      end
      
      # Valid arguments:
      #       {
      #         :connectionName => The slave server in the replication pair
      #       }
      def repStatusHost(args={})
        @connectionName = args[:connectionName] unless @connectionName
        req(:required => [:connectionName],
            :argsObject => args)
        args.delete(:sql) if args.has_key?(:sql)
        dbh = Mysqladmin::Exec.new(:connectionName => @connectionName, :sql => "SHOW SLAVE STATUS")
        dbh.go
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
        threadPoolSize = args[:threadPoolSize] || 16
        pool = ThreadPool::Threadpool.new(threadPoolSize)
        Mysqladmin::Pool.connectionPools[args[:poolName]].each do |connectionName|
          pool.process {
            @replStatus[connectionName] = repStatusHost(:connectionName = connectionName)
          }
        end
        pool.join
      end
    end
  end
end
