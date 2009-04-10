module Mysqladmin
  class Replication
    include Mysqladmin::Arguments
    include Mysqladmin::Serialize
    include Mysqladmin::ServerInfo
    
    class Status
      attr_accessor :repl_status
      # :source => The master server in the replication pair
      # :replica => The slave server in the replication pair,
      # :sbm_limit => The number of seconds behind the master the slave can be and still be considered "usably in sync"
      def initialize(args={})
        @source = args[:source] || nil
        @replica = args[:replica] || nil
        @sbm_limit = args[:sbm_limit] || 600
        @repl_status = {}
      end
      
      # :slave => The slave server in the replication pair
      def repl_status_host(args={})
        req(:required => [:slave],
            :args_object => args)
        args.delete(:sql) if args.has_key?(:sql)
        dbh = Mysqladmin::Exec.new(:connection_name => args[:slave])
        dbh.query(:sql => "SHOW SLAVE STATUS")
        res = dbh.fetch_hash
        sbm = res["Seconds_Behind_Master"].to_i
        io = res["Slave_IO_Running"].upcase
        sql = res["Slave_SQL_Running"].upcase
        if((io != "YES") || (sql != "YES"))
          return false
        elsif(sbm > @sbm_limit)
          return false
        else
          return true
        end
      end
      
      # :pool_name => the pool of servers on which you wish to gather replication status
      def repl_status_pool(args={})
        req(:required => [:pool_name],
            :args_object => args)
        thread_pool_size = args[:thread_pool_size] || 16
        pool = ThreadPool::Threadpool.new(thread_pool_size)
        Mysqladmin::Pool.connection_pools[args[:pool_name]].each do |connection_name|
          pool.process {
            @repl_status[connection_name] = repl_status_host(:slave => connection_name)
          }
        end
        pool.join
      end
      
      # :source => The server on which you wish to gather the current master status
      def master_status(args={})
        args[:source] = @source unless args.has_key?(:source)
        req(:required => [:source],
            :args_object => args)
        dbh = Mysqladmin::Exec.new(:connection_name => args[:source])
        dbh.query(:sql => "SHOW MASTER STATUS")
        res = dbh.fetch_hash
        return {
          :file => res["File"],
          :position => res["Position"],
          :binlog_do_db => res["Binlog_Do_DB"],
          :binlog_ignore_db => res["Binlog_Ignore_DB"]
        }
      end
    end
    
    class Recover
      def initialize(args={})
        
      end  
    end
  end
end
