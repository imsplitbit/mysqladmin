module Mysqladmin
  class Replication
    include Mysqladmin::Arguments
    include Mysqladmin::Serialize
    
    class Status
      def initialize(args = {})
        @master = args[:master] || nil
        @slave = args[:slave] || nil
        @sbmLimit = args[:sbmLimit] || 600
        req(:required => [:master, :slave, :sbmLimit],
            :argsObject => args)
      end
  
      def repStatusHost(args={})
        @master = args[:master] unless @master
        @slave = args[:slave] unless @slave
        req(:required => [:master, :slave],
            :argsObject => args)
        args.delete(:sql) if args.has_key?(:sql)
        dbh = Mysqladmin::Exec.new(:connectionName => @slave, :sql => "SHOW SLAVE STATUS")
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
  
      def repStatusPool(args={})
        
      end
    end
    
    class Sync
      def initialize(args={})
        @master = args[:master] || nil
        @slave = args[:slave] || nil
      end
      
      def syncTable(args={})
        
      end
      
      def syncHost(args={})
        
      end
      
      def syncPool(args={})
        
      end
    end
  end
end
