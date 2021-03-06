module Mysqladmin
  class Exec
    include Mysqladmin::Arguments
    
    attr_accessor :sql
    # :sql => SQL to run on args[:connection_name] from pool object Mysqladmin::Pool,
    # :connection_name => Name of the connection we are using to execute args[:sql]
    def initialize(args)
      @sql = args[:sql] || nil
      
      #Mandatory args:
      req(:required => [:connection_name],
          :args_object => args)
      @dbh = Mysqladmin::Pool.connections[args[:connection_name]][:dbh]
    end
    
    def query(args = {})
      args[:sql] = @sql unless args.has_key?(:sql)
      
      # Mandatory args:
      req(:required => [:sql],
          :args_object => args)
      
      @res = @dbh.query(cleanse(args))
      if @res.class == Mysql::Result
        return true
      else
        return false
      end
    end
    
    def each_hash
      if @res
        @res.each_hash do |res|
          yield res
        end
      end
    end
    
    def fetch_hash
      if @res
        @res.fetch_hash
      end
    end
    
    def num_rows
      @res.num_rows
    end
    
    # This method will be used to sanitize all sql fed into args[:sql] before it
    # is executed.  It pretty much sux now cause all it does is spit the same
    # sql back out.  But it will get there, be patient.
    #
    # :sql => SQL statement to sanitize, returns String
    def cleanse(args)
      # Clean sql, need to add code to escape quotes and prevent deletions without conditions
      @sql
    end
    
    def create_db(args)
    	sql_new = @sql
    	query(:sql => "CREATE DATABASE `#{args[:db_name]}`")
    	@sql = sql_new
    end
    
    # the following are just wrappers for the native mysql client lib functions
    def use(args)
      @dbh.select_db(args[:db_name])
    end
    
    def list_tables
      @dbh.list_tables
    end
    
    def list_processes
      @res = @dbh.list_processes
    end
    
    def list_dbs
      @dbh.list_dbs
    end
  end
end
