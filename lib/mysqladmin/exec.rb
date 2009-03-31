module Mysqladmin
  class Exec
    include Mysqladmin::Arguments
    
    attr_accessor :sql
    # :sql => SQL to run on args[:connectionName] from pool object Mysqladmin::Pool,
    # :connectionName => Name of the connection we are using to execute args[:sql]
    def initialize(args)
      @sql = args[:sql] || nil
      
      #Mandatory args:
      req(:required => [:connectionName],
          :argsObject => args)
      @dbh = Mysqladmin::Pool.connections[args[:connectionName]][:dbh]
    end
    
    # Kind of a dumb name, Object.go, but hey we just want it to... well... GO
    #
    # Obvious to me but if not to you, Object.go means execute @sql on
    # @connectionName from Mysqladmin::Pool.  No args can be given, if you want to modify
    # the sql being "GO'd" then use Object.sql="DELETE ALL OF MY STUFF" or
    # or something equally fun.  Wheeeee!!!!
    def go(args = {})
      args[:sql] = @sql unless args.has_key?(:sql)
      
      # Mandatory args:
      req(:required => [:sql],
          :argsObject => args)
      
      @res = @dbh.query(cleanse(args))
      if @res.class == Mysql::Result
        return true
      else
        return false
      end
    end
    
    def query(args = {})
      args[:sql] = @sql unless args.has_key?(:sql)
      
      # Mandatory args:
      req(:required => [:sql],
          :argsObject => args)
      
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
    
    def rows
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
    
    def createDb(dbName)
      sql = @sql
      query :sql => "CREATE DATABASE `#{dbName}`"
      @sql = sql
    end
    
    # the following are just wrappers for the native mysql client lib functions
    def use(args)
      @dbh.select_db(args[:dbName])
    end
    
    def listTables
      @dbh.list_tables
    end
    
    def processList
      @res = @dbh.list_processes
    end
    
    def listDbs
      @dbh.list_dbs
    end
  end
end
