module Mysqladmin
  class Statistics
    include Mysqladmin::Arguments
    
    def initialize(args={})
      @connectionName = args[:connectionName] || nil
    end
    
    def table(args={})
      args[:connectionName] = @connectionName unless args.has_key?(:connectionName)
      req(:required => [:tableName, :dbName],
          :argsObject => args)
      dbh = Mysqladmin::Exec.new(:connectionName = args[:connectionName])
      dbh.use(args[:dbName])
      dbh.go(:sql => "SHOW TABLE STATUS LIKE '#{args[:tableName]}'")
      if dbh.rows > 0
        
      end
    end
  end
end
