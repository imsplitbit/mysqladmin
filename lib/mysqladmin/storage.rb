module Mysqladmin
  class Storage
    include Mysqladmin::Arguments
    
    def initialize(args={})
      @connectionName = args[:connectionName] || nil
    end
    
    def table(args={})
      args[:connectionName] = @connectionName unless args.has_key?(:connectionName)
      req(:required => [:tableName, :dbName],
          :argsObject => args)
      dbh = Mysqladmin::Exec.new(:connectionName = args[:connectionName])
    end
  end
end
