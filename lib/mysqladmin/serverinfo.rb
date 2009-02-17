module Mysqladmin
  module ServerInfo
    include Mysqladmin::Arguments
    
    # :connectionName => Name of the connection on which to run the sql
    #                    "SELECT VERSION()"
    def serverVersion(args)
      # Mandatory args:
      req(:required => [:connectionName],
          :argsObject => args)
      longVersion(:connectionName => args[:connectionName]).split(".").map!{|x| x.gsub(/\D/, "").to_i}
    end
    
    def longVersion(args)
      req(:required => [:connectionName],
          :argsObject => args)
      dbh = Mysqladmin::Exec.new(:connectionName => args[:connectionName],
                                 :sql => "SELECT VERSION()")
      dbh.go
      dbh.fetch_hash["VERSION()"]
    end
  end
end