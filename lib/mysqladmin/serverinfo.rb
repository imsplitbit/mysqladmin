module Mysqladmin
  module ServerInfo
    include Mysqladmin::Arguments
    
    # Valid arguments:
    # {
    #   :connectionName => Name of the connection on which to run the sql
    #                      "SELECT VERSION()"
    # }
    def serverVersion(args)
      # Mandatory args:
      req(:required => [:connectionName],
          :argsObject => args)
      args.delete(:sql) if args.has_key?(:sql)
      dbh = Mysqladmin::Exec.new(:connectionName => args[:connectionName],
                                 :sql => "SELECT VERSION()")
      dbh.go
      res = dbh.fetch_hash
      res["VERSION()"].split(".").map!{|x| x.gsub(/\D/, "").to_i}
    end
  end
end
