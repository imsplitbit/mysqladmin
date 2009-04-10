module Mysqladmin
  module ServerInfo
    include Mysqladmin::Arguments
    
    # :connection_name => Name of the connection on which to run the sql
    #                    "SELECT VERSION()"
    def server_version(args)
      # Mandatory args:
      req(:required => [:connection_name],
          :args_object => args)
      long_version(:connection_name => args[:connection_name]).split(".").map!{|x| x.gsub(/\D/, "").to_i}
    end
    
    def long_version(args)
      req(:required => [:connection_name],
          :args_object => args)
      dbh = Mysqladmin::Exec.new(:connection_name => args[:connection_name],
                                 :sql => "SELECT VERSION()")
      dbh.go
      dbh.fetch_hash["VERSION()"]
    end
    
    # :connection_name => The named connection to use for database variables
    def server_variables(args = {})
      args[:type] = "VARIABLES" unless args.has_key?(:type)
      req(:required => [:connection_name], :args_object => args)
      valid_types = ["VARIABLES", "STATUS"]
      unless valid_types.include?(args[:type].upcase)
        raise ArgumentError, "The type #{args[:type]} is unknown"
      end
      data = {}
      major, minor, patch = server_version(:connection_name => args[:connection_name])
      dbh = Mysqladmin::Exec.new(:connection_name => args[:connection_name])
      if major <= 4
        dbh.sql = "SHOW #{args[:type]}"
      else
        dbh.sql = "SHOW GLOBAL #{args[:type]}"
      end
      dbh.go
      dbh.each_hash do |row|
        value = row["Value"]
        if value[/^\d+$/]
          value = value.to_i
        end
        data[row["Variable_name"].to_sym] = value
      end
      return data
    end
    
  end
end