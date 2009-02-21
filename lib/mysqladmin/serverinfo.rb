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
    
    # :connectionName => The named connection to use for database variables
    def serverVariables(args = {})
      args[:type] = "VARIABLES" unless args.has_key?(:type)
      req(:required => [:connectionName], :argsObject => args)
      validTypes = ["VARIABLES", "STATUS"]
      unless validTypes.include?(args[:type].upcase)
        raise ArgumentError, "The type #{args[:type]} is unknown"
      end
      data = {}
      major, minor, patch = serverVersion(:connectionName => args[:connectionName])
      dbh = Mysqladmin::Exec.new(:connectionName => args[:connectionName])
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
        symkey = row["Variable_name"].split("_")
        counter = 0
        symkey.size.times do
          if counter == 0
            symkey[counter] = symkey[counter].downcase
          else
            symkey[counter] = symkey[counter].capitalize
          end
          counter += 1
        end
        data[symkey.join.to_sym] = value
      end
      return data
    end
  end
end