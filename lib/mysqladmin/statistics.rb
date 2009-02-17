module Mysqladmin
  class Statistics
    include Mysqladmin::Arguments
    
    # :connectionName => The named connection to use to gather statistics on
    def initialize(args={})
      @connectionName = args[:connectionName] || nil
    end
    
    # :connectionName => The named connection to use to gather table statistics on,
    # :tableName => The table we want statistics on,
    # :dbName => The name of the database the table belongs to
    def table(args={})
      args[:connectionName] = @connectionName unless args.has_key?(:connectionName)
      req(:required => [:tableName, :dbName],
          :argsObject => args)
      dbh = Mysqladmin::Exec.new(:connectionName = args[:connectionName])
      dbh.use(args[:dbName])
      dbh.go(:sql => "SHOW TABLE STATUS LIKE '#{args[:tableName]}'")
      if dbh.rows > 0
        dbh.fetch_hash do |tableData|
          return {
            :tableName => tableData["Name"],
            :engine => tableData["Engine"].downcase,
            :dataLength => tableData["Data_length"].to_i,
            :indexLength => tableData["Index_length"].to_i,
            :totalLength => (tableData["Data_length"].to_i + tableData["Index_length"].to_i),
            :collation => tableData["Collation"].downcase,
            :rows => tableData["Rows"].to_i,
            :avgRowLength => tableData["Avg_row_length"].to_i,
            :maxDataLength => tableData["Max_data_length"].to_i,
            :rowFormat => tableData["Row_format"].downcase
          }
        end
      end
      
      # :connectionName => The named connection to use for database statistics,
      # :dbName => The database to gather statistics on
      def database(args={})
        args[:connectionName] = @connectionName unless args.has_key?(:connectionName)
        req(:required => [:dbName],
            :argsObject => args)
        data = {}
        dbh = Mysqladmin::Exec.new(:connectionName => args[:connectionName])
        dbh.use(args[:dbName])
        dbh.listTables.each do |tableName|
          data[tableName] = table(:tableName => args[:tableName], :dbName => args[:dbName], :connectionName => args[:connectionName])
        end
        return data
      end
      
      # :connectionName => The named connection to use for database variables
      def serverVariables(args={})
        args[:connectionName] = @connectionName unless args.has_key?(:connectionName)
        req(:required => [:connectionName],
            :argsObject => args)
        status = {}
        major, minor, patch = serverVersion(:connectionName => args[:connectionName])
        dbh = Mysqladmin::Exec.new(:connectionName => args[:connectionName])
        if major <= 4
          dbh.go(:sql => "SHOW STATUS")
        else
          dbh.go(:sql => "SHOW GLOBAL STATUS")
        end
        res = dbh.fetch_hash
        res.keys.each do |key|
          value = res[key]
          if value[/^\d+$/]
            value = value.to_i
          end
          symkey = key.split("_")
          counter = 0
          symkey.size.times do
            if counter == 0
              symkey[counter] = symkey[counter].downcase
            else
              symkey[counter] = symkey[counter].capitalize
            end
            counter += 1
          end
          status[symkey.join.to_sym] = res[key]
        end
        return status
      end
      
    end
  end
end
