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
      
      def database(args={})
        args[:connectionName] = @connectionName unless args.has_key?(:connectionName)
        req(:required => [:dbName],
            :argsObject => args)
        data = {}
        dbh = Mysqladmin::Exec.new(:connectionName => args[:connectionName])
        dbh.use(args[:dbName])
        dbh.listTables.each do |tableName|
          data[tableName] = table(:tableName => tableName)
        end
        return data
      end
      
    end
  end
end
