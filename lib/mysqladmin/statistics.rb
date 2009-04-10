module Mysqladmin
  class Statistics
    include Mysqladmin::Arguments
    
    # :connection_name => The named connection to use to gather statistics on
    def initialize(args={})
      @connection_name = args[:connection_name] || nil
    end
    
    # :connection_name => The named connection to use to gather table statistics on,
    # :table_name => The table we want statistics on,
    # :db_name => The name of the database the table belongs to
    def table(args={})
      args[:connection_name] = @connection_name unless args.has_key?(:connection_name)
      req(:required => [:table_name, :db_name],
          :args_object => args)
      dbh = Mysqladmin::Exec.new(:connection_name => args[:connection_name])
      dbh.use(args[:db_name])
      dbh.query(:sql => "SHOW TABLE STATUS LIKE '#{args[:table_name]}'")
      if dbh.rows > 0
        dbh.fetch_hash do |table_data|
          return {
            :table_name => table_data["Name"],
            :engine => table_data["Engine"].downcase,
            :data_length => table_data["Data_length"].to_i,
            :index_length => table_data["Index_length"].to_i,
            :total_length => (table_data["Data_length"].to_i + table_data["Index_length"].to_i),
            :collation => table_data["Collation"].downcase,
            :rows => table_data["Rows"].to_i,
            :avg_row_length => table_data["Avg_row_length"].to_i,
            :max_data_length => table_data["Max_data_length"].to_i,
            :row_format => table_data["Row_format"].downcase
          }
        end
      end
      
      # :connection_name => The named connection to use for database statistics,
      # :db_name => The database to gather statistics on
      def database(args={})
        args[:connection_name] = @connection_name unless args.has_key?(:connection_name)
        req(:required => [:db_name],
            :args_object => args)
        data = {}
        dbh = Mysqladmin::Exec.new(:connection_name => args[:connection_name])
        dbh.use(args[:db_name])
        dbh.list_tables.each do |table_name|
          data[table_name] = table(:table_name => args[:table_name], :db_name => args[:db_name], :connection_name => args[:connection_name])
        end
        return data
      end
      
    end
  end
end
