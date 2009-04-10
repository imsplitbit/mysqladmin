module Mysqladmin
  class Sync
    include Mysqladmin::Arguments
    include Mysqladmin::Serialize
    
    def initialize(args={})
      @source = args[:source] || nil
      @replica = args[:replica] || nil
    end
    
    def sync_table(args={:overwrite_if_exists => true, :crash_if_exists => false, :sync_slave => true})
      req(:required => [:db_name, :table_name],
          :args_object => args)
      source = args.has_key?(:source) ? args[:source] : @source
      replica = @replica unless args.has_key?(:replica)
      
      bu_job = Mysqladmin::Backup.new
      bu_job.backup_host(:per_table => true,
                       :only_these_tables => [args[:table_name]],
                       :src_host => source)
      bu_job.restore_db_from_backup(:src_db => args[:db_name],
                                :src_host => source,
                                :dest_host => replica,
                                :overwrite_if_exists => true)
      # src_table_struct = {}
      #       rep_table_struct = {}
      #       
      #       # Gather table statistics
      #       src_table_data = Mysqladmin::Statistics.new(:connection_name => source).table(:table_name => args[:table_name], :db_name => args[:db_name])
      #       
      #       # Get the source table structure
      #       sdbh = Mysqladmin::Exec.new(:connection_name => @source)
      #       sdbh.use(args[:db_name])
      #       sdbh.go(:sql => "SHOW CREATE TABLE #{args[:table_name]}")
      #       if sdbh.rows == 1
      #         src_create_table = sdbh.fetch_hash["Create Table"].gsub("\n", "")
      #       else
      #         return false
      #       end
      #       
      #       # get the source table description
      #       sdbh.go(:sql => "DESCRIBE #{args[:table_name]}")
      #       if sdbh.rows > 0
      #         sdbh.each_hash do |row|
      #           key = row["Field"]
      #           row.delete(key)
      #           src_table_struct[key] = row
      #         end
      #       else
      #         return false
      #       end
      #       
      #       # Get the replica table structure
      #       rdbh = Mysqladmin::Exec.new(:connection_name => @replica)
      #       rdbh.use(args[:db_name])
      #       rdbh.go(:sql => "SHOW CREATE TABLE #{args[:table_name]}")
      #       if rdbh.rows == 1
      #         rep_create_table = rdbh.fetch_hash["Create Table"].gsub("\n", "")
      #         rdbh.go(:sql => "DESCRIBE #{args[:table_name]}")
      #         rdbh.each_hash do |row|
      #           key = row["Field"]
      #           row.delete(key)
      #           rep_table_struct[key] = row
      #         end
      #       else
      #         rep_create_table = nil
      #       end
      #       
      #       if rep_create_table == nil
      #         rdbh.go(:sql => src_create_table)
      #       else
      #         # Diff the 2 tables
      #         src_table_struct.keys.each do |field_name|
      #           src_table_struct[field_name].keys.each do |field_attribute|
      #             if(rep_table_struct[field_name].has_key?(field_attribute) && (rep_table_struct[field_name][field_attribute] != src_table_struct[field_name][field_attribute]))
      #               case field_attribute
      #                 when "Type" then rdbh.go(:sql => "ALTER TABLE #{args[:table_name]} MODIFY '#{field_name}' #{src_table_struct[field_name][field_attribute]}")
      #                 when "Null" then rdbh.go(:sql => "ALTER TABLE #{args[:table_name]} MODIFY '#{field_name}' #{src_table_struct[field_name][field_attribute] == "NO" ? "NOT NULL" : "NULL"}")
      #                 when "Default" then rdbh.go(:sql => "ALTER TABLE #{args[:table_name]} MODIFY '#{field_name}' DEFAULT '#{src_table_struct[field_name][field_attribute]}'")
      #               end
      #             end
      #           end
      #         end
      
      if (table_data[:rows] < 100000) && (table_data[:data_length] < 134217728)
        sdbh.go(:sql => "SELECT *")
      end
    end
    
    def sycn_db(args={})
      raise NoMethodError, "Not implemented yet"
    end
    
    def syncHosts(args={})
      raise NoMethodError, "Not implemented yet"
    end

  end
end