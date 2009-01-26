module Mysqladmin
  class Sync
    include Mysqladmin::Arguments
    include Mysqladmin::Serialize
    
    def initialize(args={})
      @source = args[:source] || nil
      @replica = args[:replica] || nil
    end
    
    def syncTable(args={})
      @source = args[:source] || @source
      @replica = args[:replica] || @replica
      req(:required => [:dbName, :tableName],
          :argsObject => args)
      
      srcTableStruct = {}
      repTableStruct = {}
      
      # Get the source table structure
      sdbh = Mysqladmin::Exec.new(:connectionName => @source)
      sdbh.use(args[:dbName])
      sdbh.go(:sql => "SHOW CREATE TABLE #{args[:tableName]}")
      if sdbh.rows == 1
        srcCreateTable = sdbh.fetch_hash["Create Table"].gsub("\n", "")
      else
        return false
      end
      
      # get the source table description
      sdbh.go(:sql => "DESCRIBE #{args[:tableName]}")
      if sdbh.rows > 0
        sdbh.each_hash do |row|
          key = row["Field"]
          row.delete(key)
          srcTableStruct[key] = row
        end
      else
        return false
      end
      
      # Get the replica table structure
      rdbh = Mysqladmin:Exec.new(:connectionName => @replica)
      rdbh.use(args[:dbName])
      rdbh.go(:sql => "SHOW CREATE TABLE #{args[:tableName]}")
      if rdbh.rows == 1
        repCreateTable = rdbh.fetch_hash["Create Table"].gsub("\n", "")
        rdbh.go(:sql => "DESCRIBE #{args[:tableName]}")
        rdbh.each_hash do |row|
          key = row["Field"]
          row.delete(key)
          repTableStruct[key] = row
        end
      else
        repCreateTable = nil
      end
      
      if repCreateTable == nil
        rdbh.go(:sql => srcCreateTable)
      else
        # Diff the 2 tables
        srcTableStruct.keys.each do |fieldName|
          srcTableStruct[fieldName].keys.each do |fieldAttribute|
            if(repTableStruct[fieldName].has_key?(fieldAttribute) && (repTableStruct[fieldName][fieldAttribute] != srcTableStruct[fieldName][fieldAttribute]))
              case fieldAttribute:
              when "Type" then rdbh.go(:sql => "ALTER TABLE #{args[:tableName]} MODIFY '#{fieldName}' #{srcTableStruct[fieldName][fieldAttribute]}")
              when "Null" then rdbh.go(:sql => "ALTER TABLE #{args[:tableName]} MODIFY '#{fieldName}' #{srcTableStruct[fieldName][fieldAttribute] == "NO" ? "NOT NULL" : "NULL"}")
              when "Default" then rdbh.go(:sql => "ALTER TABLE #{args[:tableName]} MODIFY '#{fieldName}' DEFAULT '#{srcTableStruct[fieldName][fieldAttribute]}'")
              end
            end
          end
        end
      end
    end
    
    def syncDb(args={})
      
    end
    
    def syncHost(args={})
      
    end
    
    def syncPool(args={})
      
    end
  end
end