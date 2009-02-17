module Mysqladmin
  class Operations
    class Table
      include Mysqladmin::Arguments
      
      # :tableName => Name of the table to operate on,
      # :dbName => Name of the database containing the table from :tableName,
      # :connectionName => Name of the connection in the Pool
      def initialize(args={})
        req(:required => [:tableName, :dbName, :connectionName],
            :argsObject => args)
        @tableName = args[:tableName]
        @dbName = args[:dbName]
        @connectionName = args[:connectionName]
        @dbh = Mysqladmin::Exec.new(:connectionName => @connectionName)
        return true
      end
      
      # :lockType => Type of lock to acquire on the table
      def lock(args={})
        validLockTypes = ["READ", "READ LOCAL", "WRITE"]
        if args.has_key?(:lockType)
          unless validLockTypes.include?(args[:lockType].upcase)
            return false
          end
        end
        lockType = args[:lockType].upcase || "READ"
        @dbh.use(:dbName => @dbName)
        @dbh.go(:sql => "LOCK TABLE '#{@tableName}' #{lockType}")
        return true
      end
      
      #No arguments required because unlock doesn't take any.  Just issue on your
      #object to release all held locks.
      def unlock
        @dbh.go(:sql => "UNLOCK TABLES")
        return true
      end
      
      # :repairType => Which type of repair to do on the table
      def repair(args={})
        validRepairTypes = ["EXTENDED", "QUICK"]
        if args.has_key?(:repairType)
          unless validRepairTypes.include?(args[:repairType].upcase)
            return false
          end
        end
        repairType = args[:repairType].upcase || "QUICK"
        @dbh.use(:dbName => @dbName)
        @dbh.go(:sql => "REPAIR TABLE '#{@tableName}' #{repairType}")
        return true
      end
      
      # :checkType => The type of check to run on the table
      def check(args={})
        validCheckTypes = ["FOR UPGRADE", "QUICK", "FAST", "MEDIUM", "EXTENDED", "CHANGED"]
        if args.has_key?(:checkType)
          unless validCheckTypes.include?(args[:checkType].upcase)
            return false
          end
        end
        checkType = args[:checkType].upcase
        @dbh.go(:sql => "CHECK TABLE '#{@tableName}' #{checkType}")
        return true
      end
    end
    
    class Database
      attr_accessor :repairs, :locks
      include Mysqladmin::Arguments
      
      # :dbName => The name of the Database to operate on ,
      # :connectionName => The host this database is on
      def initialize(args={})
        req(:required => [:dbName, :connectionName],
            :argsObject => args)
        @locks = []
        @repairs = {}
        @checks = {}
        @dbName = args[:dbName]
        @connectionName = args[:connectionName]
        @dbh = Mysqladmin::Exec.new(:connectionName => @connectionName)
        return true
      end
      
      # :lockType => Type of lock to acquire on the tables of this database
      def lock(args={})
        validLockTypes = ["READ", "READ LOCAL", "WRITE"]
        if args.has_key?(:lockType)
          unless validLockTypes.include?(args[:lockType].upcase)
            return false
          end
        end
        lockType = args[:lockType] || "READ"
        @dbh.use(:dbName => @dbName)
        @dbh.listTables.each do |table|
          locks << Mysqladmin::Operations::Table.new(:tableName => table, :dbName => @dbName, :lockType => lockType)
        end
        return true
      end
      
      #No arguments required because unlock doesn't take any.  Just issue on your
      #object to release all held locks.
      def unlock
        @locks.each do |lock|
          lock.unlock
        end
        return true
      end
      
      # :repairType => Which type of repair to do on the tables of this database
      def repair(args={})
        validRepairTypes = ["EXTENDED", "QUICK"]
        if args.has_key?(:repairType)
          unless validRepairTypes.include?(args[:repairType].upcase)
            return false
          end
        end
        repairType = args[:repairType].upcase
        @dbh.use(:dbName => @dbName)
        @dbh.listTables.each do |table|
          @repairs[table] = Mysqladmin::Operations::Table.new(:tableName => table, :dbName => @dbName, :repairType => repairType)
        end
        return true
      end
      
      # :checkType => The type of check to run on the tables of this database
      def check(args={})
        validCheckTypes = ["FOR UPGRADE", "QUICK", "FAST", "MEDIUM", "EXTENDED", "CHANGED"]
        if args.has_key?(:checkType)
          unless validCheckTypes.include?(args[:checkType].upcase)
            return false
          end
        end
        checkType = args[:checkType].upcase
        @dbh.use(:dbName => @dbName)
        @dbh.listTables.each do |table|
          
        end
      end
      
    end
  end
end