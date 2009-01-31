module Mysqladmin
  class Operations
    class Table
      include Mysqladmin::Arguments
      
      def initialize(args={})
        req(:required => [:tableName, :dbName, :connectionName],
            :argsObject => args)
        @tableName = args[:tableName]
        @dbName = args[:dbName]
        @connectionName = args[:connectionName]
        @dbh = Mysqladmin::Exec.new(:connectionName => @connectionName)
        return true
      end
      
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
      
      def unlock
        @dbh.go(:sql => "UNLOCK TABLES")
        return true
      end
      
      def repair(args={})
        validRepairTypes = ["EXTENDED", "QUICK"]
        if args.has_key?(:repairType)
          unless validRepairTypes.include?(args[:repairType].upcase)
            return false
          end
        end
        @dbh.go(:sql => "REPAIR TABLE '#{@tableName}' #{repairType}")
        return true
      end
      
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
      
      def unlock
        @locks.each do |lock|
          lock.unlock
        end
        return true
      end
      
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