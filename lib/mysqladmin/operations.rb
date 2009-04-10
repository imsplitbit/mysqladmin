module Mysqladmin
  class Operations
    class Table
      include Mysqladmin::Arguments
      
      # :table_name => Name of the table to operate on,
      # :db_name => Name of the database containing the table from :table_name,
      # :connection_name => Name of the connection in the Pool
      def initialize(args={})
        req(:required => [:table_name, :db_name, :connection_name],
            :args_object => args)
        @table_name = args[:table_name]
        @db_name = args[:db_name]
        @connection_name = args[:connection_name]
        @dbh = Mysqladmin::Exec.new(:connection_name => @connection_name)
        return true
      end
      
      # :lock_type => Type of lock to acquire on the table
      def lock(args={})
        valid_lock_types = ["READ", "READ LOCAL", "WRITE"]
        if args.has_key?(:lock_type)
          unless valid_lock_types.include?(args[:lock_type].upcase)
            return false
          end
        end
        lock_type = args[:lock_type].upcase || "READ"
        @dbh.use(:db_name => @db_name)
        @dbh.query(:sql => "LOCK TABLE '#{@table_name}' #{lock_type}")
        return true
      end
      
      #No arguments required because unlock doesn't take any.  Just issue on your
      #object to release all held locks.
      def unlock
        @dbh.query(:sql => "UNLOCK TABLES")
        return true
      end
      
      # :repair_type => Which type of repair to do on the table
      def repair(args={})
        valid_repair_types = ["EXTENDED", "QUICK"]
        if args.has_key?(:repair_type)
          unless valid_repair_types.include?(args[:repair_type].upcase)
            return false
          end
        end
        repair_type = args[:repair_type].upcase || "QUICK"
        @dbh.use(:db_name => @db_name)
        @dbh.query(:sql => "REPAIR TABLE '#{@table_name}' #{repair_type}")
        return true
      end
      
      # :check_type => The type of check to run on the table
      def check(args={})
        valid_check_types = ["FOR UPGRADE", "QUICK", "FAST", "MEDIUM", "EXTENDED", "CHANGED"]
        if args.has_key?(:check_type)
          unless valid_check_types.include?(args[:check_type].upcase)
            return false
          end
        end
        check_type = args[:check_type].upcase
        @dbh.query(:sql => "CHECK TABLE '#{@table_name}' #{check_type}")
        return true
      end
    end
    
    class Database
      attr_accessor :repairs, :locks
      include Mysqladmin::Arguments
      
      # :db_name => The name of the Database to operate on ,
      # :connection_name => The host this database is on
      def initialize(args={})
        req(:required => [:db_name, :connection_name],
            :args_object => args)
        @locks = []
        @repairs = {}
        @checks = {}
        @db_name = args[:db_name]
        @connection_name = args[:connection_name]
        @dbh = Mysqladmin::Exec.new(:connection_name => @connection_name)
        return true
      end
      
      # :lock_type => Type of lock to acquire on the tables of this database
      def lock(args={})
        valid_lock_types = ["READ", "READ LOCAL", "WRITE"]
        if args.has_key?(:lock_type)
          unless valid_lock_types.include?(args[:lock_type].upcase)
            return false
          end
        end
        lock_type = args[:lock_type] || "READ"
        @dbh.use(:db_name => @db_name)
        @dbh.list_tables.each do |table|
          locks << Mysqladmin::Operations::Table.new(:table_name => table, :db_name => @db_name, :lock_type => lock_type)
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
      
      # :repair_type => Which type of repair to do on the tables of this database
      def repair(args={})
        valid_repair_types = ["EXTENDED", "QUICK"]
        if args.has_key?(:repair_type)
          unless valid_repair_types.include?(args[:repair_type].upcase)
            return false
          end
        end
        repair_type = args[:repair_type].upcase
        @dbh.use(:db_name => @db_name)
        @dbh.list_tables.each do |table|
          @repairs[table] = Mysqladmin::Operations::Table.new(:table_name => table, :db_name => @db_name, :repair_type => repair_type)
        end
        return true
      end
      
      # :check_type => The type of check to run on the tables of this database
      def check(args={})
        valid_check_types = ["FOR UPGRADE", "QUICK", "FAST", "MEDIUM", "EXTENDED", "CHANGED"]
        if args.has_key?(:check_type)
          unless valid_check_types.include?(args[:check_type].upcase)
            return false
          end
        end
        check_type = args[:check_type].upcase
        @dbh.use(:db_name => @db_name)
        @dbh.list_tables.each do |table|
          
        end
      end
      
    end
  end
end