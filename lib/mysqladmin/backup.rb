require "mysqladmin/arguments"
require "mysqladmin/serialize"
require "mysqladmin/system"
require "mysqladmin/serverinfo"

module Mysqladmin
  class Backup
    include Mysqladmin::Arguments
    include Mysqladmin::Serialize
    include Mysqladmin::System
    include Mysqladmin::ServerInfo
    
    attr_accessor :text_filter
    attr_reader :task_results
    
    # :src_host => Host you wish to perform backups on,
    # :src_db => Name of the database you wish to backup,
    # :src_pool => Name of the pool on which you wish to operate,
    # :dest_host => Server on which to apply mysqldump files,
    # :text_filter => Usually would be a sed command to change data as it is streamed to gzip,
    # :per_table => Use if you want your backups to be done on a per table basis,
    # :only_these_tables => Use if you want to backup tables that match the names in this list,
    # :time_stamp => true/false do you want a timestap to precede the db_name/table_name in your
    #               backup files,
    # :debug => Set to true to ouput all actions to stdout
    def initialize(args = {})
      @src_host = args[:src_host] || nil
      @src_db = args[:src_db] || nil
      @src_pool = args[:src_pool] || nil
      @dest_host = args[:dest_host] || nil
      @per_table = args[:per_table] || nil
      @only_these_tables = args[:only_these_tables] || nil
      @extended_insert = args[:extended_insert] || nil
      @time_stamp = args[:time_stamp] || nil
      @text_filter = args[:text_filter] || nil
      @debug = args[:debug] || false
      @task_results = {}
    end
    
    # We need a way of finding backups, just in case we have 1500 of them listed
    # in our @task_results hash and we bumbleheaded the location or just plain
    # can't remember.
    def find(args)
      raise RuntimeError, "Method not implemented"
    end
    
    # :src_db => "db_name to backup",
    # :src_host => Server to connect to in order to get the backup of :src_db,
    # :per_table => true to get a per_table table backup,
    # :only_these_tables => An array of table_names you wish to limit your backups to,
    # :text_filter => Ususally would be a "sed" command to change data as it is streamed to gzip,
    # :extended_insert => Set to true to enable multi-row inserts.  This will make backups faster but make mid-restore errors harder to trace and fix.,
    # :time_stamp => Prepend the backup with a time_stamp in the format of "YYYYMMDD-HHMM"
    def backup_db(args = {})
      # Set all values in args if there are instance values set, args keys have
      # precidence.
      args[:per_table] = @per_table unless args.has_key?(:per_table)
      args[:only_these_tables] = @only_these_tables unless args.has_key?(:only_these_tables)
      args[:extended_insert] = @extended_insert unless args.has_key?(:extended_insert)
      args[:time_stamp] = @time_stamp unless args.has_key?(:time_stamp)
      args[:src_db] = @src_db unless args.has_key?(:src_db)
      args[:src_host] = @src_host unless args.has_key?(:src_host)
      args[:text_filter] = @text_filter unless args.has_key?(:text_filter)
      args[:task] = :backup unless args.has_key?(:backup)
      
      # Mandatory args:
      req(:required => [:per_table,
                        :src_db,
                        :src_host,
                        :task],
          :args_object => args)

      # Store backup results in this returned hash so logging can be done if you
      # care to do that.
      @task_results[args[:src_host]] = {} unless @task_results.has_key?(args[:src_host])
      @task_results[args[:src_host]][args[:src_db]] = {} unless @task_results[args[:src_host]].has_key?(args[:src_db])
      @task_results[args[:src_host]][args[:src_db]][:backup_files] = [] unless @task_results[args[:src_host]][args[:src_db]].has_key?(:backup_files)
      @task_results[args[:src_host]][args[:src_db]][:backup_result_log] = {} unless @task_results[args[:src_host]][args[:src_db]].has_key?(:backup_result_log)
      
      # Set the backup type, this is used later in the restore process to make sure
      # we don't try to iterate through a string.  This can be done about a
      # hundred ways, I know, but I like to err on the side of verbosity.
      @task_results[args[:src_host]][args[:src_db]][:type] = case args[:per_table]
      when nil || false then :full
      when true then :per_table
      end
      
      # Get server version so as to be able to do server specific commands
      major_version, minor_version, patch_versions = server_version(:connection_name => args[:src_host])
      
      # Username and Ip are stored for persistence in the pool hash
      args[:user] = Mysqladmin::Pool.connections[args[:src_host]][:user]
      args[:src_ip] = Mysqladmin::Pool.connections[args[:src_host]][:host]
      
      # If a password is used, append that to the mysqldump command in proper
      # format for mysqldump.  i.e. -pmysouperseekrit
      if Mysqladmin::Pool.connections[args[:src_host]][:password].length > 0
        args[:password] = "-p#{Mysqladmin::Pool.connections[args[:src_host]][:password]}"
      end
      
      # Specific to mysql versions >= 5, dump stored procedures and triggers
      if major_version >= 5
        args[:procs_and_triggers] = "--routines --triggers"
      else
        args[:procs_and_triggers] = ""
      end
      
      # If a text filter is provided mangle it into a pipe
      if args[:text_filter]
        args[:text_filter] = "| #{args[:text_filter]}"
      end
      
      # Enable/Disable extended insert.  My preference is to disable extended
      # inserts as more times than not, when restoring a multi-gig database
      # a failure will occur and you have no recourse other than to drop and try
      # again and again and again
      unless args[:extended_insert]
        args[:extended_insert] = "--skip-extended-insert"
      else
        args[:extended_insert] = ""
      end
      
      if args[:per_table]
        # If we are doing per table backups, i.e. the smart way to do backups IMHO,
        # we need to get a list of tables for our database.
        dbh = Mysqladmin::Exec.new(:connection_name => args[:src_host])
        dbh.use(args[:src_db])
        dbh.list_tables.each do |table_name|
          if args[:only_these_tables].class == Array
            if args[:only_these_tables].include?(table_name)
              args[:table_name] = table_name
              do_backup(args)
              args.delete(:table_name) if args.has_key?(:table_name)
              args.delete(:status) if args.has_key?(:status)
            end
          else
            args[:table_name] = table_name
            do_backup(args)
            args.delete(:table_name) if args.has_key?(:table_name)
            args.delete(:status) if args.has_key?(:status)
          end
        end
      else
        args.delete(:table_name) if args.has_key?(:table_name)
        do_backup(args)
        args.delete(:status) if args.has_key?(:status)
      end
    end
    
    # :src_host => Host to backup,
    # :per_table => true to get a per_table table backup,
    # :only_these_tables => An array of table_names you wish to limit your backups to,
    # :text_filter => Ususally would be a "sed" command to change data as it is streamed to gzip,
    # :extended_insert => Set to true to enable multi-row inserts.  This will make backups faster but make mid-restore errors harder to trace and fix.,
    # :time_stamp => Prepend the backup with a time_stamp in the format of "YYYYMMDD-HHMM"
    def backup_host(args)
      req(:required => [:src_host],
          :args_object => args)
      args[:per_table] = @per_table unless args.has_key?(:per_table)
      args[:only_these_tables] = @only_these_tables unless args.has_key?(:only_these_tables)
      args[:text_filter] = @text_filter unless args.has_key?(:text_filter)
      args[:extended_insert] = @extended_insert unless args.has_key?(:extended_insert)
      args[:time_stamp] = @time_stamp unless args.has_key?(:time_stamp)
      
      # first thing we need to do is get a list of databases on :src_host
      dbh = Mysqladmin::Exec.new(:connection_name => args[:src_host])
      databases = dbh.list_dbs
      
      # As long as the databases array is not empty, backup all databases
      unless databases.empty?
        
        # Remove lost+found directory if it shows up in the db list
        # this will break backups
        databases.delete_if{|x| ["lost\+found", "information_schema"].include?(x) }.each do |db_name|
          
          # set the database name to the db we want to backup
          args[:src_db] = db_name
          backup_db(args)
          
          # flush the database name out of our args hash so we don't have the
          # chance of collisions or dupes
          args.delete(:src_db)
        end
      end
    end
    
    # :src_pool => Pool to backup,
    # :thread_pool_size => Number of threads to spawn for the pool operations, default is 40.
    # :per_table => true to get a per_table table backup,
    # :only_these_tables => An array of table_names you wish to limit your backups to,
    # :text_filter => Ususally would be a "sed" command to change data as it is streamed to gzip,
    # :extended_insert => Set to true to enable multi-row inserts.  This will make backups faster but make mid-restore errors harder to trace and fix.,
    # :time_stamp => Prepend the backup with a time_stamp in the format of "YYYYMMDD-HHMM"
    def backup_pool(args)
      req(:required => [:src_pool],
          :args_object => args)
      args[:thread_pool_size] = 40 unless args.has_key?(:thread_pool_size)
      args[:per_table] = @per_table unless args.has_key?(:per_table)
      args[:only_these_tables] = @only_these_tables unless args.has_key?(:only_these_tables)
      args[:text_filter] = @text_filter unless args.has_key?(:text_filter)
      args[:extended_insert] = @extended_insert unless args.has_key?(:extended_insert)
      args[:time_stamp] = @time_stamp unless args.has_key?(:time_stamp)
      
      # Create our threadpool
      pool = ThreadPool.new(args[:thread_pool_size])
      
      # iterate through the list of connections in the Array of connection names
      Mysqladmin::Pool.connection_pools[args[:src_pool]].each do |src_host|
        
        # Set :src_host in args and pass to backup_host
        args[:src_host] = src_host
        pool.process { backup_host(args) }
        
        # Remove :src_host to avoid collisions and backing up the same host twice
        args.delete(:src_host)
      end
      
      # Wait for all jobs in the pool to finish
      pool.join
    end
    
    # :src_db => Database name that is being restored,
    # :src_host => Host the backups were run on,
    # :dest_host => Name of the host we want to restore to,
    # :dest_db => Name of the database you want to restore to,
    # :text_filter => Any bash run-able command that will filter text.
    #                i.e. sed -e s/foo/bar/g,
    # :backup_files => files to restore from, if you aren't working from a
    #                 serialzed object,
    # :backup_file_format => Assumed gzip compressed, we will check to be sure
    #                      regardless, if file ends in .sql it is assumed
    #                      clear text.  If the file ends in .gz or .tgz it is
    #                      assumed compressed text.  We will add more options
    #                      when gzip sucks worse than other things. Perhaps bzip?
    #                      mixed format, i.e. .sql and .gz is not permitted
    #                      just now.  Will allow that later.
    # :crash_if_exists => Set to true if you want to raise an exception if
    #                   targets already exist, Defaults to false.
    # :overwrite_if_exists => Set to true if you want to overwrite the :dest_db
    #                       with all new data, we won't be dropping the db to
    #                       create it fresh but any tables that are there will
    #                       be overwritten by the restore.
    def restore_db_from_backup(args = {})
      # We need to get our list of files to restore from based on the database name
      # and the host it was backed up from.
      args[:src_db] = @src_db unless args.has_key?(:src_db)
      args[:src_host] = @src_host unless args.has_key?(:src_host)
      args[:backup_files] = @task_results[args[:src_host]][args[:src_db]][:backup_files] unless args.has_key?(:backup_files)
      args[:per_table] = @task_results[args[:src_host]][args[:src_db]][:type] == :per_table ? true : false
      
      # Set args[:dest_db] to args[:src_db] if we haven't received a name for it
      args[:dest_db] = args[:src_db] unless args.has_key?(:dest_db)
      
      # Return to normal operations
      args.has_key?(:text_filter) ? @text_filter = args[:text_filter] : args[:text_filter] = @text_filter
      args.has_key?(:dest_host) ? @dest_host = args[:dest_host] : args[:dest_host] = @dest_host
      args[:crash_if_exists] = false unless args.has_key?(:crash_if_exists)
      args[:overwrite_if_exists] = false unless args.has_key?(:overwrite_if_exists)
      args[:user] = Mysqladmin::Pool.connections[args[:dest_host]][:user] unless args.has_key?(:user)
      args[:password] = Mysqladmin::Pool.connections[args[:dest_host]][:password] unless args.has_key?(:password)
      args[:dest_ip] = Mysqladmin::Pool.connections[args[:dest_host]][:host] unless args.has_key?(:dest_ip)
      
      # Mandatory options:
      req(:required => [:src_db,
                        :src_host,
                        :backup_files,
                        :per_table,
                        :dest_db,
                        :dest_host,
                        :user,
                        :password,
                        :dest_ip],
          :args_object => args)

      # Delete args[:src_db] and args[:src_host] so we don't have collisions later.
      args.delete(:src_db) if args.has_key?(:src_db)
      args.delete(:src_host) if args.has_key?(:src_host)
      
      # Create our logging space if it doesn't already exist
      @task_results[args[:dest_host]] = {} unless @task_results.has_key?(args[:dest_host])
      @task_results[args[:dest_host]][args[:dest_db]] = {} unless @task_results[args[:dest_host]].has_key?(args[:dest_db])
      @task_results[args[:dest_host]][args[:dest_db]][:restore_files] = [] unless @task_results[args[:dest_host]][args[:dest_db]].has_key?(:restore_files)
      @task_results[args[:dest_host]][args[:dest_db]][:restore_result_log] = {} unless @task_results[args[:dest_host]][args[:dest_db]].has_key?(:restore_result_log)
      
      # If a text filter is provided mangle it into a pipe
      if args[:text_filter]
        args[:text_filter] = "| #{args[:text_filter]}"
      end
      
      args[:backup_files].each do |buFile|
        # Make sure the backup files exist in the path or crash
        unless File.file?(buFile)
          raise RuntimeError, "Backup file #{buFile} is not in path or cwd"
        end
        # analyze the files and make sure we can figure out how to concatenate
        # them to the mysql command for restoration
        if buFile[/^.*\.sql$/]
          args[:backup_file_format] = :text
        elsif buFile[/^.*\.[t]*gz$/]
          args[:backup_file_format] = :gzip
        else
          raise RuntimeError, "No supported backup_file_format matched"
        end
      end
      
      # Make sure the backup files exist in the path or crash
      
      # See if the target database exists and follow conditions for :crash_if_exists
      # and :overwrite_if_exists
      dbh = Mysqladmin::Exec.new(:connection_name => args[:dest_host],
                                 :sql => "SHOW DATABASES LIKE '#{args[:dest_db]}'")
      dbh.go
      if dbh.rows > 0
        if dbh.fetch_hash["Database (#{args[:dest_db]})"] == args[:dest_db]
          if args[:crash_if_exists] == true
            raise RuntimeError, "Database #{args[:dest_db]} exists on #{args[:dest_host]}"
          elsif args[:overwrite_if_exists] == true
            doRestore(args)
          else
            false
          end
        end
      else
        dbh.createDb(args[:dest_db])
        doRestore(args)
      end
      
      # Flush 
      args.delete(:crash_if_exists) if args.has_key?(:crash_if_exists)
      args.delete(:overwrite_if_exists) if args.has_key?(:overwrite_if_exists)
    end
    
    # :src_host => Host to check backup success,
    # :src_db => Db to check backups.
    # :task => :backup/:restore
    def success?(args = {})
      args[:src_host] = @src_host unless args.has_key?(:src_host)
      args[:src_db] = @src_db unless args.has_key?(:src_db)
      
      req(:required => [:src_host, :src_db, :task],
          :args_object => args)
          
      if @task_results[args[:src_host]][args[:src_db]]["#{args[:task].to_s}result_log".to_sym].class == Hash
        @task_results[args[:src_host]][args[:src_db]]["#{args[:task].to_s}result_log".to_sym].each do |table_name, backupResult|
          if backupResult == false
            return false
          end
        end
      else
        return @task_results[args[:src_host]][args[:src_db]]["#{args[:task].to_s}result_log".to_sym]
      end
    end
    
    private
    
    # :src_host => Name of the host we are operating on right now, used purely for
    #             storing the resulting data.  This is because we don't want to
    #             implement a pure ruby mysqldump as mysqldump is going to do the
    #             best job backing up our dbs.  Yay MySQL Programmers!  You are
    #             *WAY* smarter than I am.,
    # :src_db => The name of the database which is being backed up from on :src_host,
    # :per_table => If true then the :result_log will contain a list of tables and
    #              the results of their backups,
    # :table_name => If :per_table is true then we need this to populate the
    #               :result_log hash.
    def check_exit_code(args)
      if $?.exitstatus == 0
        args[:status] = true
      else
        args[:status] = false
      end
      args
    end
    
    # :task => :backup/:restore
    # *if :task => :backup*
    # :src_db => Name of the database we are backing up,
    # :src_host => Host from which we are taking the backup,
    # :status => true/false, The status of the last run commandline command,
    # :per_table => true/false, Were the backups run on a per_table basis.
    #              *requires :table_name*,
    # :table_name => Name of the table being operated on. *requires :per_table*,
    # *elsif :task => :restore*
    # :status => true/false, The status of the last run commandline command,
    # :per_table => true/false, Were the backups run on a per_table basis.
    #              *requires :table_name*,
    # :dest_db => Name of the database we are restoring to,
    # :dest_host => Host we are restoring :dest_db to,
    # :backup_file => File we are attempting to restore from.  *requires :task => :restore*,
    def updatetask_results(args)
      if args[:task] == :backup
        req(:required => [:src_db,
                          :src_host,
                          :status],
            :args_object => args)
        if args[:per_table]
          @task_results[args[:src_host]][args[:src_db]][:backup_result_log][args[:table_name]] = args[:status]
        else
          @task_results[args[:src_host]][args[:src_db]][:backup_result_log] = args[:status]
        end
      elsif args[:task] == :restore
        req(:required => [:dest_db,
                          :dest_host,
                          :status,
                          :backup_file],
            :args_object => args)
        if args[:per_table]
          @task_results[args[:dest_host]][args[:dest_db]][:restore_result_log][args[:backup_file]] = args[:status]
        else
          @task_results[args[:dest_host]][args[:dest_db]][:restore_result_log] = args[:status]
        end
      else
        raise RuntimeError, "args[:task] was not passed to the task"
      end
    end
    
    # :src_db => Db to backup from,
    # :src_host => Name of the connection in the connection manager i.e. Mysqladmin::Pool,
    # :src_ip => IP Address for :src_host,
    # :user => User to connect to :src_ip as,
    # :password => Password to authenticate with for :user on :src_ip,
    # :procs_and_triggers => String to append to mysqldump to backup stored procedures and triggers for :src_db,
    # :extended_insert => String to append to mysqldump to allow for or eliminate multi-row inserts,
    # :table_name => Table to backup from :src_db, required if this is a per_table
    #               based backup.
    # :time_stamp => If not nil or false prepend a time_stamp to the backup file_name,
    # :text_filter => commandline command to filter/replace text
    def do_backup(args)
      req(:required => [:src_db,
                        :extended_insert,
                        :procs_and_triggers,
                        :user,
                        :src_ip],
          :args_object => args)
      args[:task] = :backup
      if args[:table_name]
        file_name = "#{args[:src_db]}-#{args[:table_name]}.sql.gz"
        backupSrc = "'#{args[:src_db]}' '#{args[:table_name]}'"
      else
        file_name = "#{args[:src_db]}.sql.gz"
        backupSrc = "'#{args[:src_db]}'"
      end
      if args[:time_stamp]
        file_name = "#{Time.now.strftime("%Y%m%d-%H%M")}-#{file_name}"
      end
      `#{core_reqs(:binary => "mysqldump")} --opt -Q #{args[:extended_insert]} #{args[:procs_and_triggers]} -u #{args[:user]} #{args[:password]} -h #{args[:src_ip]} #{backupSrc} #{args[:text_filter]} | gzip > #{file_name}`
      updatetask_results(check_exit_code(args))
      @task_results[args[:src_host]][args[:src_db]][:backup_files] << file_name
    end
    
    # :dest_db => Name of the database we are restoring from the backup, not the
    #           same as :dest_db but can be,
    # :dest_host => Name of the host we are restoring to,
    # :backup_files => Array object containing file_names with paths if outside of
    #                 cwd,
    # :backup_file_format => The format of the files being passed in.  This is set
    #                      in the restoreFromBackup method but can be overwritten
    #                      if you think you know better. USE CAUTION!,
    # :text_filter => commandline command to filter text
    def doRestore(args)
      req(:required => [:backup_file_format,
                        :backup_files,
                        :user,
                        :password,
                        :dest_ip,
                        :dest_db],
          :args_object => args)
      args[:task] = :restore
      # Determine what tool we need to change the file to text and concatenate it
      # for a pipe to :text_filter and mysql
      cat = case args[:backup_file_format]
      when :text then core_reqs(:binary => "cat")
      when :gzip then core_reqs(:binary => "gunzip", :cmd_args => "-c")
      end
      
      
      # Do the actual restore, this method will be in flux, we need to use the cli
      # but need to do a better job of catching system errors, right now that needs
      # to be in check_exit_code.
      args[:backup_files].each do |backup_file|
        args[:backup_file] = backup_file
        `#{cat} #{backup_file} #{args[:text_filter]} | #{core_reqs(:binary => "mysql")} -u #{args[:user]} -p#{args[:password]} -h #{args[:dest_ip]} #{args[:dest_db]}`
        updatetask_results(check_exit_code(args))
        @task_results[args[:dest_host]][args[:dest_db]][:restore_files] << backup_file
      end
    end
  end
end
