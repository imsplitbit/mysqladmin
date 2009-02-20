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
    
    attr_accessor :textFilter
    attr_reader :taskResults
    
    # :srcHost => Host you wish to perform backups on,
    # :srcDb => Name of the database you wish to backup,
    # :srcPool => Name of the pool on which you wish to operate,
    # :destHost => Server on which to apply mysqldump files,
    # :textFilter => Usually would be a sed command to change data as it is streamed to gzip,
    # :perTable => Use if you want your backups to be done on a per table basis,
    # :onlyTheseTables => Use if you want to backup tables that match the names in this list,
    # :timeStamp => true/false do you want a timestap to precede the dbname/tablename in your
    #               backup files,
    # :debug => Set to true to ouput all actions to stdout
    def initialize(args = {})
      @srcHost = args[:srcHost] || nil
      @srcDb = args[:srcDb] || nil
      @srcPool = args[:srcPool] || nil
      @destHost = args[:destHost] || nil
      @perTable = args[:perTable] || nil
      @onlyTheseTables = args[:onlyTheseTables] || nil
      @extendedInsert = args[:extendedInsert] || nil
      @timeStamp = args[:timeStamp] || nil
      @textFilter = args[:textFilter] || nil
      @debug = args[:debug] || false
      @taskResults = {}
    end
    
    # We need a way of finding backups, just in case we have 1500 of them listed
    # in our @taskResults hash and we bumbleheaded the location or just plain
    # can't remember.
    def find(args)
      raise RuntimeError, "Method not implemented"
    end
    
    # :srcDb => "Dbname to backup",
    # :srcHost => Server to connect to in order to get the backup of :srcDb,
    # :perTable => true to get a perTable table backup,
    # :onlyTheseTables => An array of tablenames you wish to limit your backups to,
    # :textFilter => Ususally would be a "sed" command to change data as it is streamed to gzip,
    # :extendedInsert => Set to true to enable multi-row inserts.  This will make backups faster but make mid-restore errors harder to trace and fix.,
    # :timeStamp => Prepend the backup with a timestamp in the format of "YYYYMMDD-HHMM"
    def backupDb(args = {})
      # Set all values in args if there are instance values set, args keys have
      # precidence.
      args[:perTable] = @perTable unless args.has_key?(:perTable)
      args[:onlyTheseTables] = @onlyTheseTables unless args.has_key?(:onlyTheseTables)
      args[:extendedInsert] = @extendedInsert unless args.has_key?(:extendedInsert)
      args[:timeStamp] = @timeStamp unless args.has_key?(:timeStamp)
      args[:srcDb] = @srcDb unless args.has_key?(:srcDb)
      args[:srcHost] = @srcHost unless args.has_key?(:srcHost)
      args[:textFilter] = @textFilter unless args.has_key?(:textFilter)
      args[:task] = :backup unless args.has_key?(:backup)
      
      # Mandatory args:
      req(:required => [:perTable,
                        :srcDb,
                        :srcHost,
                        :task],
          :argsObject => args)

      # Store backup results in this returned hash so logging can be done if you
      # care to do that.
      @taskResults[args[:srcHost]] = {} unless @taskResults.has_key?(args[:srcHost])
      @taskResults[args[:srcHost]][args[:srcDb]] = {} unless @taskResults[args[:srcHost]].has_key?(args[:srcDb])
      @taskResults[args[:srcHost]][args[:srcDb]][:backupFiles] = [] unless @taskResults[args[:srcHost]][args[:srcDb]].has_key?(:backupFiles)
      @taskResults[args[:srcHost]][args[:srcDb]][:backupResultLog] = {} unless @taskResults[args[:srcHost]][args[:srcDb]].has_key?(:backupResultLog)
      
      # Set the backup type, this is used later in the restore process to make sure
      # we don't try to iterate through a string.  This can be done about a
      # hundred ways, I know, but I like to err on the side of verbosity.
      @taskResults[args[:srcHost]][args[:srcDb]][:type] = case args[:perTable]
      when nil || false then :full
      when true then :perTable
      end
      
      # Get server version so as to be able to do server specific commands
      majorVers, minorVers, patchVers = serverVersion(:connectionName => args[:srcHost])
      
      # Username and Ip are stored for persistence in the pool hash
      args[:user] = Mysqladmin::Pool.connections[args[:srcHost]][:user]
      args[:srcIp] = Mysqladmin::Pool.connections[args[:srcHost]][:host]
      
      # If a password is used, append that to the mysqldump command in proper
      # format for mysqldump.  i.e. -pmysouperseekrit
      if Mysqladmin::Pool.connections[args[:srcHost]][:password].length > 0
        args[:password] = "-p#{Mysqladmin::Pool.connections[args[:srcHost]][:password]}"
      end
      
      # Specific to mysql versions >= 5, dump stored procedures and triggers
      if majorVers >= 5
        args[:procsAndTriggers] = "--routines --triggers"
      else
        args[:procsAndTriggers] = ""
      end
      
      # If a text filter is provided mangle it into a pipe
      if args[:textFilter]
        args[:textFilter] = "| #{args[:textFilter]}"
      end
      
      # Enable/Disable extended insert.  My preference is to disable extended
      # inserts as more times than not, when restoring a multi-gig database
      # a failure will occur and you have no recourse other than to drop and try
      # again and again and again
      unless args[:extendedInsert]
        args[:extendedInsert] = "--skip-extended-insert"
      else
        args[:extendedInsert] = ""
      end
      
      if args[:perTable]
        # If we are doing per table backups, i.e. the smart way to do backups IMHO,
        # we need to get a list of tables for our database.
        dbh = Mysqladmin::Exec.new(:connectionName => args[:srcHost])
        dbh.use(args[:srcDb])
        dbh.listTables.each do |tableName|
          if args[:onlyTheseTables].class == Array
            if args[:onlyTheseTables].include?(tableName)
              args[:tableName] = tableName
              doBackup(args)
              args.delete(:tableName) if args.has_key?(:tableName)
              args.delete(:status) if args.has_key?(:status)
            end
          else
            args[:tableName] = tableName
            doBackup(args)
            args.delete(:tableName) if args.has_key?(:tableName)
            args.delete(:status) if args.has_key?(:status)
          end
        end
      else
        args.delete(:tableName) if args.has_key?(:tableName)
        doBackup(args)
        args.delete(:status) if args.has_key?(:status)
      end
    end
    
    # :srcHost => Host to backup,
    # :perTable => true to get a perTable table backup,
    # :onlyTheseTables => An array of tablenames you wish to limit your backups to,
    # :textFilter => Ususally would be a "sed" command to change data as it is streamed to gzip,
    # :extendedInsert => Set to true to enable multi-row inserts.  This will make backups faster but make mid-restore errors harder to trace and fix.,
    # :timeStamp => Prepend the backup with a timestamp in the format of "YYYYMMDD-HHMM"
    def backupHost(args)
      req(:required => [:srcHost],
          :argsObject => args)
      args[:perTable] = @perTable unless args.has_key?(:perTable)
      args[:onlyTheseTables] = @onlyTheseTables unless args.has_key?(:onlyTheseTables)
      args[:textFilter] = @textFilter unless args.has_key?(:textFilter)
      args[:extendedInsert] = @extendedInsert unless args.has_key?(:extendedInsert)
      args[:timeStamp] = @timeStamp unless args.has_key?(:timeStamp)
      
      # first thing we need to do is get a list of databases on :srcHost
      dbh = Mysqladmin::Exec.new(:connectionName => args[:srcHost])
      databases = dbh.listDbs
      
      # As long as the databases array is not empty, backup all databases
      unless databases.empty?
        
        # Remove lost+found directory if it shows up in the db list
        # this will break backups
        databases.delete_if{|x| ["lost\+found", "information_schema"].include?(x) }.each do |dbName|
          
          # set the database name to the db we want to backup
          args[:srcDb] = dbName
          backupDb(args)
          
          # flush the database name out of our args hash so we don't have the
          # chance of collisions or dupes
          args.delete(:srcDb)
        end
      end
    end
    
    # :srcPool => Pool to backup,
    # :threadPoolSize => Number of threads to spawn for the pool operations, default is 40.
    # :perTable => true to get a perTable table backup,
    # :onlyTheseTables => An array of tablenames you wish to limit your backups to,
    # :textFilter => Ususally would be a "sed" command to change data as it is streamed to gzip,
    # :extendedInsert => Set to true to enable multi-row inserts.  This will make backups faster but make mid-restore errors harder to trace and fix.,
    # :timeStamp => Prepend the backup with a timestamp in the format of "YYYYMMDD-HHMM"
    def backupPool(args)
      req(:required => [:srcPool],
          :argsObject => args)
      args[:threadPoolSize] = 40 unless args.has_key?(:threadPoolSize)
      args[:perTable] = @perTable unless args.has_key?(:perTable)
      args[:onlyTheseTables] = @onlyTheseTables unless args.has_key?(:onlyTheseTables)
      args[:textFilter] = @textFilter unless args.has_key?(:textFilter)
      args[:extendedInsert] = @extendedInsert unless args.has_key?(:extendedInsert)
      args[:timeStamp] = @timeStamp unless args.has_key?(:timeStamp)
      
      # Create our threadpool
      pool = ThreadPool.new(args[:threadPoolSize])
      
      # iterate through the list of connections in the Array of connection names
      Mysqladmin::Pool.connectionPools[args[:srcPool]].each do |srcHost|
        
        # Set :srcHost in args and pass to backupHost
        args[:srcHost] = srcHost
        pool.process { backupHost(args) }
        
        # Remove :srcHost to avoid collisions and backing up the same host twice
        args.delete(:srcHost)
      end
      
      # Wait for all jobs in the pool to finish
      pool.join
    end
    
    # :srcDb => Database name that is being restored,
    # :srcHost => Host the backups were run on,
    # :destHost => Name of the host we want to restore to,
    # :destDb => Name of the database you want to restore to,
    # :textFilter => Any bash run-able command that will filter text.
    #                i.e. sed -e s/foo/bar/g,
    # :backupFiles => files to restore from, if you aren't working from a
    #                 serialzed object,
    # :backupFileFormat => Assumed gzip compressed, we will check to be sure
    #                      regardless, if file ends in .sql it is assumed
    #                      clear text.  If the file ends in .gz or .tgz it is
    #                      assumed compressed text.  We will add more options
    #                      when gzip sucks worse than other things. Perhaps bzip?
    #                      mixed format, i.e. .sql and .gz is not permitted
    #                      just now.  Will allow that later.
    # :crashIfExists => Set to true if you want to raise an exception if
    #                   targets already exist, Defaults to false.
    # :overwriteIfExists => Set to true if you want to overwrite the :destDb
    #                       with all new data, we won't be dropping the db to
    #                       create it fresh but any tables that are there will
    #                       be overwritten by the restore.
    def restoreDbFromBackup(args = {})
      # We need to get our list of files to restore from based on the database name
      # and the host it was backed up from.
      args[:srcDb] = @srcDb unless args.has_key?(:srcDb)
      args[:srcHost] = @srcHost unless args.has_key?(:srcHost)
      args[:backupFiles] = @taskResults[args[:srcHost]][args[:srcDb]][:backupFiles] unless args.has_key?(:backupFiles)
      args[:perTable] = @taskResults[args[:srcHost]][args[:srcDb]][:type] == :perTable ? true : false
      
      # Set args[:destDb] to args[:srcDb] if we haven't received a name for it
      args[:destDb] = args[:srcDb] unless args.has_key?(:destDb)
      
      # Return to normal operations
      args.has_key?(:textFilter) ? @textFilter = args[:textFilter] : args[:textFilter] = @textFilter
      args.has_key?(:destHost) ? @destHost = args[:destHost] : args[:destHost] = @destHost
      args[:crashIfExists] = false unless args.has_key?(:crashIfExists)
      args[:overwriteIfExists] = false unless args.has_key?(:overwriteIfExists)
      args[:user] = Mysqladmin::Pool.connections[args[:destHost]][:user] unless args.has_key?(:user)
      args[:password] = Mysqladmin::Pool.connections[args[:destHost]][:password] unless args.has_key?(:password)
      args[:destIp] = Mysqladmin::Pool.connections[args[:destHost]][:host] unless args.has_key?(:destIp)
      
      # Mandatory options:
      req(:required => [:srcDb,
                        :srcHost,
                        :backupFiles,
                        :perTable,
                        :destDb,
                        :destHost,
                        :user,
                        :password,
                        :destIp],
          :argsObject => args)

      # Delete args[:srcDb] and args[:srcHost] so we don't have collisions later.
      args.delete(:srcDb) if args.has_key?(:srcDb)
      args.delete(:srcHost) if args.has_key?(:srcHost)
      
      # Create our logging space if it doesn't already exist
      @taskResults[args[:destHost]] = {} unless @taskResults.has_key?(args[:destHost])
      @taskResults[args[:destHost]][args[:destDb]] = {} unless @taskResults[args[:destHost]].has_key?(args[:destDb])
      @taskResults[args[:destHost]][args[:destDb]][:restoreFiles] = [] unless @taskResults[args[:destHost]][args[:destDb]].has_key?(:restoreFiles)
      @taskResults[args[:destHost]][args[:destDb]][:restoreResultLog] = {} unless @taskResults[args[:destHost]][args[:destDb]].has_key?(:restoreResultLog)
      
      # If a text filter is provided mangle it into a pipe
      if args[:textFilter]
        args[:textFilter] = "| #{args[:textFilter]}"
      end
      
      args[:backupFiles].each do |buFile|
        # Make sure the backup files exist in the path or crash
        unless File.file?(buFile)
          raise RuntimeError, "Backup file #{buFile} is not in path or cwd"
        end
        # analyze the files and make sure we can figure out how to concatenate
        # them to the mysql command for restoration
        if buFile[/^.*\.sql$/]
          args[:backupFileFormat] = :text
        elsif buFile[/^.*\.[t]*gz$/]
          args[:backupFileFormat] = :gzip
        else
          raise RuntimeError, "No supported backupFileFormat matched"
        end
      end
      
      # Make sure the backup files exist in the path or crash
      
      # See if the target database exists and follow conditions for :crashIfExists
      # and :overwriteIfExists
      dbh = Mysqladmin::Exec.new(:connectionName => args[:destHost],
                                 :sql => "SHOW DATABASES LIKE '#{args[:destDb]}'")
      dbh.go
      if dbh.rows > 0
        if dbh.fetch_hash["Database (#{args[:destDb]})"] == args[:destDb]
          if args[:crashIfExists] == true
            raise RuntimeError, "Database #{args[:destDb]} exists on #{args[:destHost]}"
          elsif args[:overwriteIfExists] == true
            doRestore(args)
          else
            false
          end
        end
      else
        dbh.createDb(args[:destDb])
        doRestore(args)
      end
      
      # Flush 
      args.delete(:crashIfExists) if args.has_key?(:crashIfExists)
      args.delete(:overwriteIfExists) if args.has_key?(:overwriteIfExists)
    end
    
    # :srcHost => Host to check backup success,
    # :srcDb => Db to check backups.
    # :task => :backup/:restore
    def success?(args = {})
      args[:srcHost] = @srcHost unless args.has_key?(:srcHost)
      args[:srcDb] = @srcDb unless args.has_key?(:srcDb)
      
      req(:required => [:srcHost, :srcDb, :task],
          :argsObject => args)
          
      if @taskResults[args[:srcHost]][args[:srcDb]]["#{args[:task].to_s}ResultLog".to_sym].class == Hash
        @taskResults[args[:srcHost]][args[:srcDb]]["#{args[:task].to_s}ResultLog".to_sym].each do |tableName, backupResult|
          if backupResult == false
            return false
          end
        end
      else
        return @taskResults[args[:srcHost]][args[:srcDb]]["#{args[:task].to_s}ResultLog".to_sym]
      end
    end
    
    private
    
    # :srcHost => Name of the host we are operating on right now, used purely for
    #             storing the resulting data.  This is because we don't want to
    #             implement a pure ruby mysqldump as mysqldump is going to do the
    #             best job backing up our dbs.  Yay MySQL Programmers!  You are
    #             *WAY* smarter than I am.,
    # :srcDb => The name of the database which is being backed up from on :srcHost,
    # :perTable => If true then the :resultLog will contain a list of tables and
    #              the results of their backups,
    # :tableName => If :perTable is true then we need this to populate the
    #               :resultLog hash.
    def checkExitCode(args)
      if $?.exitstatus == 0
        args[:status] = true
      else
        args[:status] = false
      end
      args
    end
    
    # :task => :backup/:restore
    # *if :task => :backup*
    # :srcDb => Name of the database we are backing up,
    # :srcHost => Host from which we are taking the backup,
    # :status => true/false, The status of the last run commandline command,
    # :perTable => true/false, Were the backups run on a pertable basis.
    #              *requires :tableName*,
    # :tableName => Name of the table being operated on. *requires :perTable*,
    # *elsif :task => :restore*
    # :status => true/false, The status of the last run commandline command,
    # :perTable => true/false, Were the backups run on a pertable basis.
    #              *requires :tableName*,
    # :destDb => Name of the database we are restoring to,
    # :destHost => Host we are restoring :destDb to,
    # :backupFile => File we are attempting to restore from.  *requires :task => :restore*,
    def updateTaskResults(args)
      if args[:task] == :backup
        req(:required => [:srcDb,
                          :srcHost,
                          :status],
            :argsObject => args)
        if args[:perTable]
          @taskResults[args[:srcHost]][args[:srcDb]][:backupResultLog][args[:tableName]] = args[:status]
        else
          @taskResults[args[:srcHost]][args[:srcDb]][:backupResultLog] = args[:status]
        end
      elsif args[:task] == :restore
        req(:required => [:destDb,
                          :destHost,
                          :status,
                          :backupFile],
            :argsObject => args)
        if args[:perTable]
          @taskResults[args[:destHost]][args[:destDb]][:restoreResultLog][args[:backupFile]] = args[:status]
        else
          @taskResults[args[:destHost]][args[:destDb]][:restoreResultLog] = args[:status]
        end
      else
        raise RuntimeError, "args[:task] was not passed to the task"
      end
    end
    
    # :srcDb => Db to backup from,
    # :srcHost => Name of the connection in the connection manager i.e. Mysqladmin::Pool,
    # :srcIp => IP Address for :srcHost,
    # :user => User to connect to :srcIp as,
    # :password => Password to authenticate with for :user on :srcIp,
    # :procsAndTriggers => String to append to mysqldump to backup stored procedures and triggers for :srcDb,
    # :extendedInsert => String to append to mysqldump to allow for or eliminate multi-row inserts,
    # :tableName => Table to backup from :srcDb, required if this is a perTable
    #               based backup.
    # :timeStamp => If not nil or false prepend a timestamp to the backup filename,
    # :textFilter => commandline command to filter/replace text
    def doBackup(args)
      req(:required => [:srcDb,
                        :extendedInsert,
                        :procsAndTriggers,
                        :user,
                        :srcIp],
          :argsObject => args)
      args[:task] = :backup
      if args[:tableName]
        fileName = "#{args[:srcDb]}-#{args[:tableName]}.sql.gz"
        backupSrc = "'#{args[:srcDb]}' '#{args[:tableName]}'"
      else
        fileName = "#{args[:srcDb]}.sql.gz"
        backupSrc = "'#{args[:srcDb]}'"
      end
      if args[:timeStamp]
        fileName = "#{Time.now.strftime("%Y%m%d-%H%M")}-#{fileName}"
      end
      `#{coreReqs(:binary => "mysqldump")} --opt -Q #{args[:extendedInsert]} #{args[:procsAndTriggers]} -u #{args[:user]} #{args[:password]} -h #{args[:srcIp]} #{backupSrc} #{args[:textFilter]} | gzip > #{fileName}`
      updateTaskResults(checkExitCode(args))
      @taskResults[args[:srcHost]][args[:srcDb]][:backupFiles] << fileName
      # puts "#{coreReqs(:binary => "mysqldump")} --opt -Q #{args[:extendedInsert]} #{args[:procsAndTriggers]} -u #{args[:user]} #{args[:password]} -h #{args[:srcIp]} #{backupSrc} #{args[:textFilter]} | gzip > #{fileName}"
    end
    
    # :destDb => Name of the database we are restoring from the backup, not the
    #           same as :destDb but can be,
    # :destHost => Name of the host we are restoring to,
    # :backupFiles => Array object containing filenames with paths if outside of
    #                 cwd,
    # :backupFileFormat => The format of the files being passed in.  This is set
    #                      in the restoreFromBackup method but can be overwritten
    #                      if you think you know better. USE CAUTION!,
    # :textFilter => commandline command to filter text
    def doRestore(args)
      req(:required => [:backupFileFormat,
                        :backupFiles,
                        :user,
                        :password,
                        :destIp,
                        :destDb],
          :argsObject => args)
      args[:task] = :restore
      # Determine what tool we need to change the file to text and concatenate it
      # for a pipe to :textFilter and mysql
      cat = case args[:backupFileFormat]
      when :text then coreReqs(:binary => "cat")
      when :gzip then coreReqs(:binary => "gunzip", :cmdArgs => "-c")
      end
      
      
      # Do the actual restore, this method will be in flux, we need to use the cli
      # but need to do a better job of catching system errors, right now that needs
      # to be in checkExitCode.
      args[:backupFiles].each do |backupFile|
        args[:backupFile] = backupFile
        `#{cat} #{backupFile} #{args[:textFilter]} | #{coreReqs(:binary => "mysql")} -u #{args[:user]} -p#{args[:password]} -h #{args[:destIp]} #{args[:destDb]}`
        updateTaskResults(checkExitCode(args))
        @taskResults[args[:destHost]][args[:destDb]][:restoreFiles] << backupFile
      end
    end
  end
end
