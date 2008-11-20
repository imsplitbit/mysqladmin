require "yaml"
require "mysqladmin/arguments"
require "mysqladmin/serialize"

module Mysqladmin
  class User
    include Mysqladmin::Arguments
    include Mysqladmin::Serialize
    
    attr_accessor :user, :srcHost, :srcConnection, :destConnection
    attr_reader :grants, :revokes
    
    # Valid arguments:
    # {
    #   :user => User we are going to be manipulating,
    #   :srcHost => The host from which :user will be connecting from, think of this
    #               in terms of the grant statement ... TO ':user'@':srcHost'...,
    #   :password => If a change of password is desired, set this to the desired
    #                password.  This will overwrite the existing password when
    #                you run Object.setGrants,
    #   :srcConnection => Name of the connection being treated as the source for
    #                     getting/setting grants,
    #   :destConnection => Name of the connection being treated as the destination
    #                      when setting grants/revokes.  Defaults to :srcConnection.
    #                      This is usually modified if you are migrating user
    #                      accounts from one server to another.
    # }
    def initialize(args = {})
      @grants = []
      @revokes = []
      @dbNames = []
      @user = args[:user] || nil
      @srcHost = args[:srcHost] || "%"
      @password = args[:password] || nil
      @srcConnection = args[:srcConnection] || nil
      @destConnection = args[:destConnection] || nil
    end
    
    # Valid arguments:
    # {
    #   :connectionName => Name of the connection to use in the resulting
    #                      Mysqladmin::Exec object.  Defaults to @srcConnection
    #                      if not set.,
    # OPTIONAL: Defaults to nil
    #   :customGrantSQL => If you have a custom sql to show grants or otherwise
    #                      retrieve useful user info, do it here.  This is an SQL
    #                      injection point so do not expoose this outside of your
    #                      script.
    # }
    def getGrants(args = {})
      args[:customGrantSQL] = nil unless args.has_key?(:customGrantSQL)
      args[:connectionName] = @srcConnection unless args.has_key?(:connectionName)
      
      # Mandatory args
      req(:required => [:connectionName],
          :argsObject => args)
      
      @grants = [] if @grants.length > 0
      if args[:customGrantSQL].nil?
        args[:sql] = "SHOW GRANTS FOR '#{@user}'@'#{@srcHost}'"
      else
        args[:sql] = args[:customGrantSQL]
      end
      res = Mysqladmin::Exec.new(args)
      res.go
      res.each_hash do |grant|
        @grants << grant["Grants for #{@user}@#{srcHost}"]
      end
      args.delete(:connectionName)
      args.delete(:customGrantSQL)
      args.delete(:sql)
    end
    
    # Valid arguments:
    # {
    #   :connectionName => Name of the connection to apply the contents of @grants
    #                      to.  Defaults to @destConnection if not set.
    # }
    def setGrants(args = {})
      args[:connectionName] = @destConnection unless args.has_key?(:connectionName)
      
      # Mandatory args:
      req(:required => [:connectionName],
          :argsObject => args)
      
      @grants.each do |grant|
        args[:sql] = grant
        dbh = Mysqladmin::Exec.new(args)
        dbh.go
        args.delete(:sql)
      end
      args.delete(:connectionName)
    end
    
    # Valid arguments:
    # {
    #   :user => Username we are manipulating,
    #   :srcHost => The host from which :user will be connecting from, think of this
    #               in terms of the grant statement ... TO ':user'@':srcHost'...,
    #   :password => Password to set for ':user'@':srcHost',
    #   :connectionName => Server on which you wish to set args[:user]'s password.
    # }
    def setPassword(args)
      args[:user] = @user unless args.has_key?(:user)
      args[:srcHost] = @srcHost unless args.has_key?(:srcHost)
      args[:password] = @password unless args.has_key?(:password)
      args[:connectionName] = @destConnection unless args.has_key?(:connectionName)
      
      # Mandatory args:
      req(:required => [:user,
                        :srcHost,
                        :password,
                        :connectionName],
          :argsObject => args)

      if args[:password] || @password
        password = args[:password] || @password
        args[:sql] = "SET PASSWORD FOR '#{args[:user]}'@'#{args[:srcHost]}' = PASSWORD('#{args[:password]}')"
        dbh = Mysqladmin::Exec.new(args)
        dbh.go
        args.delete(:sql)
      end
      args.delete(:connectionName)
    end
    
    # Valid arguments:
    # {
    #   :sql => Grant statement to append to the @grants Array.
    # OPTIONAL:
    #   :privileges => Array of privileges to Grant/Revoke,
    #   :srcHost => host the user will be connecting from,
    #   :user => Username to affect,
    #   :password => Password to use,
    #   :dbName => Database name,
    #   :tableName => Table
    # }
    def addItem(args)
      # Mandatory args:
      req(:required => [:sql,
                        :type],
          :argsObject => args)
      
      
      
      case args[:type]
      when :grant then @grants << args[:sql]
      when :revoke then @revokes << args[:sql]
      else
        raise RuntimeError, "I don't understand what #{args[:type]} is"
      end
    end
    
    # No args but I wanted to prevent users from changing @grants from an Array
    # by accident.
    def flushItem(args)
      # Mandatory args:
      req(:required => [:type],
          :argsObject => args)
      case args[:type]
      when :grant then @grants = []
      when :revoke then @revokes = []
      else
        raise RuntimeError, "I don't understand what #{args[:type]} is"
      end
    end
    
    # This does an analysis of valid grant statements in @grants and
    # converts them into revoke statements.  You will find this handy
    # when you need to move users from one database server to another.
    #
    # Keep in mind that REVOKE USAGE doesn't actually remove the user entirely
    # there will still be a stubbed user that has USAGE privs on the machine.
    #
    # Use Object.deleteUser(args) to remove a user once and for all from
    # the args[:destConnection] server.
    #
    # Valid arguments:
    # {
    #   NONE
    # }
    def convGrantsToRevokes
      if @revokes.empty?
        @grants.delete_if{|x| x == nil}.each do |grant|
          user = grant[/'*[\w]+.[\w]'*@'*[\w|\.|\%]+'*/].split("@").first.gsub("'", "")
          srcHost = grant[/'*[\w]+.[\w]'*@'*[\w|\.|\%]+'*/].split("@").last.gsub("'", "")
          dbName = grant[/ON\ [\w|\W]+\.[\w|\W]+\ TO/].split[1].split(".").map{|x| x.include?("*") ? x : x.gsub!(/\W/,"")}[0]
          tableName = grant[/ON\ [\w|\W]+\.[\w|\W]+\ TO/].split[1].split(".").map{|x| x.include?("*") ? x : x.gsub!(/\W/,"")}[1]
          
          unless dbName == "*"
            dbName = "`#{dbName}`"
          end
          
          # Collect database names in an Array, @dbNames for later use.
          @dbNames << dbName
          
          unless tableName == "*"
            tableName = "`#{tableName}`"
          end
          privileges = []
          if grant[/^GRANT\ .+\ ON/].upcase.include?("ALL PRIVILEGES")
            privileges << "ALL PRIVILEGES"
          else
            privs = grant[/^GRANT\ .+\ ON/i].upcase.sub("GRANT ", "").sub(" ON", "").split(",")
            privs.each do |priv|
              unless priv == "GRANT" || priv == "ON"
                if priv[/^.*[\W|\s|\d]+.*$/]
                  priv = priv.gsub!(/\W|\s|\d/, "")
                end
                privileges << priv
              end
            end
          end
          if privileges.length <= 1
            privileges = privileges.first
          else
            privileges = privileges.join(", ")
          end
          @revokes << "REVOKE #{privileges} ON #{dbName}.#{tableName} FROM '#{user}'@'#{srcHost}'"
        end
      end
    end
    
    # Valid arguments:
    # {
    #   :connectionName => Server on which you wish to revoke args[:user]'s granted
    #                      privileges.
    # }
    def setRevokes(args = {})
      args[:connectionName] = @srcConnection unless args.has_key?(:connectionName)
      
      # Mandatory args
      req(:required => [:connectionName],
          :argsObject => args)
      
      @revokes.each do |revoke|
        args[:sql] = revoke
        dbh = Mysqladmin::Exec.new(args)
        dbh.go
        args.delete(:sql)
      end
      args.delete(:connectionName)
    end
  end
end
