require "yaml"
require "mysqladmin/arguments"
require "mysqladmin/serialize"

module Mysqladmin
  class User
    include Mysqladmin::Arguments
    include Mysqladmin::Serialize
    
    attr_accessor :user, :src_host, :src_connection, :dest_connection
    attr_reader :grants, :revokes
    
    # :user => User we are going to be manipulating,
    # :src_host => The host from which :user will be connecting from, think of this
    #             in terms of the grant statement ... TO ':user'@':src_host'...,
    # :password => If a change of password is desired, set this to the desired
    #              password.  This will overwrite the existing password when
    #              you run Object.set_grants,
    # :src_connection => Name of the connection being treated as the source for
    #                   getting/setting grants,
    # :dest_connection => Name of the connection being treated as the destination
    #                    when setting grants/revokes.  Defaults to :src_connection.
    #                    This is usually modified if you are migrating user
    #                    accounts from one server to another.
    def initialize(args = {})
      @grants = []
      @revokes = []
      @db_name = []
      @user = args[:user] || nil
      @src_host = args[:src_host] || "%"
      @password = args[:password] || nil
      @src_connection = args[:src_connection] || nil
      @dest_connection = args[:dest_connection] || nil
    end
    
    # :connection_name => Name of the connection to use in the resulting
    #                    Mysqladmin::Exec object.  Defaults to @src_connection
    #                    if not set.,
    # :custom_grant_sql => If you have a custom sql to show grants or otherwise
    #                    retrieve useful user info, do it here.  This is an SQL
    #                    injection point so do not expoose this outside of your
    #                    script.
    def get_grants(args = {})
      args[:custom_grant_sql] = nil unless args.has_key?(:custom_grant_sql)
      args[:connection_name] = @src_connection unless args.has_key?(:connection_name)
      
      # Mandatory args
      req(:required => [:connection_name],
          :args_object => args)
      
      @grants = [] if @grants.length > 0
      if args[:custom_grant_sql].nil?
        args[:sql] = "SHOW GRANTS FOR '#{@user}'@'#{@src_host}'"
      else
        args[:sql] = args[:custom_grant_sql]
      end
      res = Mysqladmin::Exec.new(args)
      res.query
      res.each_hash do |grant|
        @grants << grant["Grants for #{@user}@#{src_host}"]
      end
      args.delete(:connection_name)
      args.delete(:custom_grant_sql)
      args.delete(:sql)
    end
    
    # :connection_name => Name of the connection to apply the contents of @grants
    #                    to.  Defaults to @dest_connection if not set.
    def set_grants(args = {})
      args[:connection_name] = @dest_connection unless args.has_key?(:connection_name)
      
      # Mandatory args:
      req(:required => [:connection_name],
          :args_object => args)
      
      @grants.each do |grant|
        args[:sql] = grant
        dbh = Mysqladmin::Exec.new(args)
        dbh.query
        args.delete(:sql)
      end
      args.delete(:connection_name)
    end
    
    # :user => Username we are manipulating,
    # :src_host => The host from which :user will be connecting from, think of this
    #             in terms of the grant statement ... TO ':user'@':src_host'...,
    # :password => Password to set for ':user'@':src_host',
    # :connection_name => Server on which you wish to set args[:user]'s password.
    def set_password(args)
      args[:user] = @user unless args.has_key?(:user)
      args[:src_host] = @src_host unless args.has_key?(:src_host)
      args[:password] = @password unless args.has_key?(:password)
      args[:connection_name] = @dest_connection unless args.has_key?(:connection_name)
      
      # Mandatory args:
      req(:required => [:user,
                        :src_host,
                        :password,
                        :connection_name],
          :args_object => args)

      if args[:password] || @password
        password = args[:password] || @password
        args[:sql] = "SET PASSWORD FOR '#{args[:user]}'@'#{args[:src_host]}' = PASSWORD('#{args[:password]}')"
        dbh = Mysqladmin::Exec.new(args)
        dbh.query
        args.delete(:sql)
      end
      args.delete(:connection_name)
    end
    
    # :sql => Grant statement to append to the @grants Array.
    # :privileges => Array of privileges to Grant/Revoke,
    # :src_host => host the user will be connecting from,
    # :user => Username to affect,
    # :password => Password to use,
    # :db_name => Database name,
    # :table_name => Table
    def add_item(args)
      # Mandatory args:
      req(:required => [:sql,
                        :type],
          :args_object => args)
      case args[:type]
      when :grant then @grants << args[:sql]
      when :revoke then @revokes << args[:sql]
      else
        raise RuntimeError, "I don't understand what #{args[:type]} is"
      end
    end
    
    # No args but I wanted to prevent users from changing @grants from an Array
    # by accident.
    def flush_item(args)
      # Mandatory args:
      req(:required => [:type],
          :args_object => args)
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
    # the args[:dest_connection] server.
    def conv_grants_to_revokes
      if @revokes.empty?
        @grants.delete_if{|x| x == nil}.each do |grant|
          user = grant[/'*[\w]+.[\w]'*@'*[\w|\.|\%]+'*/].split("@").first.gsub("'", "")
          src_host = grant[/'*[\w]+.[\w]'*@'*[\w|\.|\%]+'*/].split("@").last.gsub("'", "")
          db_name = grant[/ON\ [\w|\W]+\.[\w|\W]+\ TO/].split[1].split(".").map{|x| x.include?("*") ? x : x.gsub!(/\W/,"")}[0]
          table_name = grant[/ON\ [\w|\W]+\.[\w|\W]+\ TO/].split[1].split(".").map{|x| x.include?("*") ? x : x.gsub!(/\W/,"")}[1]
          
          unless db_name == "*"
            db_name = "`#{db_name}`"
          end
          
          # Collect database names in an Array, @db_name for later use.
          @db_name << db_name
          
          unless table_name == "*"
            table_name = "`#{table_name}`"
          end
          privileges = []
          if grant[/^GRANT\ .+\ ON/].upcase.include?("ALL PRIVILEGES")
            privileges << "ALL PRIVILEGES"
          else
            privs = grant[/^GRANT\ .+\ ON/i].upcase.sub("GRANT ", "").sub(" ON", "").split(",")
            privs.each do |priv|
              privileges << priv
            end
          end
          if privileges.length <= 1
            privileges = privileges.first
          else
            privileges = privileges.join(", ")
          end
          @revokes << "REVOKE #{privileges} ON #{db_name}.#{table_name} FROM '#{user}'@'#{src_host}'"
        end
      end
    end
    
    # :connection_name => Server on which you wish to revoke args[:user]'s granted
    #                    privileges.
    def set_revokes(args = {})
      args[:connection_name] = @src_connection unless args.has_key?(:connection_name)
      
      # Mandatory args
      req(:required => [:connection_name],
          :args_object => args)
      
      @revokes.each do |revoke|
        args[:sql] = revoke
        dbh = Mysqladmin::Exec.new(args)
        dbh.query
        args.delete(:sql)
      end
      args.delete(:connection_name)
    end
  end
end
