require "mysql"
require "mysqladmin/arguments"
    
module Mysqladmin
  module Pool
    extend Mysqladmin::Arguments
    
    attr_reader :connections, :connection_pools
    
    # :default_pool => Pool to place all connections in by default.  This is
    #                 set to "all" by default,
    # :crash_if_exists => Set to true if you want the application to raise an
    #                   exception if the item you are creating already exists,
    # :overwrite_if_exists => Set to true if you want the application to nicely
    #                       close an existing item if you try to create one of
    #                       the same name.
    # :debug => Print everything to stdout instead of doing anything
    def self.create(args = {})
      return if defined? @@connections
      @@default_pool = args.has_key?(:default_pool) ? args[:default_pool] : "all"
      @@crash_if_exists = args[:crash_if_exists] || false
      @@overwrite_if_exists = args[:overwrite_if_exists] || false
      @@debug = args[:debug] || false
      @@connections = {}
      @@connection_pools = {}
      @@connection_pools[@@default_pool] = []
      return true
    end
    
    def self.connections
      @@connections
    end
    
    def self.connection_pools
      @@connection_pools
    end
    
    # :host => Host to connect to.  This must be an FQDN or IP Address,
    # :user => User to connect to :host as,
    # :password => Password for :user when connecting to :host,
    # :port => Port server on :host is listening on, Defaults to *3306*,
    # :connection_name => Human parseable/understandable name for :host, may be
    #                    set to anything that helps you understand what that
    #                    host is.  i.e. I use the FQDN for :connection_name,
    # :pool_name => Pool to add the connection with :host to.  Defaults to "all".
    #              This is stored in the instance attribute connection_pools
    #              which is a hash keyed on the pool name.  Each key points
    #              to an array of :connection_name values,
    # :crash_if_exists => See initialize(args) above,
    # :overwrite_if_exists => See initialize(args) above.
    def self.add_connection(args)
      args[:crash_if_exists]      = @@crash_if_exists unless args.has_key?(:crash_if_exists)
      args[:overwrite_if_exists]  = @@overwrite_if_exists unless args.has_key?(:overwrite_if_exists)
      args[:pool_name]            = @@default_pool unless args.has_key?(:pool_name)
      args[:port]                 = 3306 unless args.has_key?(:port)
      
      # Mandatory args:
      req(:required => [:crash_if_exists,
                        :overwrite_if_exists,
                        :pool_name,
                        :port,
                        :host,
                        :user,
                        :password,
                        :connection_name],
          :args_object => args)
      
      if @@connections.keys.include?(args[:connection_name])
        if args[:crash_if_exists] == true
          raise NameError, "The connection named '#{args[:connection_name]}' exists"
        elsif args[:overwrite_if_exists] == true
          # Flush out the overwrite_if_exists so that it doesn't carry over to other methods
          args.delete(:overwrite_if_exists) if args.has_key?(:overwrite_if_exists)
          args.delete(:crash_if_exists) if args.has_key?(:crash_if_exists)
          close(args)
          connect(args)
        else
          false
        end
      else
        # Flush out the overwrite_if_exists so that it doesn't carry over to other methods
        args.delete(:overwrite_if_exists) if args.has_key?(:overwrite_if_exists)
        args.delete(:crash_if_exists) if args.has_key?(:crash_if_exists)
        connect(args)
        add_to_pool(args)
      end
    end
    
    # :pool_name => Pool to create,
    # :crash_if_exists => See initialize(args) above,
    # :overwrite_if_exists => See initialize(args) above.
    def self.add_pool(args)
      args[:crash_if_exists] = @@crash_if_exists unless args.has_key?(:crash_if_exists)
      args[:overwrite_if_exists] = @@overwrite_if_exists unless args.has_key?(:overwrite_if_exists)
      
      # Mandatory args:
      req(:required => [:pool_name],
          :args_object => args)
      if @@connection_pools.has_key?(args[:pool_name])
        if args[:crash_if_exists] == true
          raise NameError, "The pool name '#{args[:pool_name]}' exists"
        elsif args[:overwrite_if_exists] == true
          # Flush overwrite_if_exists so it doesn't filter down to other method calls
          args.delete(:overwrite_if_exists) if args.has_key?(:overwrite_if_exists)
          args.delete(:crash_if_exists) if args.has_key?(:crash_if_exists)
          close_pool(args)
          @@connection_pools[args[:pool_name]] = []
          true
        else
          false
        end
      else
        @@connection_pools[args[:pool_name]] = []
        true
      end
    end

    # :pool_name => Name of the pool to add :connection_name to,
    # :connection_name => Name of the connection to include in :pool_name,
    # :crash_if_exists => See initialize(args) above,
    # :overwrite_if_exists => See initialize(args) above.
    def self.add_to_pool(args)
      args[:crash_if_exists] = @@crash_if_exists unless args.has_key?(:crash_if_exists)
      args[:overwrite_if_exists] = @@overwrite_if_exists unless args.has_key?(:overwrite_if_exists)
      
      # Mandatory args:
      req(:required => [:pool_name,
                        :connection_name],
          :args_object => args)
      
      add_pool(args)
      if @@connection_pools[args[:pool_name]].include?(args[:connection_name])
        if args[:crash_if_exists] == true
          raise NameError, "The connection #{args[:connection_name]} exists in #{args[:pool_name]}"
        elsif args[:overwrite_if_exists] == true
          # Flush overwrite_if_exists so it doesn't filter down to other method calls
          args.delete(:overwrite_if_exists) if args.has_key?(:overwrite_if_exists)
          args.delete(:crash_if_exists) if args.has_key?(:crash_if_exists)
          close(args[:connection_name])
          @@connection_pools[args[:pool_name]] << args[:connection_name]
          true
        else
          false
        end
      else
        @@connection_pools[args[:pool_name]] << args[:connection_name]
        true
      end
      unless args[:pool_name] == @@default_pool
        add_to_pool(:pool_name => @@default_pool)
      end
    end
    
    # :connection_name => Name of the connection to remove from :pool_name,
    # :pool_name => Pool to delete :connection_name from.
    def self.delete_from_pool(args)
      # Mandatory args:
      req(:required => [:pool_name,
                        :connection_name],
          :args_object => args)

      @@connection_pools[args[:pool_name]].delete(args[:connection_name])
    end
    
    #   :connection_name => Name of the connection to close if it is open.
    def self.close(args)
      # Mandatory args:
      req(:required => [:connection_name],
          :args_object => args)
      
      @@connections[args[:connection_name]][:dbh].close if @@connections[args[:connection_name]][:dbh]
      @@connections.delete(args[:connection_name])
      delete_from_pool(args)
    end
    
    # :pool_name => Name of the pool to totally close and destroy.  All connections
    #              associated with :pool_name will be gracefully closed if the
    #              connection handle it stores has a Object.close method.
    def self.close_pool(args)
      # Mandatory args:
      req(:required => [:pool_name],
          :args_object => args)
      
      @@connection_pools[args[:pool_name]].each do |connection_name|
        close(:connection_name => connection_name, :pool_name => args[:pool_name])
      end
      @@connection_pools.delete(args[:pool_name]) unless args[:pool_name] == @default_pool
    end
    
    def self.close_all_connections
      close_pool(:pool_name => @@default_pool)
    end
    
    private
    
    # :host => FQDN or IP Address to connect to,
    # :user => User to connect to :host,
    # :password => Password to use when authenticating on :host as :user,
    # :port => Port the server on :host is listening on, defaults to 3306.
    # :connection_name => Human readable/understandable name to give this connection.
    def self.connect(args)
      # Mandatory args:
      req(:required => [:connection_name,
                        :host,
                        :user,
                        :password,
                        :port],
          :args_object => args)
      
      @@connections[args[:connection_name]] = {}
      @@connections[args[:connection_name]][:dbh] = Mysql.connect(host = args[:host], user = args[:user], passwd = args[:password], db = nil, port = args[:port].to_i)
      @@connections[args[:connection_name]][:host] = args[:host]
      @@connections[args[:connection_name]][:user] = args[:user]
      @@connections[args[:connection_name]][:password] = args[:password]
      @@connections[args[:connection_name]][:queries] = {}
      @@connections[args[:connection_name]][:queries][:stats] = {}
      @@connections[args[:connection_name]][:queries][:stats][:select] = 0
      @@connections[args[:connection_name]][:queries][:stats][:insert] = 0
      @@connections[args[:connection_name]][:queries][:stats][:update] = 0
      @@connections[args[:connection_name]][:queries][:stats][:delete] = 0
      @@connections[args[:connection_name]][:queries][:data] = {}
      @@connections[args[:connection_name]][:queries][:data][:sql] = {}
    end
  end
end