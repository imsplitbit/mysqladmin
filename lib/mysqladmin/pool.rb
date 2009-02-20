require "mysql"
require "mysqladmin/arguments"
    
module Mysqladmin
  module Pool
    extend Mysqladmin::Arguments
    
    attr_reader :connections, :connectionPools
    
    # :defaultPool => Pool to place all connections in by default.  This is
    #                 set to "all" by default,
    # :crashIfExists => Set to true if you want the application to raise an
    #                   exception if the item you are creating already exists,
    # :overwriteIfExists => Set to true if you want the application to nicely
    #                       close an existing item if you try to create one of
    #                       the same name.
    # :debug => Print everything to stdout instead of doing anything
    def self.create(args = {})
      return if defined? @@connections
      @@defaultPool = args.has_key?(:defaultPool) ? args[:defaultPool] : "all"
      @@crashIfExists = args[:crashIfExists] || false
      @@overwriteIfExists = args[:overwriteIfExists] || false
      @@debug = args[:debug] || false
      @@connections = {}
      @@connectionPools = {}
      @@connectionPools[@@defaultPool] = []
      return true
    end
    
    def self.connections
      @@connections
    end
    
    def self.connectionPools
      @@connectionPools
    end
    
    # :host => Host to connect to.  This must be an FQDN or IP Address,
    # :user => User to connect to :host as,
    # :password => Password for :user when connecting to :host,
    # :port => Port server on :host is listening on, Defaults to *3306*,
    # :connectionName => Human parseable/understandable name for :host, may be
    #                    set to anything that helps you understand what that
    #                    host is.  i.e. I use the FQDN for :connectionName,
    # :poolName => Pool to add the connection with :host to.  Defaults to "all".
    #              This is stored in the instance attribute connectionPools
    #              which is a hash keyed on the pool name.  Each key points
    #              to an array of :connectionName values,
    # :crashIfExists => See initialize(args) above,
    # :overwriteIfExists => See initialize(args) above.
    def self.addConnection(args)
      args[:crashIfExists]      = @@crashIfExists unless args.has_key?(:crashIfExists)
      args[:overwriteIfExists]  = @@overwriteIfExists unless args.has_key?(:overwriteIfExists)
      args[:poolName]           = @@defaultPool unless args.has_key?(:poolName)
      args[:port]               = 3306 unless args.has_key?(:port)
      
      # Mandatory args:
      req(:required => [:crashIfExists,
                        :overwriteIfExists,
                        :poolName,
                        :port,
                        :host,
                        :user,
                        :password,
                        :connectionName],
          :argsObject => args)
      
      if @@connections.keys.include?(args[:connectionName])
        if args[:crashIfExists] == true
          raise NameError, "The connection named '#{args[:connectionName]}' exists"
        elsif args[:overwriteIfExists] == true
          # Flush out the overwriteIfExists so that it doesn't carry over to other methods
          args.delete(:overwriteIfExists) if args.has_key?(:overwriteIfExists)
          args.delete(:crashIfExists) if args.has_key?(:crashIfExists)
          close(args)
          connect(args)
        else
          false
        end
      else
        # Flush out the overwriteIfExists so that it doesn't carry over to other methods
        args.delete(:overwriteIfExists) if args.has_key?(:overwriteIfExists)
        args.delete(:crashIfExists) if args.has_key?(:crashIfExists)
        connect(args)
        addToPool(args)
      end
    end
    
    # :poolName => Pool to create,
    # :crashIfExists => See initialize(args) above,
    # :overwriteIfExists => See initialize(args) above.
    def self.addPool(args)
      args[:crashIfExists] = @@crashIfExists unless args.has_key?(:crashIfExists)
      args[:overwriteIfExists] = @@overwriteIfExists unless args.has_key?(:overwriteIfExists)
      
      # Mandatory args:
      req(:required => [:poolName],
          :argsObject => args)
      if @@connectionPools.has_key?(args[:poolName])
        if args[:crashIfExists] == true
          raise NameError, "The pool name '#{args[:poolName]}' exists"
        elsif args[:overwriteIfExists] == true
          # Flush overwriteIfExists so it doesn't filter down to other method calls
          args.delete(:overwriteIfExists) if args.has_key?(:overwriteIfExists)
          args.delete(:crashIfExists) if args.has_key?(:crashIfExists)
          closePool(args)
          @@connectionPools[args[:poolName]] = []
          true
        else
          false
        end
      else
        @@connectionPools[args[:poolName]] = []
        true
      end
    end

    # :poolName => Name of the pool to add :connectionName to,
    # :connectionName => Name of the connection to include in :poolName,
    # :crashIfExists => See initialize(args) above,
    # :overwriteIfExists => See initialize(args) above.
    def self.addToPool(args)
      args[:crashIfExists] = @@crashIfExists unless args.has_key?(:crashIfExists)
      args[:overwriteIfExists] = @@overwriteIfExists unless args.has_key?(:overwriteIfExists)
      
      # Mandatory args:
      req(:required => [:poolName,
                        :connectionName],
          :argsObject => args)
      
      addPool(args)
      if @@connectionPools[args[:poolName]].include?(args[:connectionName])
        if args[:crashIfExists] == true
          raise NameError, "The connection #{args[:connectionName]} exists in #{args[:poolName]}"
        elsif args[:overwriteIfExists] == true
          # Flush overwriteIfExists so it doesn't filter down to other method calls
          args.delete(:overwriteIfExists) if args.has_key?(:overwriteIfExists)
          args.delete(:crashIfExists) if args.has_key?(:crashIfExists)
          close(args[:connectionName])
          @@connectionPools[args[:poolName]] << args[:connectionName]
          true
        else
          false
        end
      else
        @@connectionPools[args[:poolName]] << args[:connectionName]
        true
      end
      unless args[:poolName] == @@defaultPool
        addToPool(:poolName => @@defaultPool)
      end
    end
    
    # :connectionName => Name of the connection to remove from :poolName,
    # :poolName => Pool to delete :connectionName from.
    def self.deleteFromPool(args)
      # Mandatory args:
      req(:required => [:poolName,
                        :connectionName],
          :argsObject => args)

      @@connectionPools[args[:poolName]].delete(args[:connectionName])
    end
    
    #   :connectionName => Name of the connection to close if it is open.
    def self.close(args)
      # Mandatory args:
      req(:required => [:connectionName],
          :argsObject => args)
      
      @@connections[args[:connectionName]][:dbh].close if @@connections[args[:connectionName]][:dbh]
      @@connections.delete(args[:connectionName])
      deleteFromPool(args)
    end
    
    # :poolName => Name of the pool to totally close and destroy.  All connections
    #              associated with :poolName will be gracefully closed if the
    #              connection handle it stores has a Object.close method.
    def self.closePool(args)
      # Mandatory args:
      req(:required => [:poolName],
          :argsObject => args)
      
      @@connectionPools[args[:poolName]].each do |connectionName|
        close(:connectionName => connectionName, :poolName => args[:poolName])
      end
      @@connectionPools.delete(args[:poolName]) unless args[:poolName] == @defaultPool
    end
    
    def self.closeAllConnections
      closePool(:poolName => @@defaultPool)
    end
    
    private
    
    # :host => FQDN or IP Address to connect to,
    # :user => User to connect to :host,
    # :password => Password to use when authenticating on :host as :user,
    # :port => Port the server on :host is listening on, defaults to 3306.
    # :connectionName => Human readable/understandable name to give this connection.
    def self.connect(args)
      # Mandatory args:
      req(:required => [:connectionName,
                        :host,
                        :user,
                        :password,
                        :port],
          :argsObject => args)
      
      @@connections[args[:connectionName]] = {}
      @@connections[args[:connectionName]][:dbh] = Mysql.connect(host = args[:host], user = args[:user], passwd = args[:password], db = nil, port = args[:port].to_i)
      @@connections[args[:connectionName]][:host] = args[:host]
      @@connections[args[:connectionName]][:user] = args[:user]
      @@connections[args[:connectionName]][:password] = args[:password]
      @@connections[args[:connectionName]][:queries] = {}
      @@connections[args[:connectionName]][:queries][:stats] = {}
      @@connections[args[:connectionName]][:queries][:stats][:select] = 0
      @@connections[args[:connectionName]][:queries][:stats][:insert] = 0
      @@connections[args[:connectionName]][:queries][:stats][:update] = 0
      @@connections[args[:connectionName]][:queries][:stats][:delete] = 0
      @@connections[args[:connectionName]][:queries][:data] = {}
      @@connections[args[:connectionName]][:queries][:data][:sql] = {}
    end
  end
end