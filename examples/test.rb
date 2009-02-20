#!/usr/bin/env ruby

require "rubygems"
require "mysqladmin"
require "pp"

Mysqladmin::Pool.addConnection(:host => "localhost",
                               :user => "root",
                               :password => "password",
                               :connectionName => "local")

vars = Mysqladmin::ServerInfo.serverVariables(:connectionName => "local")

pp vars