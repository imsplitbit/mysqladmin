#!/usr/bin/env ruby

#if File.directory?("../lib")
#  $:.unshift("../lib","../lib/mysqladmin")
#  puts "Using local mysqladmin code"
#end

require "rubygems"
require "mysqladmin"
require "pp"

Mysqladmin::Pool.addConnection(:host => "localhost",
                               :user => "root",
                               :password => "password",
                               :connectionName => "local1")
rootlocal = Mysqladmin::User.new(:user => "341448_wpsb",
                                 :srcConnection => "local1")
rootlocal.getGrants
rootlocal.convGrantsToRevokes
#rootlocal.setRevokes(:connectionName => "local1")
rootlocal.getGrants
pp rootlocal
rootlocal.save(:fileName => "341448user.yml")
newObj = rootlocal.load(:fileName => "341448user.yml")
pp newObj
Mysqladmin::Pool.closeAllConnections