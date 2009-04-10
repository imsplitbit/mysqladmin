#!/usr/bin/env ruby

#if File.directory?("../lib")
#  $:.unshift("../lib","../lib/mysqladmin")
#  puts "Using local mysqladmin code"
#end

require "rubygems"
require "mysqladmin"
require "pp"

Mysqladmin::Pool.add_connection( :host => "localhost",
                                 :user => "root",
                                 :password => "password",
                                 :connection_name => "local1")
rootlocal = Mysqladmin::User.new(:user => "341448_wpsb",
                                 :src_connection => "local1")
rootlocal.get_grants
rootlocal.conv_grants_to_revokes
#rootlocal.set_revokes(:connection_name => "local1")
rootlocal.get_grants
pp rootlocal
rootlocal.save(:file_name => "341448user.yml")
new_obj = rootlocal.load(:file_name => "341448user.yml")
pp new_obj
Mysqladmin::Pool.close_all_connections