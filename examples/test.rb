#!/usr/bin/env ruby

require "rubygems"
require "mysqladmin"
require "pp"

Mysqladmin::Pool.addConnection(:host => "localhost",
                               :user => "root",
                               :password => "password",
                               :connectionName => "local")
foo = Mysqladmin::Logs.new(:connectionName => "local")

print foo.binaryLogs

foo.listEntry(:type => :binary, :position => 3273, :fileName => "mysql-bin.000001")

puts foo.lastEntry
puts foo.lastDb
pp foo.tables
#pp foo