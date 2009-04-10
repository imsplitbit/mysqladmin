#!/usr/bin/env ruby

require "rubygems"
require "mysqladmin"
require "pp"

Mysqladmin::Pool.add_connection(:host => "localhost",
                               :user => "root",
                               :password => "password",
                               :connection_name => "local")
foo = Mysqladmin::Logs.new(:connection_name => "local")

print foo.binary_logs

foo.list_entry(:type => :binary, :position => 3273, :file_name => "mysql-bin.000001")

puts foo.last_entry
puts foo.last_db
pp foo.tables
#pp foo