require "rubygems"
require "mysqladmin"
require "pp"

Mysqladmin::Pool.add_connection( :host => "localhost",
                                 :user => "root",
                                 :password => "password",
                                 :connection_name => "local1")
puts "BEFORE"
pp Mysqladmin::Pool.connections
Mysqladmin::Pool.create

puts "AFTER"
pp Mysqladmin::Pool.connections
