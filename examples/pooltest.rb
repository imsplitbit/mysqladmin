require "rubygems"
require "mysqladmin"
require "pp"

Mysqladmin::Pool.addConnection(:host => "localhost",
                               :user => "root",
                               :password => "password",
                               :connectionName => "local1")
puts "BEFORE"
pp Mysqladmin::Pool.connections
Mysqladmin::Pool.create

puts "AFTER"
pp Mysqladmin::Pool.connections
