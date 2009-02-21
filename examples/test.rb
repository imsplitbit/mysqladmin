#!/usr/bin/env ruby

require "rubygems"
require "mysqladmin"
require "pp"

Mysqladmin::Pool.addConnection(:host => "localhost",
                               :user => "root",
                               :password => "password",
                               :connectionName => "local")
foo = Mysqladmin::Logs.new(:connectionName => "local")

pp foo.foo