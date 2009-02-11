#!/usr/bin/env ruby

require "rubygems"
require "mysqladmin"

Mysqladmin::Pool.addConnection(:host => "localhost",
                               :user => "root",
                               :password => "password",
                               :connectionName => "local1")
# Mysqladmin::Pool.addConnection(:host => "localhost",
#                                :user => "root",
#                                :password => "password",
#                                :connectionName => "local2")


buJob1 = Mysqladmin::Backup.new
buJob1.backupHost(:perTable => false,
                  :extendedInsert => true,
                  :srcHost => "local1")
buJob1.restoreDbFromBackup(:srcDb => "testing",
                           :srcHost => "local1",
                           :destDb => "testing2",
                           :destHost => "local1",
                           :overwriteIfExists => true)