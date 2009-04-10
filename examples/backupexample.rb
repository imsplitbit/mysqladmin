#!/usr/bin/env ruby

require "rubygems"
require "mysqladmin"

Mysqladmin::Pool.add_connection( :host => "localhost",
                                 :user => "root",
                                 :password => "password",
                                 :connection_name => "local1")
                                 
# Mysqladmin::Pool.add_connection(:host => "localhost",
#                                :user => "root",
#                                :password => "password",
#                                :connection_name => "local2")


bu_job1 = Mysqladmin::Backup.new
bu_job1.backup_host( :per_table => false,
                     :extended_insert => true,
                     :src_host => "local1")
bu_job1.restore_db_from_backup( :src_db => "testing",
                                :src_host => "local1",
                                :dest_db => "testing2",
                                :dest_host => "local1",
                                :overwrite_if_exists => true)