$:.unshift(File.dirname(__FILE__)) unless $:.include?(File.dirname(__FILE__)) || $:.include?(File.expand_path(File.dirname(__FILE__)))

require "mysqladmin/backup"
require "mysqladmin/pool"
require "mysqladmin/error"
require "mysqladmin/exec"
require "mysqladmin/monitor"
require "mysqladmin/perfstat"
require "mysqladmin/replication"
require "mysqladmin/statistics"
require "mysqladmin/user"
require "mysqladmin/serverinfo"
require "mysqladmin/system"
require "mysqladmin/arguments"
require "mysqladmin/logger"
require "mysqladmin/serialize"
require "mysqladmin/filesystem"
require "mysqladmin/cluster"
require "threadpool"

module Mysqladmin
  Mysqladmin::Pool.create
end