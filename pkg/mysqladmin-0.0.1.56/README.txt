= mysqladmin

* http://projects.splitbit.com/projects/show/rbmysqladmin

== DESCRIPTION:

MySQL administration library.  Use for maintaining small or large numbers of
mysql servers.

== EXAMPLES:
  I like hashes, love it, or rewrite it.  All objects take hashes as arguments
  which gets this module closer to being 1.9 ready when, hopefully, we get
  keyword arguments added as a core feature.
  
  Get user grants:
  
  pool = Mysqladmin::Pool.new
  pool.addConnection(:host => "localhost",
                     :user => "root",
                     :password => "password",
                     :connectionName => "local1")
  userInfo = Mysqladmin::User.new(:username => "root",
                                  :pool => pool,
                                  :srcConnectionName => "local1")
  userInfo.getGrants
  puts userInfo.grants.inspect
  
  
  Take a backup of all databases:
  

== FEATURES/PROBLEMS:

* Connection pooling
* Multi-threaded pool actions
* Just plain cool!

== SYNOPSIS:

  require "mysqladmin"
  and go!!!!

== REQUIREMENTS:

* mysql       >= 2.7
* fastthread  >= 1.0

== INSTALL:

* sudo gem install mysqladmin

== LICENSE:

(The MIT License)

Copyright (c) 2008 Monkey Puppet Labs

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
'Software'), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
