#!/usr/bin/env ruby
require "../lib/mysqladmin/thread"
puts "Testing thread pool"

counter = 0

def iterCounter(counter, num)
  counter += num
end

pool = ThreadPool.new(40)
400.times do |num|
  pool.process { puts iterCounter(counter, num) }
end

pool.join