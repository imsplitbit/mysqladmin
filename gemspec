#!/usr/bin/env ruby

require 'rubygems'
require 'yaml'
require 'lib/mysqladmin/version'

Gem::manage_gems

REV = YAML.load(`svn info`)['Revision']
VERS = Mysqladmin::VERSION::STRING + (REV ? ".#{REV}" : "")

spec = Gem::Specification.new do |s|
  s.name = "mysqladmin"
  s.version = VERS
  s.author = "Daniel Salinas"
  s.email = "imsplitbit@gmail.com"
  s.homepage = "http://projects.splitbit.com/projects/show/rbmysqladmin"
  s.platform = Gem::Platform::RUBY
  s.summary = "A toolkit mysql administration"

  files = Dir.glob("{bin,tests,lib,docs}/**/*")

  s.files = files.delete_if do |file|
    file.match(/^.svn/) || file.match(/~$/)
  end

  s.require_path = "lib"
  s.has_rdoc = true
  s.add_dependency("mysql", ">= 2.7")
end


if $0 == __FILE__
  Gem::manage_gems
  Gem::Builder.new(spec).build
end

