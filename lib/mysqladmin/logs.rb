module Mysqladmin
  class Logs
    include Mysqladmin::System
    include Mysqladmin::Arguments
    include Mysqladmin::ServerInfo
    
    attr_accessor :time
    attr_reader :binary_logs, :slow_log, :general_log, :relay_log, :last_entry, :last_db, :tables
    
    def initialize(args={})
      req(:required => [:connection_name], :args_object => args)
      @start = Time.now
      @binary_logs = {}
      @slow_log = nil
      @general_log = nil
      @relay_logs = {}
      @tables = []
      @vars = server_variables(:connection_name => args[:connection_name], :type => "variables")
      @status = server_variables(:connection_name => args[:connection_name], :type => "status")
      parse_my_cnf
    end
    
    def parse_my_cnf
      my_cnf_files = ["/etc/my.cnf", "#{@vars[:datadir]}my.cnf"]
      my_cnf_files.each do |file|
        if File.exist?(file)
          File.readlines(file).each do |line|
            if line.include?("!include")
              my_cnf_files << file.split(" ")[1].strip
            end
          end
        end
      end
      my_cnf_files.each do |file|
        if File.exist?(file)
          File.readlines(file).each do |line|
            if line.include?("=")
              var = line.split("=")[0].gsub("-", "_").strip
              val = line.split("=")[1].strip
              if var[/^log_bin$/i]
                Dir.glob("#{val}*").delete_if{|x| x[/index$/]}.each do |bin_log|
                  bin_log_name = File.basename(bin_log)
                  @binary_logs[bin_log_name] = {}
                  @binary_logs[bin_log_name][:path] = bin_log
                end
              elsif var[/^relay_log$/i]
                Dir.glob("#{val}*").delete_if{|x| x[/index$/]}.each do |relay_log|
                  relay_log_name = File.basename(relay_log)
                  @relay_logs[relay_log] = {}
                  @relay_logs[relay_log][:path] = relay_log
                end
              elsif var[/^log_slow_queries$/i]
                @slow_log = val
              elsif var[/^log$/i]
                @general_log = val
              end
            end
          end
        end
      end
    end
    
    def list_entry(args = {})
      req(:required => [:type, :file_name, :position], :args_object => args)
      valid_types = [:binary, :relay]
      if valid_types.include?(args[:type])
        if args[:type] == :relay 
          log_file = @relay_logs[args[:file_name]][:path]
        elsif args[:type] == :binary
          log_file = @binary_logs[args[:file_name]][:path]
        end
        
        @last_entry = `#{core_reqs(:binary => "/usr/local/mysql/bin/mysqlbinlogs")} --start-position=#{args[:position]} --stop-position=#{args[:position].to_i + 1} #{args[:file_name]}`.split(";").map{|x| x.strip}
        @last_entry.each do |entry|
          db_name = entry.match(/use (.+)/i)
          unless db_name == nil
            @last_db = db_name[1]
          end
        end
        @last_entry.each do |entry|
          entry = entry.gsub("\n", "").gsub(/\s+/, " ")
          if entry[/^insert/i]
            x = entry.match(/into (.+?)(value|\(| )/i)
          elsif entry[/^update/i]
            x = entry.match(/update (.+?) set/i)
          elsif entry[/^delete/i]
            x = entry.match(/from (.+?) (where|order|limit|$).*/)
          end
          if x[1].include?(",")
            @tables + x[1].split(",").map{ |x| x.strip! }
          else
            @tables + x[1].strip.to_a
          end
        end
      end
    end
    
  end
end