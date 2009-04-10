module Mysqladmin
  module System
    include Mysqladmin::Arguments
    
    # :binary => Binary file that is required for continued operation,
    # :cmd_args => Arguments to add to the end of the command returned if
    #             arguments are required for proper operation.
    def core_reqs(args)
      # Mandatory args:
      req(:required => [:binary],
          :args_object => args)
      
      core_file = `which #{args[:binary]}`.strip
      if $?.exitstatus == 0
        core_file += " #{args[:cmd_args]}" if args.has_key?(:cmd_args)
        return core_file
      else
        raise RuntimeError, "The core requirement '#{args[:binary]}' doesn't exist in the system path!"
      end
    end
  end
end
