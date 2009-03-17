module Mysqladmin
  module System
    include Mysqladmin::Arguments
    
    # :binary => Binary file that is required for continued operation,
    # :cmdArgs => Arguments to add to the end of the command returned if
    #             arguments are required for proper operation.
    def coreReqs(args)
      # Mandatory args:
      req(:required => [:binary],
          :argsObject => args)
      
      coreFile = `which #{args[:binary]}`.strip
      if $?.exitstatus == 0
        coreFile += " #{args[:cmdArgs]}" if args.has_key?(:cmdArgs)
        return coreFile
      else
        raise RuntimeError, "The core requirement '#{args[:binary]}' doesn't exist in the system path!"
      end
    end
  end
end
