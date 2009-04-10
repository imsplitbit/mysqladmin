module Mysqladmin
  module Arguments
    # :required => Array of required keys in the args hash,
    # :argsObject => arg object to inspect.
    def req(args)
      args[:required].each do |req|
        unless args[:args_object].keys.include?(req)
          raise RuntimeError, "The required argument #{req} was not given"
        end
        if args[:args_object][req].nil?
          raise RuntimeError, "The required argument #{req} was nil, cannot continue"
        end
      end
    end
  end
end
