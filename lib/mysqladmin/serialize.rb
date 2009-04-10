require "yaml"

module Mysqladmin
  module Serialize
    # :file_name => Path/file_name in which to preserve the Mysqladmin::Backup
    #              object in yaml form, path must exist!.
    def save(args)
      File.open(args[:file_name], "w") do |fh|
        fh.sync = true
        fh.puts self.to_yaml
        fh.close
      end
    end
    
    # :file_name => Path/file_name of a serialized (yaml) Mysqladmin::Backup object
    #              to load.  Returns a new object of Mysqladmin::Backup class.
    def load(args)
      YAML::load_file(args[:file_name])
    end
  end
end