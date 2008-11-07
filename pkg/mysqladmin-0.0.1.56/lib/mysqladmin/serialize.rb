require "yaml"

module Mysqladmin
  module Serialize
    # Valid arguments:
    # {
    #   :fileName => Path/Filename in which to preserve the Mysqladmin::Backup
    #                object in yaml form, path must exist!.
    # }
    def save(args)
      File.open(args[:fileName], "w") do |fh|
        fh.sync = true
        fh.puts self.to_yaml
        fh.close
      end
    end
    
    # Valid arguments:
    # {
    #   :fileName => Path/Filename of a serialized (yaml) Mysqladmin::Backup object
    #                to load.  Returns a new object of Mysqladmin::Backup class.
    # }
    def load(args)
      YAML::load_file(args[:fileName])
    end
  end
end