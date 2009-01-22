module Mysqladmin
  class Sync
    def initialize(args={})
      @source = args[:source] || nil
      @replica = args[:replica] || nil
    end
    
    def syncTable(args={})
      
    end
    
    def syncHost(args={})
      
    end
    
    def syncPool(args={})
      
    end
  end
end