# Grabbed from here: http://snippets.dzone.com/posts/show/3276
#
# applying my own stuff to it, plus some suggested patches.

require "thread"

class ThreadPool
  class Worker
    def initialize
      @mutex = Mutex.new
      @thread = Thread.new do
        while true
          sleep 0.001
          block = getBlock
          if block
            block.call
            resetBlock
          end
        end
      end
    end
    
    def getBlock
      @mutex.synchronize { @block }
    end
    
    def setBlock(block)
      @mutex.synchronize do
        raise RuntimeError, "Thread already busy." if @block
        @block = block
      end
    end
    
    def resetBlock
      @mutex.synchronize { @block = nil }
    end
    
    def busy?
      @mutex.synchronize { !@block.nil? }
    end
  end
  
  attr_accessor :maxSize
  attr_reader :workers

  def initialize(maxSize = 10)
    @maxSize = maxSize
    @workers = []
    @mutex = Mutex.new
  end
  
  def size
    @mutex.synchronize { @workers.size }
  end
  
  def busy?
    @mutex.synchronize { @workers.any? { |w| w.busy? } }
  end
  
  def join
    sleep 0.01 while busy?
  end
  
  def process(&block)
    while true
      @mutex.synchronize do
        worker = findAvailableWorker
        if worker
          return worker.setBlock(block)
        end
      end
      sleep 0.01
    end
  end
  
  def waitForWorker
    while true
      worker = findAvailableWorker
      return worker if worker
      sleep 0.01
    end
  end
  
  def findAvailableWorker
    freeWorker || createWorker
  end
  
  def freeWorker
    @workers.each { |w| return w unless w.busy? }; nil
  end
  
  def createWorker
    return nil if @workers.size >= @maxSize
    worker = Worker.new
    @workers << worker
    worker
  end
end
