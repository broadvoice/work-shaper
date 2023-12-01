module WorkShaper
  # The Manager is responsible for organizing the work to be done, triggering calls to acknowledge work done
  # for each offset in monotonically increasing order (independent of the execution order), and gracefully
  # cleaning up when `#shutdown` is called.
  class Manager
    include LoggerFactory
    # Several of the parameters here are Lambdas (not Proc). Note you can pass a method using
    # `method(:some_method)` or a lambda directly `->{ puts 'Hello'}`.
    #
    # @param work [#call(message, partition, offset)] Lambda that we will call to execute work.
    # @param on_done [#call(message, partition, offset)] Lambda that we call when work is done.
    # @param ack [] Lambda we will call when it is safe to commit an offset. This is not the
    #   same as Done.
    # @param on_error [#call(exception, message, partition, offset)] Lambda that we call if an
    #   error is encountered.
    # @param max_in_queue [Integer] The maximum in flight jobs per Sub Key. This affects how many
    #   message could get replayed if your process crashes before the offsets are committed.
    def initialize(work, on_done, ack, on_error, max_in_queue = 3)
      @work = work
      @on_done = on_done
      @ack = ack
      @on_error = on_error
      @workers = {}
      @last_ack = {}
      @received_offsets = {}
      @completed_offsets = {}
      @max_in_queue = max_in_queue
      @semaphore = Mutex.new
      @shutdown = false

      @total_enqueued = 0

      @heartbeat = Thread.new do
        while true
          report
          sleep 15
        end
      rescue => e
        puts 'Shutdown from Heartbeat'
        puts e
        shutdown
      end

      @offset_manager = Thread.new do
        while true
          @completed_offsets.each_key do |partition|
            offset_ack(partition)
          end
          sleep 0.005
          # puts "ack loop" if Random.rand < 0.01
        end
      rescue => e
        puts 'Shutdown from Offset Manager'
        puts e
        shutdown
      end
    end

    # Enqueue a message to be worked on the given `sub_key`, `partition`, and `offset`.
    def enqueue(sub_key, message, partition, offset)
      raise StandardError, 'Shutting down' if @shutdown
      pause_on_overrun

      worker = nil
      @semaphore.synchronize do
        @total_enqueued += 1
        (@received_offsets[partition] ||= SortedSet.new) << offset

        worker =
          @workers[sub_key] ||=
            Worker.new(
              @work,
              @on_done,
              method(:offset_ack),
              @on_error,
              @last_ack,
              @completed_offsets,
              @semaphore,
              @max_in_queue
            )
      end

      worker.enqueue(message, partition, offset)
    end

    # Flush any offsets for which work has been completed. Only lowest continuous run of
    # offsets will be acknowledged. Any offset after a discontinuity will be replayed when
    # the consumer restarts.
    def flush(safe: true)
      sleep 5
      @completed_offsets.each_key do |k|
        safe ? offset_ack(k) : offset_ack_unsafe(k)
      end
    end

    # Output state of Last Acked and Pending Offset Ack's.
    def report(detailed: false)
      @semaphore.synchronize do
        puts "Total Enqueued: #{@total_enqueued}"
        puts "Total Acked: #{@total_acked}"
        puts "In flight: #{@total_enqueued.to_i - @total_acked.to_i}"
        puts "Last Ack'd Offsets: #{@last_ack}"
        puts "Worker Count: #{@workers.keys.count}"
        if detailed
          puts "Pending Ack: #{@completed_offsets}"
          puts "Received: #{@received_offsets}"
        end
      end
    end

    # Stop the underlying threads
    def shutdown
      @shutdown = true
      report(detailed: true)
      Thread.kill(@heartbeat)
      Thread.kill(@offset_manager)
      @workers.each_value(&:shutdown)
    end

    private

    def offset_ack(partition)
      @semaphore.synchronize do
        offset_ack_unsafe(partition)
      end
    end

    def offset_ack_unsafe(partition)
      @total_acked ||= 0

      completed = @completed_offsets[partition]
      received = @received_offsets[partition]

      offset = completed.first
      while received.any? && received.first == offset
        # We observed Kafka sending the same message twice, even after
        # having committed the offset. Here we skip this offset if we
        # know it has already been committed.
        last_offset = @last_ack[partition]
        if last_offset && offset <= last_offset
          puts "Received Duplicate #{partition}:#{offset}"
        else
          result = @ack.call(partition, offset)
          if result.is_a? Exception
            puts @completed_offsets[partition].to_a[0..10].join(',')
            puts @received_offsets[partition].to_a[0..10].join(',')
          else
            @total_acked += 1
            @last_ack[partition] = offset
          end
        end

        completed.delete(offset)
        received.delete(offset)

        offset = completed.first
        if result.is_a? Exception
          puts result.message
          # raise result
        end
        # raise result if result.is_a? Exception
      end
    end

    def pause_on_overrun
      overrun = lambda do
        @total_enqueued.to_i - @total_acked.to_i > @max_in_queue
      end

      # We have to be careful here to avoid a deadlock. Another thread may be waiting
      # for the mutex to ack and remove offsets. If we wrap enqueue in a synchronize
      # block, that would lead to a deadlock. Here the sleep allows other threads
      # to wrap up.
      sleep 0.005 while @semaphore.synchronize { overrun.call }
    end
  end
end