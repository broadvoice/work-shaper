module WorkShaper
  # The Manager is responsible for organizing the work to be done, triggering calls to acknowledge work done
  # for each offset in monotonically increasing order (independent of the execution order), and gracefully
  # cleaning up when `#shutdown` is called.
  class Manager
    attr_reader :total_acked, :total_enqueued, :shutting_down

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
    def initialize(work:, on_done:, ack:, on_error:, max_in_queue: 3,
                   heartbeat_period_sec: 60, offset_commit_period_ms: 5)
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
      @shutting_down = false

      @total_enqueued = 0
      @total_acked = 0

      @heartbeat = Thread.new do
        while true
          report(detailed: true)
          sleep heartbeat_period_sec
        end
      rescue => e
        WorkShaper.logger.warn({ message: 'Shutdown from Heartbeat', error: e })
        shutdown
      end

      @offset_manager = Thread.new do
        while true
          @completed_offsets.each_key do |partition|
            offset_ack(partition)
          end
          sleep offset_commit_period_ms / 1000.0
        end
      rescue => e
        WorkShaper.logger.warn({ message: 'Shutdown from Offset Manager', error: e })
        shutdown
      end
    end

    # Enqueue a message to be worked on the given `sub_key`, `partition`, and `offset`.
    def enqueue(sub_key, message, partition, offset)
      raise StandardError, 'Shutting down' if @shutting_down
      pause_on_overrun

      offset_holder = OffsetHolder.new(partition, offset)
      WorkShaper.logger.debug "Enqueue: #{sub_key}/#{offset_holder}"

      worker = nil
      @semaphore.synchronize do
        @total_enqueued += 1
        (@received_offsets[partition] ||= Array.new) << offset_holder

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

      worker.enqueue(message, offset_holder)
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
        WorkShaper.logger.info(
          { message: 'Reporting', total_enqueued: @total_enqueued,
            total_acked: @total_acked,
            in_flight: (@total_enqueued.to_i - @total_acked.to_i),
            last_acked_offsets: @last_ack,
            worker_count: @workers.keys.count,
            offset_mgr: @offset_manager.status
          })
        if detailed
          WorkShaper.logger.info(
            {
              message: 'Reporting - Extra Detail',
              pending_ack: @completed_offsets,
              received_offsets: @received_offsets
            })
        end
      end
    end

    # Stop the underlying threads
    def shutdown
      @shutting_down = true
      WorkShaper.logger.warn({ message: 'Shutting Down' })
      Thread.kill(@heartbeat)
      Thread.kill(@offset_manager)
      report(detailed: true)
      @workers.each_value(&:shutdown)
    end

    private

    def offset_ack(partition)
      @semaphore.synchronize do
        offset_ack_unsafe(partition)
      end
    end

    def offset_ack_unsafe(partition)
      completed = @completed_offsets[partition].sort!
      received = @received_offsets[partition].sort!

      begin
        offset = completed.first
        while received.any? && received.first == offset
          # We observed Kafka sending the same message twice, even after
          # having committed the offset. Here we skip this offset if we
          # know it has already been committed.
          last_offset = @last_ack[partition]
          if last_offset && offset <= last_offset
            WorkShaper.logger.warn(
              { message: 'Received Duplicate Offset',
                offset: "#{partition}:#{offset}",
                last_acked: last_offset,
              })
          end

          result =
            begin
              @ack.call(partition, offset.to_i)
            rescue => e
              # We expect @ack to handle it's own errors and return the error or false if it
              # is safe to continue. Otherwise @ack should raise an error and we will
              # shutdown.
              WorkShaper.logger.error({ message: 'Error in ack', error: e })
              WorkShaper.logger.error(e.backtrace.join("\n"))
              shutdown
              break
            end

          if result.is_a? Exception || !result
            WorkShaper.logger.warn(
              { message: 'Failed to Ack Offset, likely re-balance',
                offset: "#{partition}:#{offset}",
                completed: @completed_offsets[partition].to_a[0..10].join(','),
                received: @received_offsets[partition].to_a[0..10].join(',')
              })
          else
            @last_ack[partition] = [@last_ack[partition] || offset, offset].max
          end

          @total_acked += 1
          WorkShaper.logger.debug "@total_acked: #{@total_acked}"
          WorkShaper.logger.debug "completed: [#{completed.join(', ')}]"
          WorkShaper.logger.debug "received: [#{received.join(', ')}]"
          completed.delete(offset)
          received.delete(offset)

          offset = completed.first
        end
      rescue => e
        WorkShaper.logger.error({ message: 'Error in offset_ack', error: e })
        WorkShaper.logger.error(e.backtrace.join("\n"))
      end
    end

    def pause_on_overrun
      overrun = lambda do
        @total_enqueued.to_i - @total_acked.to_i > @max_in_queue
      end

      pause_cycles = 0
      # We have to be careful here to avoid a deadlock. Another thread may be waiting
      # for the mutex to ack and remove offsets. If we wrap enqueue in a synchronize
      # block, that would lead to a deadlock. Here the sleep allows other threads
      # to wrap up.
      while @semaphore.synchronize { overrun.call } do
        if pause_cycles % 12000 == 0
          WorkShaper.logger.warn 'Paused on Overrun'
          report(detailed: true)
        end
        pause_cycles += 1
        sleep 0.005
      end
    end
  end
end
