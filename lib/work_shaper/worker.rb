module WorkShaper
  # The worker that runs the stuff
  class Worker
    include LoggerFactory
    # rubocop:disable Metrics/ParameterLists
    # rubocop:disable Layout/LineLength
    # @param work [Lambda] Lambda that we will #call(message) to execute work.
    # @param on_done [Lambda] Lambda that we #call(partition, offset) when work is done.
    # @param on_error [Lambda] Lambda that we #call(exception) if an error is encountered.
    def initialize(work, on_done, ack_handler, on_error, last_ack, offset_stack, semaphore, max_in_queue)
      @jobs = []
      @work = work
      @on_done = on_done
      @ack_handler = ack_handler
      @on_error = on_error
      @last_ack = last_ack
      @completed_offsets = offset_stack
      @semaphore = semaphore
      @max_in_queue = max_in_queue
      @thread_pool = Concurrent::FixedThreadPool.new(1, auto_terminate: false)
    end

    # rubocop:enable Metrics/ParameterLists
    # rubocop:enable Layout/LineLength

    def enqueue(message, partition, offset)
      # rubocop:disable Style/RescueStandardError
      @thread_pool.post do
        @work.call(message, partition, offset)
        @on_done.call(message, partition, offset)
        @semaphore.synchronize do
          (@completed_offsets[partition] ||= SortedSet.new) << offset
        end
        # @ack_handler.call(partition, offset)
      rescue => e
        puts("Error processing #{partition}:#{offset} #{e}")
        puts(e.backtrace.join("\n"))
        # logger.error("Acking it anyways, why not?")
        @on_error.call(e, message, partition, offset)
        # @ack_handler.call(partition, offset)
      end
      # rubocop:enable Style/RescueStandardError
    end

    def shutdown
      # Cannot call logger from trap{}
      WorkShaper.logger.info({message: 'Shutting down worker'})
      @thread_pool.shutdown
      @thread_pool.wait_for_termination
      sleep 0.05 while @thread_pool.queue_length.positive?
    end

    private
  end
end