class MockWorkNoErrors
  attr_reader :acked, :acked_count, :errored

  def work_sleep_ms
    5
  end

  def do_work(message, _partition, _offset)
    raise ArgumentError, 'Offset is expected to be an Integer' unless _offset.is_a?(Integer)
    raise ArgumentError, 'Partition is expected to be an Integer' unless _partition.is_a?(Integer)

    sleep work_sleep_ms / 1000.0 * Random.rand
  end

  def on_done(_message, _partition, _offset)
    raise ArgumentError, 'Offset is expected to be an Integer' unless _offset.is_a?(Integer)
    raise ArgumentError, 'Partition is expected to be an Integer' unless _partition.is_a?(Integer)
  end

  def on_ack(_partition, _offset)
    raise ArgumentError, 'Offset is expected to be an Integer' unless _offset.is_a?(Integer)
    raise ArgumentError, 'Partition is expected to be an Integer' unless _partition.is_a?(Integer)

    @offset_max ||= _offset
    @offset_max = [@offset_max, _offset].max

    @acked_count ||= 0
    @acked_count += 1
    puts "on_ack: #{@acked_count}"

    return StandardError.new("Offset #{_offset} is less than #{@offset_max}") if _offset < @offset_max
    @acked = true
  end

  def on_error(_e, _message, _partition, _offset)
    @errored = true
    raise _e
  end

end