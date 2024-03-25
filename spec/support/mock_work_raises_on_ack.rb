class MockWorkThatRaisesOnAck
  attr_reader :acked, :acked_count, :errored

  def work_sleep_ms
    5
  end

  def do_work(message, _partition, _offset)
    raise ArgumentError, 'Offset is expected to be an Integer' unless _offset.is_a?(Integer)
    raise ArgumentError, 'Partition is expected to be an Integer' unless _partition.is_a?(Integer)
    sleep work_sleep_ms / 1000.0
  end

  def on_done(_message, _partition, _offset)
    raise ArgumentError, 'Offset is expected to be an Integer' unless _offset.is_a?(Integer)
    raise ArgumentError, 'Partition is expected to be an Integer' unless _partition.is_a?(Integer)
  end

  def on_ack(_partition, _offset)
    raise StandardError, "Fatal Error on Ack"
  end

  def on_error(_e, _message, _partition, _offset)
    @errored = true
    raise _e
  end

end