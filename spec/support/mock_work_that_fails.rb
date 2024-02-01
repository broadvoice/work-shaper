class MockWorkThatFails
  attr_reader :acked, :errored

  def work_sleep_ms
    5
  end
  def do_work(message, _partition, _offset)
    sleep work_sleep_ms / 1000.0
    raise 'foo'
  end

  def on_done(_message, _partition, _offset) end

  def on_ack(_partition, _offset)
    @acked = true
  end

  def on_error(_e, _message, _partition, _offset)
    @errored = true
  end

end