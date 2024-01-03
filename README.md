# WorkShaper

WorkShaper is inspired by Kafka partitions and offsets, but could be used to organize and
parallelize other forms of work. The original goal was to parallelize processing offsets in
a given partition while maintaining order for a subset of the messages based on Sub Keys.

The key concepts include Sub Key, Partition, and Offset. Work on a given Sub Key must be
executed in the order in which it is enqueued. However, work on different Sub Keys can run
in parallel. All Work (offsets) on a given Partition must be Acknowledged in continuous
monotonically increasing order. If a higher offset's work is completed before a lower offset,
the Manager will hold the acknowledgement until all lower offsets are acknowledged. Remember,
work (offsets) for a given sub key are still processed in order.

## Installation

TODO: Replace `UPDATE_WITH_YOUR_GEM_NAME_PRIOR_TO_RELEASE_TO_RUBYGEMS_ORG` with your gem name right after releasing it to RubyGems.org. Please do not do it earlier due to security reasons. Alternatively, replace this section with instructions to install your gem from git if you don't plan to release to RubyGems.org.

Install the gem and add to the application's Gemfile by executing:

    $ bundle add UPDATE_WITH_YOUR_GEM_NAME_PRIOR_TO_RELEASE_TO_RUBYGEMS_ORG

If bundler is not being used to manage dependencies, install the gem by executing:

    $ gem install UPDATE_WITH_YOUR_GEM_NAME_PRIOR_TO_RELEASE_TO_RUBYGEMS_ORG

## Usage

### Example

```ruby
consumer = MyRdKafka.consumer(...)

# Called for each message
work = ->(message, _p, _o) do
  MsgProcessor.process(message)
end

# Called each time `work` completes
done = ->(_m, _p, _o) {}

# Called periodically after work is complete to acknowledge the
# completed work. Completed offsets are queued and processed every
# 5 ms by the OffsetManager.
ack = ->(p, o) do
  consumer.store_offset(ENV.fetch('TOPIC_NAME'), p, o)
rescue InvalidOffset => e
  # On rebalance, RdKafka sets the offset to _INVALID for the consumer
  # losing that offset. In this scenario InvalidOffset is expected
  # and we should move on.
  # TODO: This can probably be more elegantly handled.
end

# Call if an exception in encountered in `done`. It is important to
# understand `work` is being called in a sub thread, so the exception
# will not bubble up.
error = ->(e, m, p, o) do
  logger.error "#{e} on #{p} #{o}"
  @fatal_error = e
end

max_in_queue = ENV.fetch('MAX_THREAD_QUEUE_SIZE', 25)

work_shaper = WorkShaper::Manager.new(
  work: work, 
  on_done: done, 
  ack: ack, 
  on_error: error, 
  max_in_queue: max_in_queue
)

@value_to_subkey = {}
max_sub_keys = ENV.fetch('MAX_SUB_KEYS', 100)
consumer.each_message do |message|
  break if @fatal_error
  
  sub_key = @value_to_subkey[message.payload['some attribute']] ||=
    MurmurHash3::V32.str_hash(message.payload['some attribute']) % max_sub_keys

  work_shaper.enqueue(
    sub_key,
    message,
    message.partition,
    message.offset
  )
end
```


## Development


## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/broadvoice/work-shaper.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
