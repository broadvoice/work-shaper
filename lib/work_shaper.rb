# frozen_string_literal: true

require_relative "work_shaper/version"

# WorkShaper is inspired by Kafka partitions and offsets, but could be used to organize and
# parallelize other forms of work. The original goal was to parallelize processing offsets in
# a given partition while maintaining order for a subset of the messages based on Sub Keys.
#
# The key concepts include Sub Key, Partition, and Offset. Work on a given Sub Key must be
# executed in the order in which it is enqueued. However, work on different Sub Keys can run
# in parallel. All Work (offset) on a given Partition must be Acknowledged in continuous
# monotonically increasing order. If a higher offset's work is completed before a lower offset,
# the Manager will hold the acknowledgement until all lower offsets are acknowledged. Remember,
# work (offsets) for a given sub key are still processed in order.
module WorkShaper
  def self.logger=(logger)
    @logger = logger
  end
  def self.logger
    @logger ||= Logger.new(
      $stdout,
      level: ENV['LOG_LEVEL'] || 'DEBUG',
      formatter: Ruby::JSONFormatter::Base.new
    )
  end
end

