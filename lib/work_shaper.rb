# frozen_string_literal: true

require 'logger'
require 'sorted_set'
require 'concurrent-ruby'
require_relative "work_shaper/version"
require_relative "work_shaper/manager"
require_relative "work_shaper/worker"
require 'json'

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
      formatter: formatter
    )
  end

  def self.formatter
    proc do |severity, datetime, _progname, msg|
      datefmt = datetime.strftime('%Y-%m-%dT%H:%M:%S.%6N')
      {
        timestamp: datefmt,
        level: severity.ljust(5),
        pid: Process.pid,
        msg: msg
      }.to_json + "\n"
    end
  end
end

