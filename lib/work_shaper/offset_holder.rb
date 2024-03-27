module WorkShaper
  class OffsetHolder
    attr_reader :partition, :offset, :state

    STATES = {enqueued: 2, acked: 1, completed: 0}
    def initialize(partition, offset, at: Time.now.to_f)
      @partition = partition
      @offset = offset
      @at = at

      @state = :enqueued
    end

    def <=(other)
      self.<=>(other) <= 0
    end

    def <(other)
      self.<=>(other) == -1
    end

    def <=>(other)
      r = offset <=> other.offset

      if r == 0
        r = STATES[state] <=> STATES[other.state]
        WorkShaper.logger.debug "States: #{r} | #{STATES[state]} #{STATES[other.state]}"
      end

      if r == 0
        r = @at <=> other.instance_variable_get(:@at)
        WorkShaper.logger.debug "At: #{r}"
      end
      WorkShaper.logger.debug "Final: #{r}"
      r
    end

    def ack!
      @state = :acked
    end

    def complete!
      @state = :completed
    end

    def completed?
      @state == :completed
    end

    def to_i
      offset
    end

    def to_s
      "#{partition}/#{offset}:#{STATES[state]}"
    end
  end
end