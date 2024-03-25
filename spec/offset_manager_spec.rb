# frozen_string_literal: true
require 'spec_helper'
require 'work_shaper'

RSpec.describe WorkShaper do
  subject(:work_shaper) do
    WorkShaper::Manager.new(
      work: some_work.method(:do_work),
      on_done: some_work.method(:on_done),
      ack: some_work.method(:on_ack),
      on_error: some_work.method(:on_error),
      heartbeat_period_sec: 1,
      max_in_queue: 5
    )
  end


  context 'Received message twice' do
    let(:some_work) { MockWorkNoErrors.new }

    it "should only ack matching offset reference" do
      work_shaper = subject
      t = Time.now
      o1 = WorkShaper::OffsetHolder.new(0, 0, at: t)
      o2 = WorkShaper::OffsetHolder.new(0, 0, at: t)

      o1.complete!
      o2.complete!
      puts "o1 == o2? #{o1 == o2}"
      completed_offsets = { 0 => [o1] }
      received_offsets = { 0 => [o2] }

      work_shaper.instance_variable_set(:@completed_offsets, completed_offsets)
      work_shaper.instance_variable_set(:@received_offsets, received_offsets)

      work_shaper.send(:offset_ack_unsafe, 0)

      puts completed_offsets
      puts received_offsets

    end
  end
end
