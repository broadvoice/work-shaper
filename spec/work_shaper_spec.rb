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

  it "has a version number" do
    expect(WorkShaper::VERSION).not_to be nil
  end

  context 'Work that Fails' do
    let(:some_work) { MockWorkNoErrors.new }
    it "should still ack" do
      work_shaper = subject

      work_shaper.enqueue('1', 'message', 0, 0)

      sleep 0.05 while work_shaper.total_acked < 1
      expect(some_work.acked).to be true
      expect(work_shaper.total_acked).to be 1
      expect(work_shaper.total_enqueued).to be 1
    end
  end

  context 'Work that Fails to Ack' do
    let(:some_work) { MockWorkThatRaisesOnAck.new }
    it "should shutdown" do
      work_shaper = subject

      work_shaper.enqueue('1', 'message', 0, 0)
      sleep 0.5
      expect(work_shaper.shutting_down).to be true
    end
  end


  context 'Received message twice' do
    let(:some_work) { MockWorkNoErrors.new }
    it "should still ack" do
      work_shaper = subject

      work_shaper.enqueue('1', 'message', 0, 0)
      work_shaper.enqueue('1', 'message', 0, 0)

      sleep 0.05 while work_shaper.total_acked < 2
      expect(work_shaper.total_acked).to be 2
      expect(work_shaper.total_enqueued).to be 2
    end

    let(:some_work) { MockWorkThatFails.new }
    it "Multiple Partitions should work" do
      work_shaper = subject
      partitions = {}
      (0..10).each do |i|
        partitions[i] ||= 0
      end

      100.times do
        p = Random.rand(10)
        partitions[p] += 1
        partitions[p] -= 2 if Random.rand > 0.95

        work_shaper.enqueue("#{p}", 'message', 0, partitions[p])
      end

      sleep 0.05 while work_shaper.total_acked < 100
      expect(some_work.acked).to be true
      expect(work_shaper.total_acked).to be 100
      expect(work_shaper.total_enqueued).to be 100
      subject.report
    end
  end
end
