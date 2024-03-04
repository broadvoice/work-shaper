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
      max_in_queue: 1
    )
  end

  it "has a version number" do
    expect(WorkShaper::VERSION).not_to be nil
  end

  context 'Work that Fails' do
    let(:some_work) { MockWorkThatFails.new }
    it "should still ack" do
      subject.enqueue('1', 'message', 0, 0)

      sleep some_work.work_sleep_ms / 1000.0 + 0.1
      expect(some_work.acked).to be true
    end
  end


  context 'Received message twice' do
    let(:some_work) { MockWorkThatFails.new }
    it "should still ack" do
      subject.enqueue('1', 'message', 0, 0)
      subject.enqueue('1', 'message', 0, 0)

      sleep some_work.work_sleep_ms / 1000.0 + 0.5
      expect(subject.total_acked).to be 2
    end
  end
end
