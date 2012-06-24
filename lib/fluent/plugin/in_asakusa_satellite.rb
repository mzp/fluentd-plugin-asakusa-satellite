#! /opt/local/bin/ruby -w
# -*- mode:ruby; coding:utf-8 -*-
require 'uri'
require 'json'

class AsakusaSatelliteInput < Fluent::Input
  Fluent::Plugin.register_input('asakusa_satellite', self)
  config_param :tag, :string
  config_param :url, :string
  config_param :room, :string
  config_param :apikey, :string
  config_param :interval, :time, :default => 60
  config_param :pos_file, :string, :default => nil

  class TimerWatcher < Coolio::TimerWatcher
    def initialize(interval, repeat, &callback)
      @callback = callback
      super(interval, repeat)
    end

    def on_timer
      @callback.call
    rescue
      $log.error $!.to_s
      $log.error_backtrace
    end
  end

  class AsClient < Coolio::HttpClient
    def preinitialize(addr, port, f)
      super addr,port

      @json = ""
      @action = f
    end

    def on_body_data(data)
      @json << data
    end

    def on_request_complete
      $log.info "fetch as data"
      JSON.parse(@json).each(&@action)
    end

    def on_error(reason)
      $log.error reason
    end
  end

  def start
    super
    @loop = Coolio::Loop.new
    @entry_point = URI.join(@url, "/api/v1/message/list.json")

    @timer = TimerWatcher.new(@interval, true, &method(:on_timer))
    @loop.attach(@timer)

    @thread = Thread.new(&method(:run))
  end

  def shutdown
    @loop.watchers.each {|w| w.detach }
    @loop.stop
    @thread.join
  end

  private
  def run
    @loop.run
  rescue
    $log.error "unexpected error", :error=>$!.to_s
    $log.error_backtrace
  end

  def on_timer
    @client = AsClient.connect(@entry_point.host, @entry_point.port, method(:on_message))
    @loop.attach(@client)
    query = {
      :room_id => @room,
      :apikey => @apikey,
    }
    if since_id then
      query.merge!(:since_id => since_id)
    end
    @client.request("GET", @entry_point.path, :query => query)
  end

  def on_message(message)
    update_since_id message['id']
    p message['created_at']
    Fluent::Engine.emit(@tag,
                        Time.parse(message['created_at'] + " UTC").to_i, {
                          "id"   => message['id'],
                          "body" => message['body'],
                          "name" => message['name'],
                          "screen_name" => message['screen_name'],
                          "room" => message['room']['name']
                        })
  end

  def since_id
    if @pos_file
      File.read(@pos_file) rescue nil
    else
      @since_id
    end
  end

  def update_since_id(id)
    if @pos_file then
      File.open(@pos_file,'w') do|io|
        io.write id
      end
    else
      @since_id = id
    end
  end
end
