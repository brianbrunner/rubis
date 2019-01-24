require 'binary_search_tree'
require 'csv'
require 'json'
require 'set'
require 'socket'

class Entry
  @name = 'Base Hash Entry'

  def initialize(key, value = nil)
    @key = key
    @value = value
    @lock = Mutex.new
  end
  attr_reader :lock
end

class SetEntry < Entry
  @name = 'Set'

  def initialize(key, value = nil)
    if value.nil?
      value = Set.new
    end
    super(key, value)
  end

  def sadd(value)
    @value.add value
  end

  def srem(value)
    @value.delete value
  end

  def scard
    @value.size
  end

  def sismember(key)
    @value.member? key
  end
end

class SortedSetEntry < Entry
  @name = 'SortedSet'

  def initialize(key, value = nil)
    if value.nil?
      value = BinarySearchTreeHash.new
    end
    super(key, value)
  end
end

class ListEntry < Entry
  @name = 'List'

  def initialize(key, value = [])
    super(key, value)
  end

  def lpush(value)
    @value.unshift(0, value)
  end

  def lpop
    @value.shift
  end

  def rpush(value)
    @value.push(value)
  end

  def rpop
    @value.pop
  end
end

class HashEntry < Entry
  @name = 'Hash'

  def initialize(key, value = {})
    super(key, value)
  end

  def hset(key, value)
    @value[key] = value
  end

  def hget(key)
    @value[key]
  end
end

class StringEntry < Entry
  @name = 'String'

  def initialize(key, value = '')
    super(key, value)
  end

  def set(value)
    @value = value
  end

  def append(value)
    @value += value
  end

  def get
    @value
  end
end

class Server

  def initialize(address = 'localhost', port = 6369)
    @address = address
    @port = port
    @server_socket = TCPServer.open(address, port)

    @master_lock = Mutex.new
    @data = {}
    @entry_classes = [StringEntry, HashEntry, ListEntry, SetEntry]
    @entry_class_methods = {}

    @entry_classes.each do |entry_class|
      entry_class.instance_methods(false).each do |method|
        @entry_class_methods[method] = entry_class
      end
    end
  end

  def run
    puts "Listening on #{@address}:#{@port}"
    loop do
      Thread.start(@server_socket.accept) do |client|
        read_commands(client)
      end
    end
  end

  def read_commands(client)
    loop do
      message = client.gets.chomp
      parts = CSV::parse_line(message, { :col_sep => ' ' })
      command = parts[0]
      key = parts[1]
      args = *parts[2..parts.length]
      handle_command(client, command, key, args)
    end
  end

  def handle_command(client, command, key, args)
    if @data.key?(key)
      begin
        entry = @data[key]
        result = nil
        entry.lock.synchronize do
          result = entry.send(command, *args)
        end
        json_result = JSON.generate(result)
        client.puts json_result
      rescue Exception => e
        client.puts e.to_s
      end
    elsif @entry_class_methods.key?(command.to_sym)
      interleaved = false
      @master_lock.synchronize do
        if !@data.key?(key)
          entry_class = @entry_class_methods[command.to_sym]
          entry = entry_class.new(key)
          result = nil
          entry.lock.synchronize do
            @data[key] = entry
            result = entry.send(command, *args)
          end
          json_result = JSON.generate(result)
          client.puts json_result
        else
          interleaved = true
        end
      end
      if interleaved
        handle_command(client, command, key, args)
      end
    else
      client.puts 'Error: that key does not exist'
    end
  end
end

server = Server.new
server.run
