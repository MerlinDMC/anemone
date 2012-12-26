begin
  require 'leveldb'
rescue LoadError
  puts $!
  puts "You need the leveldb-ruby gem to use Anemone::Storage::LevelDB"
  exit
end

require 'forwardable'

module Anemone
  module Storage
    class LevelDB
      extend Forwardable

      def_delegators :@db, :close, :size, :keys, :has_key?

      def initialize(file)
        @db = ::LevelDB::DB.new(file)
      end

      def [](key)
        if value = @db[key]
          load_value(value)
        end
      end

      def []=(key, value)
        @db[key] = [Marshal.dump(value)].pack("m")
      end

      def delete(key)
        value = self[key]
        @db.delete(key)
        value
      end

      def each
        @db.keys.each do |k, v|
          if v
            yield(k, load_value(v))
          else
            yield(k, v)
          end
        end
      end

      def merge!(hash)
        hash.each { |key, value| self[key] = value }
        self
      end

      private

      def load_value(value)
        Marshal.load(value.unpack("m")[0])
      end

    end
  end
end
