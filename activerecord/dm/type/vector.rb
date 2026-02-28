# frozen_string_literal: true
require 'json'

module ActiveRecord
  module Type
    module Dm
      class Vector < ActiveRecord::Type::Value

        attr_reader :dim, :format, :storage_format

        MIN_DIM = 1
        MAX_DIM = 65535

        def initialize(dim: nil, limit: nil, format: nil, storage_format: nil)
          @dim = dim
          @limit = limit
          @fomat = format
        end

        def type
          :vector
        end

        def serialize(value)
          if value.is_a?(Array)
            value = value.to_s
          end
          value
        end

        def ==(other)
          local_dim = dim || limit
          other_local_dim = other.dim || other.limit
          self.class == other.class &&
            local_dim == other_local_dim &&
            format == other.format &&
            storage_format == other.storage_format
        end
        alias eql? ==

        def hash
          [self.class, dim, format, storage_format].hash
        end

        def l1_distance(column)
          return column
        end

        private
          def cast_value(value)
            if value.is_a?(String)
              if value == "[]"
                return []
              else
                value = JSON.parse(value)
              end
            end
            value
         end
      end
    end
  end
end

