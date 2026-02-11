# frozen_string_literal: true
require 'json'

module ActiveRecord
  module Type
    module Dm
      class Vector < ActiveRecord::Type::Value

        attr_reader :dim, :format

        MIN_DIM = 1
        MAX_DIM = 65535

        def initialize(dim: nil, format: nil)
          @dim = dim
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
          self.class == other.class &&
            dim == other.dim &&
            format == other.format
        end
        alias eql? ==

        def hash
          [self.class, dim, format].hash
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

