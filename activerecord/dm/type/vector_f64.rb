# frozen_string_literal: true

module ActiveRecord
  module Type
    module Dm
      class VectorFloat64 < ActiveRecord::Type::Dm::Vector

        attr_reader :limit

        MIN_DIM = 1
        MAX_DIM = 65535

        def initialize(limit: nil)
          @limit = limit
        end

        def type
          :vector_float64
        end

      end
    end
  end
end

