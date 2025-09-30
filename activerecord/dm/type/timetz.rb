# frozen_string_literal: true

module ActiveRecord
  module Type
    module Dm
      class TimeTz < ActiveRecord::Type::Time
        def type
          :timetz
        end

        class Data < DelegateClass(::Time) # :nodoc:
        end

        def serialize(value)
          case value = super
          when ::Time
            Data.new(value)
          else
            value
          end
        end
      end
    end
  end
end
