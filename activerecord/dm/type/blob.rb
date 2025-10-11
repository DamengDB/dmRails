# frozen_string_literal: true

module ActiveRecord
  module Type
    module Dm
      class Blob < ActiveRecord::Type::Binary
        def type
          :blob
        end
      end
    end
  end
end
