# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Dm
      class Column < ConnectionAdapters::Column
        delegate :virtual, to: :sql_type_metadata, allow_nil: true

        def virtual?
          virtual
        end

        def auto_increment?
          extra == "auto_increment"
        end
      end
    end
  end
end
