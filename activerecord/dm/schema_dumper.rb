module ActiveRecord
  module ConnectionAdapters
    module Dm
      module ColumnDumper # :nodoc:
        def prepare_column_options(column)
          spec = super
          spec[:unsigned] = "true" if column.unsigned?
          spec[:auto_increment] = "true" if column.auto_increment?

          if supports_virtual_columns? && column.virtual?
            spec[:as] = extract_expression_for_virtual_column(column)
            spec[:stored] = "true" if /\b(?:STORED|PERSISTENT)\b/.match?(column.extra)
            spec = { type: schema_type(column).inspect }.merge!(spec)
          end

          spec
        end

        def column_spec_for_primary_key(column)
          spec = super
          spec.delete(:auto_increment) if column.type == :integer && column.auto_increment?
          spec
        end

        def migration_keys
          super + [:unsigned]
        end

        private

          def default_primary_key?(column)
            super && column.auto_increment? && !column.unsigned?
          end

          def explicit_primary_key_default?(column)
            column.type == :integer && !column.auto_increment?
          end

          def schema_type(column)
            case column.sql_type
            when /\Atimestamp\b/
              :timestamp
            when "tinyblob"
              :blob
            else
              super
            end
          end

          def schema_precision(column)
            super unless /\A(?:date)?time(?:stamp)?\b/.match?(column.sql_type) && column.precision == 0
          end

          def schema_collation(column)
            if column.collation && table_name = column.table_name
              @table_collation_cache ||= {}
              column.collation.inspect if column.collation != @table_collation_cache[table_name]
            end
          end
      end
    end

    module DmMySQL
      module ColumnDumper
        include Dm::ColumnDumper
      end
    end
    
  end
end
