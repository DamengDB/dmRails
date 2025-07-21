# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Dm
      class SchemaCreation < AbstractAdapter::SchemaCreation

        private
          def add_column_options!(sql, options)
            sql << " DEFAULT #{quote_default_expression(options[:default], options[:column])}" if options_include_default?(options)
            # must explicitly check for :null to allow change_column to work on migrations
            if options[:null] == false
              sql << " NOT NULL"
            end
            if options[:primary_key] == true
              sql << " PRIMARY KEY"
            end
            if options[:auto_increment] == true
              sql << " AUTO_INCREMENT"
            end
            sql
          end

          def visit_ChangeColumnDefinition(o)
            change_column_sql = "MODIFY #{accept(o.column)}"
            if column_options(o.column)[:first]
              change_column_sql << "FIRST"
            elsif column_options(o.column)[:after]
              change_column_sql << " AFTER #{quote_column_name(column_options(o.column)[:after])}"
            end
            puts change_column_sql
            change_column_sql
          end

      end
    end
  end
end
