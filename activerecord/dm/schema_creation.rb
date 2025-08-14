# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Dm
      class SchemaCreation < AbstractAdapter::SchemaCreation

        private
          def add_column_options!(sql, options)
            sql << " DEFAULT #{quote_default_expression(options[:default], options[:column])}" if !options[:default].nil?
            if options[:null] == false
              sql << " NOT NULL"
            elsif options[:null] == true
              sql << " NULL"
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
            change_column_sql
          end

      end
    end
  end
end
