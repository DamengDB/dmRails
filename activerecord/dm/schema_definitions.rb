# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Dm
      module ColumnMethods
        def primary_key(name, type = :primary_key, **options)
          options[:auto_increment] = true if [:integer, :bigint].include?(type) && !options.key?(:default)
          super
        end

        def blob(*args, **options)
          args.each { |name| column(name, :blob, options) }
        end

        def tinyblob(*args, **options)
          args.each { |name| column(name, :tinyblob, options) }
        end

        def mediumblob(*args, **options)
          args.each { |name| column(name, :mediumblob, options) }
        end

        def longblob(*args, **options)
          args.each { |name| column(name, :longblob, options) }
        end

        def tinytext(*args, **options)
          args.each { |name| column(name, :tinytext, options) }
        end

        def mediumtext(*args, **options)
          args.each { |name| column(name, :mediumtext, options) }
        end

        def longtext(*args, **options)
          args.each { |name| column(name, :longtext, options) }
        end

        def json(*args, **options)
          args.each { |name| column(name, :json, options) }
        end

        def jsonb(*args, **options)
          args.each { |name| column(name, :jsonb, options) }
        end

        def unsigned_integer(*args, **options)
          args.each { |name| column(name, :unsigned_integer, options) }
        end

        def unsigned_bigint(*args, **options)
          args.each { |name| column(name, :unsigned_bigint, options) }
        end

        def unsigned_float(*args, **options)
          args.each { |name| column(name, :unsigned_float, options) }
        end

        def unsigned_decimal(*args, **options)
          args.each { |name| column(name, :unsigned_decimal, options) }
        end

        def timetz(*args, **options)
          args.each { |name| column(name, :timetz, options) }
        end

        def timestamptz(*args, **options)
          args.each { |name| column(name, :timestamptz, options) }
        end

        def timestampltz(*args, **options)
          args.each { |name| column(name, :timestampltz, options) }
        end
      end

      class TableDefinition < ActiveRecord::ConnectionAdapters::TableDefinition
        include ColumnMethods

        def new_column_definition(name, type, **options) # :nodoc:
          case type
          when :virtual
            type = options[:type]
          when :primary_key
            type = :bigint
            options[:auto_increment] = true
            options[:primary_key] = true
          when /\Aunsigned_(?<type>.+)\z/
            type = $~[:type].to_sym
            options[:unsigned] = true
          end

          super
        end

        private
        def aliased_types(name, fallback)
          fallback
        end
      end

      class Table < ActiveRecord::ConnectionAdapters::Table
        include ColumnMethods
      end
    end

    module DmMySQL
      module ColumnMethods
        include Dm::ColumnMethods

        def timestamptz(*args, **options)
          raise NoMethodError, "undefined Type 'timestamptz' for #{self}"
        end

        def timestampltz(*args, **options)
          raise NoMethodError, "undefined Type 'timestampltz' for #{self}"
        end
      end

      class TableDefinition < Dm::TableDefinition
        include DmMySQL::ColumnMethods
      end

      class Table < Dm::Table
      end
    end
  end
end
