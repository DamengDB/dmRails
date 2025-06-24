module ActiveRecord
  module ConnectionAdapters
    module Dm
      class Name # :nodoc:
        SEPARATOR = "."
        attr_reader :schema, :identifier

        def initialize(schema, identifier)
          @schema, @identifier = unquote(schema), unquote(identifier)
        end

        def to_s
          parts.join SEPARATOR
        end

        def ==(o)
          o.class == self.class && o.parts == parts
        end
        alias_method :eql?, :==

        def hash
          parts.hash
        end

        protected

          def parts
            @parts ||= [@schema, @identifier].compact
          end

        private
          def unquote(part)
            if part && part.start_with?('"')
              part[1..-2]
            else
              part
            end
          end
      end

      module Utils # :nodoc:
        extend self

        def extract_schema_qualified_name(string)
          schema, table = string.scan(/[^".\s]+|"[^"]*"/)
          if table.nil?
            table = schema
            schema = 'CURRENT_USER'
          end
          Dm::Name.new(schema, table)
        end
      end
    end
  end
end
