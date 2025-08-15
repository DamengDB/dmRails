module ActiveRecord
  module ConnectionAdapters
    module Dm
      module Quoting # :nodoc:
        QUOTED_TRUE, QUOTED_FALSE = "1".freeze, "0".freeze

        def quote_column_name(name)
          version = Rails.version
          if $parse_type == "MYSQL"
            quote_sign = "`"
            dquote_sign = "``"
          else
            quote_sign = '"'
            dquote_sign = '""'
          end
          if version < "6.0"
            result = @quoted_column_names[name] ||= "#{quote_sign}#{super.gsub(quote_sign, dquote_sign)}#{quote_sign}".freeze
          else
            result = self.class.quoted_column_names[name] ||= "#{quote_sign}#{super.gsub(quote_sign, dquote_sign)}#{quote_sign}".freeze
          end
          result
        end

        def quote_table_name(name)
          if $parse_type == "MYSQL"
            dquote_sign = '`.`'
          else
            dquote_sign = '"."'
          end
          version = Rails.version
          if version < "6.0"
            result = @quoted_table_names[name] ||= super.gsub(".", dquote_sign).freeze
          else
            result = self.class.quoted_table_names[name] ||= super.gsub(".", dquote_sign).freeze
          end
          result
        end

        def quoted_true
          QUOTED_TRUE
        end

        def unquoted_true
          1
        end

        def quoted_false
          QUOTED_FALSE
        end

        def unquoted_false
          0
        end

        def quoted_date(value)
          if supports_datetime_with_precision?
            super
          else
            super.sub(/\.\d{6}\z/, "")
          end
        end

        def quoted_binary(value)
          "'#{value.hex}'"
        end
      end
    end
  end
end
