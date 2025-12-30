require "active_support/core_ext/string/strip"
require "active_record/connection_adapters/dm/utils"
require "active_record/connection_adapters/dm/column"
require "active_record/connection_adapters/dm/quoting"
require "active_record/connection_adapters/dm/type/blob"
require "active_record/connection_adapters/dm/type/timetz"
require "active_record/connection_adapters/statement_pool"
require "active_record/connection_adapters/abstract_adapter"
require "active_record/connection_adapters/dm/type_metadata"
require "active_record/connection_adapters/dm/schema_dumper"
require "active_record/connection_adapters/dm/schema_creation"
require "active_record/connection_adapters/dm/type/timestamptz"
require "active_record/connection_adapters/dm/type/timestampltz"
require "active_record/connection_adapters/dm/schema_statements"
require "active_record/connection_adapters/dm/schema_definitions"
require "active_record/connection_adapters/dm/database_statements"
require "active_record/connection_adapters/dm/explain_pretty_printer"

gem "dmRuby"
require "dm"

module ActiveRecord
  module ConnectionHandling
    def raise_argument_error(content)
      raise ArgumentError.new("parse_type is only allowed to be a string type or nil, and when it is a string type, it can only be one of DM, MYSQL or TSQL")
    end

    def dm_connection(config)
      config = config.symbolize_keys
      config[:flags] ||= 0

      parse_type = 'DM'

      if config[:database] != nil and config[:schema] == nil
        config[:schema] = config[:database]
      end

      if config.include?(:parse_type)
        if config[:parse_type] != nil and config[:parse_type].is_a?(String)
          content = "parse_type is only allowed to be a string type or nil, and when it is a string type, it can only be one of DM, MYSQL or TSQL"
          if ["DM", "MYSQL", "TSQL"].include?(config[:parse_type].upcase)
            parse_type = config[:parse_type].upcase
          else
            raise_argument_error(content)
          end
        elsif config[:parse_type] != nil
          raise_argument_error(content)
        end
      end

      client = Dm::Client.new(config)
      if parse_type == 'MYSQL' or parse_type == 'TSQL'
        client.query("SP_SET_SESSION_PARSE_TYPE('#{parse_type}')")
      end
      if parse_type == 'MYSQL'
        ConnectionAdapters::DmMySQLAdapter.new(client, logger, nil, config)
      else
        ConnectionAdapters::DmAdapter.new(client, logger, nil, config)
      end
    rescue ::Dm::Error => error
      if error.message.include?("Unknown database")
        raise ActiveRecord::NoDatabaseError
      else
        raise
      end
    end
  end

  module ConnectionAdapters
    class DmAdapter < AbstractAdapter
      ADAPTER_NAME = "Dm".freeze
      include Dm::Quoting
      include Dm::SchemaStatements
      include Dm::DatabaseStatements

      FIXED_NLS_PARAMETERS = {
        nls_date_format: "YYYY-MM-DD HH24:MI:SS",
        nls_timestamp_format: "YYYY-MM-DD HH24:MI:SS:FF6"
      }

      NATIVE_DATABASE_TYPES = {
        primary_key: "bigint PRIMARY KEY auto_increment",
        string:      { name: "varchar", limit: 255 },
        text:        { name: "text", limit: 65535 },
        integer:     { name: "integer" },
        float:       { name: "float" },
        decimal:     { name: "decimal" },
        datetime:    { name: "datetime" },
        timestamp:   { name: "timestamp" },
        timetz:      { name: "time with time zone" },
        timestamptz: { name: "timestamp with time zone" },
        timestampltz: { name: "timestamp with local time zone" },
        time:        { name: "time" },
        date:        { name: "date" },
        blob:        { name: "blob" },
        binary:      { name: "binary"},
        boolean:     { name: "bit"},
        json:        { name: "json" },
        jsonb:       { name: "jsonb" },
        varchar:     { name: "varchar" },
        bigint:      { name: "bigint" },
      }

      DM_TYPE_DICT = {
        decimal:      ["decimal", "number", "numeric", "dec"],
        varchar:      ["varchar", "varchar2", "char", "character", "rowid"],
        date:         ["date"],
        time:         ["time"],
        datetime:     ["datetime", "timestamp"],
        timetz:       ["time with time zone"],
        timestamptz:  ["timestamp with time zone", "datetime with time zone"],
        timestampltz: ["timestamp with local time zone"],
        integer:      ["integer", "int", "pls_integer", "tinyint", "smallint", "byte"],
        binary:       ["binary", "varbinary", "image", "longvarbinary", "raw"],
        float:        ["float", "real"],
        double:       ["double", "double precision"],
        boolean:      ["bool", "bit"],
        text:         ["text", "long", "longvarchar", "clob"],
        blob:         ["blob"],
        json:         ["json", "jsonb"]
      }

      def initialize(connection, logger, connection_options, config)
        super(connection, logger, config)
        @statements = StatementPool.new(self.class.type_cast_config_to_integer(config[:statement_limit]))
        @prepared_statements = false unless config.key?(:prepared_statements)
        @type_map = Type::HashLookupTypeMap.new
        initialize_type_map(type_map)
      end

      def schema_creation # :nodoc:
        Dm::SchemaCreation.new self
      end

      class StatementPool < ConnectionAdapters::StatementPool
        private
        def dealloc(stmt)
          stmt.close
        end
      end

      def connect
        @connection = Dm::Client.new(@config)
      end

      def reconnect
        @lock.synchronize do
          @connection&.close
          @connection = nil
          connect
        end
      end

      def arel_visitor
        Arel::Visitors::Dm.new(self)
      end

      def supports_foreign_keys?
        true
      end

      def supports_datetime_with_precision?
        true
      end

      def supports_explain?
        true
      end

      class << self
        def dbconsole(config, options = {})
          dm_config = config.configuration_hash

          args = {
            host: "--host",
            port: "--port",
            username: "--user",
            schema: "--schema",
            encoding: 1,
          }.filter_map { |opt, arg| "#{arg}=#{dm_config[opt]}" if dm_config[opt] }

          if dm_config[:password] && options[:include_password]
            args << "--password=#{dm_config[:password]}"
          elsif dm_config[:password] && !dm_config[:password].to_s.empty?
            args << "-p"
          end

          args << config.database

          find_cmd_and_exec("dm", config.database)
        end
      end

      def version #:nodoc:
        @version ||= Version.new(version_string)
      end

      def native_database_types
        NATIVE_DATABASE_TYPES
      end

      def execute_and_free(sql, name = nil) # :nodoc:
        yield execute(sql, name)
      end

      def commit_db_transaction # :nodoc:
        execute("COMMIT")
      end

      def exec_rollback_db_transaction # :nodoc:
        execute("ROLLBACK")
      end

      def empty_insert_statement_value(primary_key = nil) # :nodoc:
        sql = "DEFAULT VALUES"
        sql
      end

      def explain(arel, binds = [])
        sql = "EXPLAIN FOR #{to_sql(arel, binds)}"
        start   = Concurrent.monotonic_time
        result  = exec_query(sql, "EXPLAIN", binds)
        elapsed = Concurrent.monotonic_time - start

        Dm::ExplainPrettyPrinter.new.pp(result, elapsed)
      end

      def recreate_database(name, options = {})
        drop_database(name)
        sql = create_database(name, options)
        reconnect!
        sql
      end

      def current_database
        query_value("SELECT SYS_CONTEXT('userenv', 'current_schema') FROM DUAL", "SCHEMA")
      end

      def table_comment(table_name) # :nodoc:
        scope = quoted_scope(table_name)

        result = array_query(<<~SQL, "SCHEMA").presence
          SELECT COMMENTS 
          FROM DBA_TAB_COMMENTS
          WHERE OWNER = #{scope[:schema]}
          AND TABLE_NAME = #{scope[:name]}
        SQL
        result.map(&:first)[0]
      end

      def temporary_table?(name)
        scope = quoted_scope(name)

        result = array_query(<<~SQL, "SCHEMA").presence
          SELECT TEMPORARY 
          FROM ALL_TABLES
          WHERE OWNER = #{scope[:schema]}
          AND TABLE_NAME = #{scope[:name]}
        SQL
        result.map(&:first)[0]
      end

      def change_table_comment(table_name, comment_or_changes)
        comment = extract_new_comment_value(comment_or_changes)
        comment = "" if comment.nil?
        execute("COMMENT ON TABLE #{quote_table_name(table_name)} IS '#{quote_string(comment)}'")
      end

      def rename_table(table_name, new_name, **options)
        if new_name.to_s.length > 128
          raise(ActiveRecordError, "New table name length exceeds the limit")
        end
        execute "ALTER TABLE #{quote_table_name(table_name)} RENAME TO #{quote_table_name(new_name)}"
      end

      def drop_table(table_name, **options)
        schema_cache.clear_data_source_cache!(table_name.to_s)
        execute "DROP TABLE#{' IF EXISTS' if options[:if_exists]} #{quote_table_name(table_name)}#{' CASCADE' if options[:force] == :cascade}"
      end

      def rename_index(table_name, old_name, new_name)
        validate_index_length!(table_name, new_name)

        schema, = extract_schema_qualified_name(table_name)
        execute "ALTER INDEX #{quote_table_name(schema) + '.' if schema}#{quote_column_name(old_name)} RENAME TO #{quote_table_name(new_name)}"
      end

      def remove_index(table_name, options={})
        index_name = index_name_for_remove(table_name, options)
        execute "DROP INDEX #{quote_column_name(index_name)}"
      end

      $ER_VIOLATE_UNIQUE_CONSTRAINT        = -6602
      $ER_NEED_MORE_PARAM                  = -6804

      def analyse_exception(exception)
        if exception.nil?
          return exception
        end

        message = exception.message
        if message.start_with?('[CODE:')
          code = message.match(/\[CODE:(-?\d+)\]/)[1].to_i
          return code
        else
          return exception
        end
      end

      def translate_exception(exception, message:, sql:, binds:)
        code = analyse_exception(exception)
        if code.is_a?(Integer)
          case code
          when $ER_VIOLATE_UNIQUE_CONSTRAINT
            RecordNotUnique.new(message, sql: sql, binds: binds)
          when $ER_NEED_MORE_PARAM
            ::Dm::Error.new(message, code)
          else
            super
          end
        else
          super
        end
      end

      def change_column_default(table_name, column_name, default_or_changes) # :nodoc:
        default = extract_new_default_value(default_or_changes)
        execute "ALTER TABLE #{quote_table_name(table_name)} ALTER COLUMN #{quote_column_name(column_name)} SET DEFAULT #{quote(default)}"
      end

      def build_change_column_default_definition(table_name, column_name, default_or_changes) # :nodoc:
        column = column_for(table_name, column_name)
        return unless column

        default = extract_new_default_value(default_or_changes)
        ChangeColumnDefaultDefinition.new(column, default)
      end

      def change_column_null(table_name, column_name, null, default = nil) #:nodoc:
        column = column_for(table_name, column_name)

        unless null || default.nil?
          execute("UPDATE #{quote_table_name(table_name)} SET #{quote_column_name(column_name)}=#{quote(default)} WHERE #{quote_column_name(column_name)} IS NULL")
        end

        change_column table_name, column_name, column.sql_type, null: null
      end

      def change_column_comment(table_name, column_name, comment_or_changes) # :nodoc:
        comment = extract_new_comment_value(comment_or_changes)
        if comment.is_a?(String)
          comment_str = comment.gsub(/'/, "''")
        else
          comment_str = comment.comment_text.gsub(/'/, "''")
        end
        comment_str = "'" + comment_str + "'"
        execute("COMMENT ON COLUMN #{quote_table_name(table_name)}.#{quote_column_name(column_name)} IS #{comment_str}")
      end

      def change_column(table_name, column_name, type, **options) # :nodoc:
        if options.has_key?(:comment) and options[:comment] != nil
          if options[:comment].is_a?(String)
            comment_str = options[:comment]
          else
            comment_str = options[:comment].to_s
          end
          change_column_comment(table_name, column_name, comment_str)
        end
        execute("ALTER TABLE #{quote_table_name(table_name)} #{change_column_for_alter(table_name, column_name, type, **options)}")
      end

      def bind_string(name, value)
        ActiveRecord::Relation::QueryAttribute.new(name, value, Type::String.new)
      end

      def build_change_column_definition(table_name, column_name, type, **options) # :nodoc:
        column = column_for(table_name, column_name)
        type ||= column.sql_type

        unless options.key?(:default)
          options[:default] = column.default
        end

        unless options.key?(:null)
          options[:null] = column.null
        end

        unless options.key?(:comment)
          options[:comment] = column.comment
        end

        if options[:collation] == :no_collation
          options.delete(:collation)
        else
          options[:collation] ||= column.collation
        end

        td = create_table_definition(table_name)
        cd = td.new_column_definition(column.name, type, **options)
        ChangeColumnDefinition.new(cd, column.name)
      end

      def rename_column(table_name, column_name, new_column_name)
        execute("ALTER TABLE #{quote_table_name(table_name)} RENAME COLUMN #{quote_table_name(column_name)} TO #{quote_table_name(new_column_name)}")
        rename_column_indexes(table_name, column_name, new_column_name)
      end

      def add_index(table_name, column_name, options = {}) #:nodoc:
        index_name, index_type, index_columns, uniques, index_algorithm, index_using, comment = add_index_options(table_name, column_name, options)
        type = nil
        if index_type == "NORMAL" or index_type == "VIRTUAL"
          if options[:unique]
            type = "UNIQUE"
          else
            type = nil
          end
        elsif index_type == "CLUSTER" and options[:unique]
          type = "CLUSTER UNIQUE"
        else
          type = index_type
        end
        sql = "CREATE #{type} INDEX #{quote_column_name(index_name)} ON #{quote_table_name(table_name)} (#{index_columns}) #{index_algorithm}"
        execute add_sql_comment!(sql, comment)
      end

      def add_sql_comment!(sql, comment) # :nodoc:
        sql << " COMMENT #{quote(comment)}" if comment.present?
        sql
      end

      def build_insert_sql(insert) # :nodoc:
        sql = +"INSERT #{insert.into} #{insert.values_list}"

        if insert.skip_duplicates?
          no_op_column = quote_column_name(insert.keys.first)
          sql << " ON DUPLICATE KEY UPDATE #{no_op_column}=#{no_op_column}"
        elsif insert.update_duplicates?
          sql << " ON DUPLICATE KEY UPDATE "
          sql << insert.updatable_columns.map { |column| "#{column}=VALUES(#{column})" }.join(",")
        end

        sql
      end

      def column_definitions(table_name)
        (owner, desc_table_name) = extract_schema_qualified_name(table_name)
        if (owner != nil)
          owner = "\'#{owner.upcase}\'"
        else
          owner = "SYS_CONTEXT('userenv', 'current_schema')"
        end
        query(<<-SQL, "SCHEMA")
        SELECT cols.column_name AS "name",
                CASE LOWER(cols.data_type)  WHEN 'blob' THEN
                CASE syscol.scale
                WHEN 0x4000 THEN 'jsonb'
                WHEN 0x2000 THEN 'json'
                ELSE LOWER(cols.data_type) 
                END
                ELSE LOWER(cols.data_type) END AS "sql_type",
                 cols.data_default as "data_default", LOWER(cols.nullable) as "nullable",
                 cols.data_type_owner AS "sql_type_owner", CAST(cols.DATA_PRECISION AS INT) AS "precision",
                 syscol.LENGTH$ AS "limit", syscol.scale AS "scale",
                 comments.comments AS "column_comment"
            FROM all_tab_cols cols, all_col_comments comments, syscolumns syscol, sysobjects sysobj
            WHERE cols.table_name = '#{desc_table_name}'
             AND cols.owner      = #{owner}
             AND cols.hidden_column = 'NO'
             AND cols.owner = comments.schema_name
             AND cols.table_name = comments.table_name
             AND cols.column_name = comments.column_name
             AND cols.column_name = syscol.name
             AND sysobj.schid = (SELECT id FROM SYSOBJECTS WHERE NAME = #{owner} AND TYPE$='SCH')
             AND sysobj.name = cols.table_name
             AND syscol.id = sysobj.id
        SQL
      end

      def foreign_keys(table_name)
        (_owner, desc_table_name) = extract_schema_qualified_name(table_name)
        fk_info = query(<<~SQL.squish, "SCHEMA")
          SELECT r.table_name as "to_table"
                ,rc.column_name as "references_column"
                ,cc.column_name as "column_name"
                ,c.constraint_name as "name"
                ,c.delete_rule as "delete_rule"
            FROM all_constraints c, all_cons_columns cc,
                 all_constraints r, all_cons_columns rc
           WHERE c.owner = SYS_CONTEXT('userenv', 'current_schema')
             AND c.table_name = '#{desc_table_name}'
             AND c.constraint_type = 'R'
             AND cc.owner = c.owner
             AND cc.constraint_name = c.constraint_name
             AND r.constraint_name = c.r_constraint_name
             AND r.owner = c.owner
             AND rc.owner = r.owner
             AND rc.constraint_name = r.constraint_name
             AND rc.position = cc.position
          ORDER BY name, to_table, column_name, references_column
        SQL

        fk_info.map do |row|
          options = {
            column: row["column_name"],
            name: row["name"],
            primary_key: row["references_column"]
          }
          options[:on_delete] = extract_foreign_key_action(row["delete_rule"])
          ActiveRecord::ConnectionAdapters::ForeignKeyDefinition.new(table_name, row["to_table"], options)
        end
      end

      def extract_foreign_key_action(specifier) # :nodoc:
        case specifier
        when "CASCADE"; :cascade
        when "SET NULL"; :nullify
        end
      end

      def table_options(table_name) # :nodoc:
        options = {}
        comment = table_comment(table_name)
        options[:comment] = comment if !comment.nil?

        if temporary_table?(table_name) == 'Y'
          options[:temporary] = true
        end
        options
      end

      def primary_keys(table_name) # :nodoc:
        (owner, desc_table_name) = extract_schema_qualified_name(table_name)
        if (owner == nil)
          owner = "SYS_CONTEXT('userenv', 'current_schema')"
        else
          owner = "'#{owner}'"
        end
        result = array_query(<<~SQL.squish, "SCHEMA")
          SELECT cc.column_name
            FROM all_constraints c, all_cons_columns cc
           WHERE c.owner = #{owner}
             AND c.table_name = \'#{desc_table_name}\'
             AND c.constraint_type = 'P'
             AND cc.owner = c.owner
             AND cc.constraint_name = c.constraint_name
             order by cc.position
        SQL
        if result.length == 1
          return result.first
        else
          return result.flatten
        end
      end

      def columns_for_distinct(columns, orders) # :nodoc:
        order_columns = orders.reject(&:blank?).map { |s|
          s = visitor.compile(s) unless s.is_a?(String)
          # remove any ASC/DESC modifiers
          s.gsub(/\s+(ASC|DESC)\s*?/i, "")
        }.reject(&:blank?).map.with_index { |column, i|
          "FIRST_VALUE(#{column}) OVER (PARTITION BY #{columns} ORDER BY #{column}) AS alias_#{i}__"
        }
        (order_columns << super).join(", ")
      end

      def default_index_type?(index) # :nodoc:
        index.using == :btree || super
      end

      def quote_string(string)
        string.gsub(/'/, "''")
      end

      def new_column_from_field(table_name, field)
        precision, limit, scale, sql_type = field["precision"], field["limit"], field["scale"], ''
        if DM_TYPE_DICT[:decimal].include?(field["sql_type"])
          if scale == 0
            if precision == nil
              field["sql_type"] = "decimal"
            else
              field["sql_type"] = "decimal(#{(limit || 38).to_i})"
            end
          else
            field["sql_type"] = "decimal(#{(limit || 38).to_i}, #{(scale || 0).to_i})"
          end
          sql_type = "decimal"
        elsif DM_TYPE_DICT[:varchar].include?(field["sql_type"])
            field["sql_type"] = "varchar(#{(limit || 256).to_i})"
            field["scale"] = nil
            sql_type = "varchar"
        elsif DM_TYPE_DICT[:binary].include?(field["sql_type"])
          field["sql_type"] = "binary(#{(limit || 8188).to_i})"
          field["scale"] = nil
          sql_type = "binary"
        elsif DM_TYPE_DICT[:datetime].include?(field["sql_type"])
          field["sql_type"] = "datetime(#{(scale || 6).to_i})"
          field["limit"] = nil
          sql_type = "datetime"
        elsif DM_TYPE_DICT[:timetz].include?(field["sql_type"])
          field["sql_type"] = "time(#{(scale || 6).to_i}) with time zone"
          field["limit"] = nil
          sql_type = 'time with time zone'
        elsif DM_TYPE_DICT[:timestamptz].include?(field["sql_type"])
          field["sql_type"] = "datetime(#{(scale || 6).to_i}) with time zone"
          field["limit"] = nil
          sql_type = "datetime with time zone"
        elsif DM_TYPE_DICT[:timestampltz].include?(field["sql_type"])
          field["sql_type"] = "datetime(#{(scale || 6).to_i}) with time zone"
          field["limit"] = nil
          sql_type = "datetime with local time zone"
          mask = (1 << 3) - 1
          field["scale"] = field["scale"] & mask
        elsif DM_TYPE_DICT[:time].include?(field["sql_type"])
          field["sql_type"] = "time(#{(scale || 6).to_i}) with time zone"
          field["limit"] = nil
          sql_type = "time"
        elsif DM_TYPE_DICT[:double].include?(field["sql_type"])
          field["sql_type"] = "double(#{(scale || 6).to_i})"
          sql_type = "double"
        end

        re_define_flag = false
        if DM_TYPE_DICT[:integer].include?(field["sql_type"])
          sql_type = field["sql_type"] = "integer"
          re_define_flag = true
        elsif DM_TYPE_DICT[:binary].include?(field["sql_type"])
          sql_type = field["sql_type"] = "binary"
          re_define_flag = true
        elsif DM_TYPE_DICT[:float].include?(field["sql_type"])
          sql_type = field["sql_type"] = "float"
          re_define_flag = true
        elsif DM_TYPE_DICT[:boolean].include?(field["sql_type"])
          sql_type = field["sql_type"] = "boolean"
          re_define_flag = true
        elsif DM_TYPE_DICT[:text].include?(field["sql_type"])
          sql_type = field["sql_type"] = "text"
          re_define_flag = true
        elsif DM_TYPE_DICT[:blob].include?(field["sql_type"])
          sql_type = field["sql_type"] = "blob"
          re_define_flag = true
        elsif DM_TYPE_DICT[:date].include?(field["sql_type"])
          field["sql_type"] = "date"
          re_define_flag = true
        elsif DM_TYPE_DICT[:json].include?(field["sql_type"])
          field["sql_type"] = sql_type = field["sql_type"]
          re_define_flag = true
        end

        if re_define_flag
          field["limit"] = nil
          field["scale"] = nil
        end

        if sql_type == ''
          sql_type = field["sql_type"]
        end

        if field["sql_type_owner"] != nil
          field["sql_typesql_type"] = field["sql_type_owner"] + "." + field["sql_type"]
        end

        is_virtual = field["virtual_column"] == "YES"

        if field["data_default"] && !is_virtual
          field["data_default"].sub!(/^(.*?)\s*$/, '\1')

          field["data_default"].sub!(/^'(.*)'$/m, '\1')
          field["data_default"] = nil if /^(null|empty_[bc]lob\(\))$/i.match?(field["data_default"])
          field["data_default"] = false if field["data_default"] == "N"
          if ["float", "decimal", "double"].include?(sql_type)
            field["data_default"] = field["data_default"].to_f
          elsif "integer" == sql_type
            field["data_default"] = field["data_default"].to_i
          end
        end

        type_metadata = fetch_type_metadata(sql_type)
        default_value = extract_value_from_default(field["data_default"])
        type_metadata.instance_variable_set("@sql_type", field["sql_type"])
        type_metadata.instance_variable_set("@precision", field["precision"])
        type_metadata.instance_variable_set("@limit", field["limit"])
        if field["scale"] == 0
          type_metadata.instance_variable_set("@scale", nil)
        else
          type_metadata.instance_variable_set("@scale", field["scale"])
        end
        default_value = nil if is_virtual
        if default_value != nil
          default_value, default_function = extract_default_value(sql_type, default_value)
        end
        version = Rails.version
        if version < "6.0"
          result = new_column(field["name"], default_value, type_metadata, field["nullable"] == "y", table_name, comment: field["column_comment"], default_function: default_function)
        else
          result = Dm::Column.new(field["name"], default_value, type_metadata, field["nullable"] == "y", comment: field["column_comment"], default_function: default_function)
        end
        result
      end

      def extract_value_from_default(default)
        case default
        when String
          default.gsub("''", "'")
        else
          default
        end
      end

      def extract_default_value(sql_type, default_value)
        default, default_function = default_value, nil

        time_type_dict = ["time", "datetime", "datetime with time zone", "datetime with local time zone"]

        if time_type_dict.include?(sql_type) and (/\ACURRENT_TIMESTAMP(?:\([0-6]?\))?\z/i.match?(default) or /\ACURRENT_TIME(?:\([0-6]?\))?\z/i.match?(default))
          default, default_function = nil, default
        end
        return default, default_function
      end

      def each_hash(result) # :nodoc:
        if block_given?
          result.each(as: :hash, symbolize_keys: true) do |row|
            yield row
          end
        else
          to_enum(:each_hash, result)
        end
      end

      def active?
        @connection.ping
      end

      def supports_index_sort_order?
        true
      end

      def supports_comments?
        true
      end

      def supports_comments_in_create?
        false
      end

      def supports_partial_index?
        true
      end

      def supports_insert_on_duplicate_skip?
        true
      end

      def supports_insert_on_duplicate_update?
        true
      end

      def supports_views?
        true
      end

      def change_column_for_alter(table_name, column_name, type, **options)
        cd = build_change_column_definition(table_name, column_name, type, **options)
        schema_creation.accept(cd)
      end

      def rename_column_for_alter(table_name, column_name, new_column_name)
        return rename_column_sql(table_name, column_name, new_column_name)
      end

      def build_statement_pool
        StatementPool.new(self.class.type_cast_config_to_integer(@config[:statement_limit]))
      end

      private
        def initialize_type_map(m)
          super
          register_class_with_precision m, "time", Type::Time
          register_class_with_limit m, "datetime", Type::DateTime
          m.register_type "decimal",       Type::Decimal.new
          m.register_type "varchar",       Type::String.new
          m.register_type "binary",        Type::Binary.new
          m.register_type "tinytext",      Type::Text.new(limit: 2**8 - 1)
          m.register_type "tinyblob",      Type::Binary.new(limit: 2**8 - 1)
          m.register_type "text",          Type::Text.new(limit: 2**16 - 1)
          m.register_type "float",         Type::Float.new(limit: 24)
          m.register_type "double",        Type::Float.new(limit: 53)
          m.register_type "bigint",        Type::BigInteger.new
          m.register_type "integer",       Type::Integer.new
          m.register_type "bit",           Type::Boolean.new
          m.register_type "boolean",       Type::Boolean.new
          m.register_type "date",          Type::Date.new
          m.register_type "json",          Type::Json.new
          m.register_type "jsonb",         Type::Json.new
          m.register_type "time",          Type::Time.new
          m.register_type "blob",          Type::Dm::Blob.new
          m.register_type "time with time zone", Type::Dm::TimeTz.new
          m.register_type "datetime with time zone", Type::Dm::TimestampTz.new
          m.register_type "datetime with local time zone", Type::Dm::TimestampLtz.new
        end

        version = Rails.version

        if version < "6.0"
          def create_table_definition(*args) # :nodoc:
            Dm::TableDefinition.new(*args)
          end
  
          class DmJson < Type::Internal::AbstractJson # :nodoc:
          end
  
          class DmJsonb < Type::Internal::AbstractJson # :nodoc:
          end
        else
          def create_table_definition(*args, **options)
            Dm::TableDefinition.new(self, *args, **options)
          end
        end
    end

    class DmMySQLAdapter < DmAdapter

      ADAPTER_NAME = "DmMySQL".freeze

      include DmMySQL::Quoting
      include DmMySQL::DatabaseStatements

      def arel_visitor
        Arel::Visitors::DmMySQL.new(self)
      end

      def empty_insert_statement_value(primary_key = nil) # :nodoc:
        sql = 'VALUES()'
        sql
      end

      def explain(arel, binds = [])
        sql = "EXPLAIN  #{to_sql(arel, binds)}"
        start   = Concurrent.monotonic_time
        result  = exec_query(sql, "EXPLAIN", binds)
        elapsed = Concurrent.monotonic_time - start

        DmMySQL::ExplainPrettyPrinter.new.pp(result, elapsed)
      end

      def supports_explain?
        false
      end

      def schema_creation # :nodoc:
        DmMySQL::SchemaCreation.new self
      end

      def change_table_comment(table_name, comment_or_changes)
        comment = extract_new_comment_value(comment_or_changes)
        comment = "" if comment.nil?
        execute("ALTER TABLE #{quote_table_name(table_name)} COMMENT '#{quote_string(comment)}'")
      end

      def change_column_comment(table_name, column_name, comment_or_changes) # :nodoc:
        comment = extract_new_comment_value(comment_or_changes)
        change_column table_name, column_name, nil, comment: comment
      end

      def change_column(table_name, column_name, type, **options) # :nodoc:
        execute("ALTER TABLE #{quote_table_name(table_name)} #{change_column_for_alter(table_name, column_name, type, **options)}")
      end

      def remove_index(table_name, options={})
        index_name = index_name_for_remove(table_name, options)
        schema, tabname = extract_schema_qualified_name(table_name)
        prefix_str = tabname + "$"
        if index_name.start_with?(prefix_str)
          len = table_name.size + 1
          index_name = index_name[len..-1]
          execute "DROP INDEX #{quote_column_name(index_name)} ON #{quote_table_name(table_name)}"
        else
          raise "Cannot delete indexes not created in MySQL syntax mode"
        end
      end

      private
        version = Rails.version
        if version < "6.0"
          def create_table_definition(*args) # :nodoc:
            DmMySQL::TableDefinition.new(*args)
          end
        else
          def create_table_definition(*args, **options)
            DmMySQL::TableDefinition.new(self, *args, **options)
          end
        end

    end
  end
end
