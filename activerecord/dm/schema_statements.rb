module ActiveRecord
  module ConnectionAdapters
    module Dm
      module SchemaStatements # :nodoc:
        def indexes(table_name, name=nil)
          if name
            ActiveSupport::Deprecation.warn(<<-MSG.squish)
              Passing name to #indexes is deprecated without replacement.
            MSG
          end
          schema, tabname = extract_schema_qualified_name(table_name)
          if (schema == nil)
            schema = "SYS_CONTEXT('userenv', 'current_schema')"
          else
            schema = "'#{schema}'"
          end
          result = query(<<-SQL, "SCHEMA")
            SELECT i.table_name AS "table_name", c.descend AS "order", i.index_name AS "index_name", i.uniqueness AS "uniqueness",
              i.index_type AS "index_type", i.ityp_owner AS "ityp_owner", i.ityp_name AS "ityp_name", i.parameters AS "parameters",
              i.tablespace_name AS "tablespace_name",
              c.column_name AS "column_name", atc.virtual_column AS "virtual_column"
            FROM all_indexes i
              JOIN all_ind_columns c ON c.index_name = i.index_name AND c.index_owner = i.owner
              LEFT OUTER JOIN all_tab_cols atc ON i.table_name = atc.table_name AND
                c.column_name = atc.column_name AND i.owner = atc.owner AND atc.hidden_column = 'NO'
            WHERE i.owner = SYS_CONTEXT('userenv', 'current_schema')
               AND i.table_owner = SYS_CONTEXT('userenv', 'current_schema')
               AND i.table_name = '#{tabname}'
               AND i.owner = #{schema}
               AND i.index_type != 'VIRTUAL'
               AND NOT EXISTS (SELECT uc.index_name FROM all_constraints uc
                WHERE uc.index_name = i.index_name AND uc.owner = i.owner)
            ORDER BY i.index_name, c.column_position
          SQL

          version = Rails.version

          id = 0
          pk_arr = []
          order_dict = {}
          reflect_dict = {}
          result.map do |row|
            if reflect_dict.include?(row['index_name'])
              sort_id = reflect_dict[row['index_name']]
              pk_temp = pk_arr[sort_id]
              if pk_temp['column_name'].is_a?(String)
                new_array = [pk_temp['column_name'], row['column_name']]

                pk_arr[sort_id]['column_name'] = new_array
              else
                pk_arr[sort_id]['column_name'].push(row['column_name'])
              end

              pk_arr[sort_id]['order'][row['column_name']] = row['order']

            else
              pk_arr.push(row)
              reflect_dict.store(row['index_name'], id)
              pk_arr[id]['order'] = {pk_arr[id]['column_name'] => row['order']}
              id += 1
            end
          end
          pk_arr.map do |row|
            table_name = row["table_name"]
            index_name = row["index_name"]
            unique = row["uniqueness"]
            index_type = row["index_type"]
            columns = row['column_name']
            orders = row['order']
            if index_type == 'NORMAL'
              using_type = 'BTREE'
            else
              using_type = index_type
            end
            IndexDefinition.new(table_name, index_name, unique == 'UNIQUE', columns, orders: orders, type: index_type, using: using_type)
          end.compact
        end

        def type_to_sql(type, limit: nil, precision: nil, scale: nil, **) # :nodoc:

          type_str = type.to_s
          if ["timetz", "timestamptz", "timestampltz"].include?(type_str)
            if type_str == "timetz"
              return precision.nil? ? "time with time zone":"time(#{precision.to_i}) with time zone"
            elsif type_str == "timestamptz"
              return precision.nil? ? "timestamp with time zone":"timestamp(#{precision.to_i}) with time zone"
            elsif type_str == "timestampltz"
              return precision.nil? ? "timestamp with time zone":"timestamp(#{precision.to_i}) with local time zone"
            end
          end
          super
        end

        def quote_version_column(column_name)
          if @config[:parse_type] == "mysql"
            quote_sign = '`'
          else
            quote_sign = '"'
          end
          result = quote_sign + column_name + quote_sign
          result
        end

        def insert_versions_sql(versions)
          sm_table = quote_table_name(schema_migration.table_name)

          if versions.is_a?(Array)
            sql = +"INSERT INTO #{sm_table} (#{quote_version_column("version")}) VALUES\n"
            sql << versions.map { |v| "(#{quote(v)})" }.join(",\n")
            sql << ";\n\n"
            sql
          else
            "INSERT INTO #{sm_table} (#{quote_version_column("version")}) VALUES (#{quote(versions)});"
          end
        end

        def assume_migrated_upto_version(version, migrations_paths = nil)
          unless migrations_paths.nil?
            ActiveSupport::Deprecation.warn(<<~MSG.squish)
            Passing migrations_paths to #assume_migrated_upto_version is deprecated and will be removed in Rails 6.1.
          MSG
          end

          version = version.to_i
          sm_table = quote_table_name(schema_migration.table_name)

          migrated = migration_context.get_all_versions
          versions = migration_context.migrations.map(&:version)

          unless migrated.include?(version)
            execute "INSERT INTO #{sm_table} (#{quote_version_column("version")}) VALUES (#{quote(version)})"
          end

          inserting = (versions - migrated).select { |v| v < version }
          if inserting.any?
            if (duplicate = inserting.detect { |v| inserting.count(v) > 1 })
              raise "Duplicate migration #{duplicate}. Please renumber your migrations to resolve the conflict."
            end
            execute insert_versions_sql(inserting)
          end
        end

        private
          def data_source_sql(name = nil, type: nil)
            scope = quoted_scope(name, type: type)
            if type != nil
              if type == "VIEW"
                all_name = "all_views"
                col_name = "view_name"
              else type == "BASE TABLE"
                all_name = "all_tables"
                col_name = "table_name"
              end

              sql = "{data_source_sql}SELECT tab.#{col_name} FROM #{all_name} tab, sysobjects obj"
              sql << "\nWHERE obj.name = tab.#{col_name}"
              sql << "\nAND tab.owner = #{scope[:schema]}"
              sql << "\nAND obj.schid = (SELECT id FROM SYSOBJECTS WHERE NAME = #{scope[:schema]} AND TYPE$='SCH')"
              sql << "\nAND tab.#{col_name} = #{scope[:name]}" if scope[:name]
              sql << "\nAND obj.subtype$ = #{scope[:type]}" if scope[:type]
              sql
            else
              sql = "{data_source_sql}SELECT tab.table_name AS \"table_name\" FROM all_tables tab, sysobjects obj  "
              sql << "\nWHERE obj.name = tab.table_name"
              sql << "\nAND tab.owner = #{scope[:schema]}"
              sql << "\nAND tab.table_name = #{scope[:name]}" if scope[:name]
              sql << "\nAND obj.subtype$ = #{scope[:type]}" if scope[:type]
              sql << "\n UNION ALL"
              sql << "\nSELECT views.VIEW_NAME AS \"table_name\" FROM all_views views, sysobjects obj "
              sql << "\nWHERE obj.name = views.view_name"
              sql << "\nAND views.owner = #{scope[:schema]}"
              sql << "\nAND obj.schid = (SELECT id FROM SYSOBJECTS WHERE NAME = #{scope[:schema]} AND TYPE$='SCH')"
              sql << "\nAND views.view_name = #{scope[:name]}" if scope[:name]
              sql << "\nAND obj.subtype$ = #{scope[:type]}" if scope[:type]
              sql

            end
          end

          def add_index_length(quoted_columns, **options)
            lengths = options_for_index_columns(options[:length])
            quoted_columns.each do |name, column|
              quoted_columns[name] = "SUBSTR(#{column}, 1, #{lengths[name]})" if lengths[name].present?
            end
          end

          def add_options_for_index_columns(quoted_columns, **options)
            quoted_columns = add_index_length(quoted_columns, **options)
            super
          end

          def quoted_scope(name = nil, type: nil)
            schema, name = extract_schema_qualified_name(name)
            scope = {}
            if schema
              scope[:schema] = quote(schema)
            else
              scope[:schema] = "SYS_CONTEXT('userenv', 'current_schema')"
            end
            type = \
              case type
              when "BASE TABLE"
                "UTAB"
              when "VIEW"
                "VIEW"
              end
            scope[:name] = quote(name) if name
            scope[:type] = quote(type) if type
            scope
          end

          def extract_schema_qualified_name(string)
            schema, name = string.to_s.scan(/[^`.\s]+|`[^`]*`/)
            schema, name = nil, schema unless name
            [schema, name]
          end
      end
    end

    module DmMySQL
      module SchemaStatements
        include Dm::SchemaStatements

      end
    end
  end
end
