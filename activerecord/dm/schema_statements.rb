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
            schema = 'CURRENT_USER'
          else
            schema = "\'#{scheam}\'"
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
               AND i.table_name = \'#{tabname}\'
               AND i.owner = #{schema}
               AND NOT EXISTS (SELECT uc.index_name FROM all_constraints uc
                WHERE uc.index_name = i.index_name AND uc.owner = i.owner)
            ORDER BY i.index_name, c.column_position
          SQL

          version = Rails.version
          
          if version < "6.0"
            result.map do |row|
              table_name = row["table_name"]
              index_name = row["index_name"]
              unique = row["uniqueness"]
              index_type = row["index_type"]
              order = row["order"]
              if index_type == 'NORMAL'
                using_type = 'BTREE'
              else
                using_type = index_type
              end
              IndexDefinition.new(table_name, index_name, unique == 'UNIQUE', [], {}, nil, nil, index_type, using_type, nil)
            end.compact
          else
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
                pk_arr[0]['order'] = {pk_arr[0]['column_name'] => row['order']}
                reflect_dict.store(row['index_name'], id)
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
              sql << " WHERE obj.name = tab.#{col_name}"
              sql << " AND tab.owner = #{scope[:schema]}"
              sql << " AND tab.#{col_name} = #{scope[:name]}" if scope[:name]
              sql << " AND obj.subtype$ = #{scope[:type]}" if scope[:type]
              sql
            else
              sql = "{data_source_sql}SELECT tab.table_name AS \"table_name\" FROM all_tables tab, sysobjects obj  "
              sql << " WHERE obj.name = tab.table_name"
              sql << " AND tab.owner = #{scope[:schema]}"
              sql << " AND tab.table_name = #{scope[:name]}" if scope[:name]
              sql << " AND obj.subtype$ = #{scope[:type]}" if scope[:type]
              sql << "\n UNION ALL\n"
              sql << "SELECT views.VIEW_NAME AS \"table_name\" FROM all_views views, sysobjects obj "
              sql << " WHERE obj.name = views.view_name"
              sql << " AND views.owner = #{scope[:schema]}"
              sql << " AND views.view_name = #{scope[:name]}" if scope[:name]
              sql << " AND obj.subtype$ = #{scope[:type]}" if scope[:type]
              sql

            end
          end

          def add_index_length(quoted_columns, **options)
            lengths = options_for_index_columns(options[:length])
            quoted_columns.each do |name, column|
              column << "(#{lengths[name]})" if lengths[name].present?
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
              scope[:schema] = "CURRENT_USER"
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
