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
            SELECT i.table_name AS table_name, i.index_name AS index_name, i.uniqueness,
              i.index_type, i.ityp_owner, i.ityp_name, i.parameters,
              i.tablespace_name AS tablespace_name,
              c.column_name AS column_name, atc.virtual_column
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

          result.map do |row|
            table_name = row["table_name"]
            index_name = row["index_name"]
            unique = row["uniqueness"]
            index_type = row["index_type"]
            if index_type == 'NORMAL'
              using_type = 'BTREE'
            else
              using_type = index_type
            end
            IndexDefinition.new(table_name, index_name, unique == 'UNIQUE', [], {}, nil, nil, index_type, using_type, nil)
          end.compact
        end

        private
          def data_source_sql(name = nil, type: nil)
            scope = quoted_scope(name, type: type)
            sql = "SELECT tab.table_name FROM all_tables tab, sysobjects obj"
            sql << " WHERE obj.name = tab.table_name"
            sql << " AND tab.owner = #{scope[:schema]}"
            sql << " AND tab.table_name = #{scope[:name]}" if scope[:name]
            sql << " AND obj.subtype$ = #{scope[:type]}" if scope[:type]
            sql
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
  end
end
