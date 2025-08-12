module ActiveRecord
  module ConnectionAdapters
    module Dm
      module DatabaseStatements
        def query(sql, name = nil) # :nodoc:
          if sql == data_source_sql(type: "BASE TABLE") || sql == data_source_sql()
            execute(sql, name).each(as: :array)
          else
            execute(sql, name).to_a
          end
        end

        def array_query(sql, name = nil) # :nodoc:
          execute(sql, name).each(as: :array)
        end

        def execute(sql, name = nil)
          result = log(sql, name) do
            ActiveSupport::Dependencies.interlock.permit_concurrent_loads do
              @connection.query(sql)
            end
          end
        end

        def exec_query(sql, name = "SQL", binds = [], prepare: false)
          if without_prepared_statement?(binds)
            execute_and_free(sql, name) do |result|
              ActiveRecord::Result.new(result.fields, result.each(as: :array)) if result
            end
          else
            exec_stmt_and_free(sql, name, binds, cache_stmt: prepare) do |_, result|
              ActiveRecord::Result.new(result.fields,result.each(as: :array)) if result
            end
          end
        end

        def insert(arel, name = nil, pk = nil, id_value = nil, sequence_name = nil, binds = [])
          sql, binds = to_sql_and_binds(arel, binds)
          value = exec_insert(sql, name, binds, pk, sequence_name)
          id_value || last_inserted_id(value)
        end
        alias create insert

        def exec_insert(sql, name = nil, binds = [], pk = nil, sequence_name = nil)
          sql, binds = sql_for_insert(sql, pk, binds)
          exec_query(sql, name, binds)
        end

        def exec_delete(sql, name = nil, binds = [])
          if without_prepared_statement?(binds)
            execute_and_free(sql, name) { @connection.affected_rows }
          else
            exec_stmt_and_free(sql, name, binds) { |stmt| stmt.affected_rows }
          end
        end
        alias :exec_update :exec_delete

        private

        def last_inserted_id(result)
          result = @connection.query("SELECT LAST_INSERT_ID | SCOPE_IDENTITY;")
          inserted_id = result.each(as: :array)[0][0]
        end

        def exec_stmt_and_free(sql, name, binds, cache_stmt: false)

          type_casted_binds = type_casted_binds(binds)

          log(sql, name, binds, type_casted_binds) do
            if cache_stmt
              cache = @statements[sql] ||= {
                stmt: @connection.prepare(sql)
              }
              stmt = cache[:stmt]
            else
              stmt = @connection.prepare(sql)
            end

            begin
              result = ActiveSupport::Dependencies.interlock.permit_concurrent_loads do
                stmt.execute(*type_casted_binds)
              end
            rescue Dm::Error => e
              if cache_stmt
                @statements.delete(sql)
              else
                stmt.close
              end
              raise e
            end

            ret = yield stmt, result
            result.free if result
            stmt.close unless cache_stmt
            ret
          end
        end
      end
    end
  end
end
