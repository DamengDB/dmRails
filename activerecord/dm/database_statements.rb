module ActiveRecord
  module ConnectionAdapters
    module Dm
      module DatabaseStatements
        def query(sql, name = nil) # :nodoc:
          if sql.start_with?("{data_source_sql}")
            sql = sql[17..-1]
            execute(sql, name).each(as: :array)
          elsif sql.start_with?("{table_comment}")
            sql = sql[15..-1]
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
          if binds == []
            execute_and_free(sql, name) do |result|
              ActiveRecord::Result.new(result.fields, result.each(as: :array)) if result and result.fields != []
            end
          else
            if without_prepared_statement?(binds)
              begin
                execute_and_free(sql, name) do |result|
                  ActiveRecord::Result.new(result.fields, result.each(as: :array)) if result and result.fields != []
                end
              rescue ::Dm::Error => e
                code = e.instance_variable_get(:@error_number) || 0
                if code == $ER_NEED_MORE_PARAM
                  exec_stmt_and_free(sql, name, binds, cache_stmt: prepare) do |_, result|
                    ActiveRecord::Result.new(result.fields,result.each(as: :array)) if result and result.fields != []
                  end
                else
                  raise e
                end
              end
            else
              exec_stmt_and_free(sql, name, binds, cache_stmt: prepare) do |_, result|
                ActiveRecord::Result.new(result.fields,result.each(as: :array)) if result and result.fields != []
              end
            end
          end
        end

        def begin_db_transaction
          execute "START TRANSACTION"
        end

        def begin_isolated_db_transaction(isolation)
          execute "START TRANSACTION ISOLATION LEVEL #{transaction_isolation_levels.fetch(isolation)}"
        end

        def commit_db_transaction #:nodoc:
          execute "COMMIT"
        end

        def exec_rollback_db_transaction #:nodoc:
          execute "ROLLBACK"
        end

        def insert(arel, name = nil, pk = nil, id_value = nil, sequence_name = nil, binds = [])
          sql, binds = to_sql_and_binds(arel, binds)
          value = exec_insert(sql, name, binds, pk, sequence_name)
          id_value || last_inserted_id(value)
        end
        alias create insert

        def exec_delete(sql, name = nil, binds = [])
          exec_stmt_and_free(sql, name, binds) { |stmt| stmt.affected_rows }
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
            rescue ::Dm::Error => e
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

    module DmMySQL
      module DatabaseStatements
        include Dm::DatabaseStatements
      end
    end
  end
end
