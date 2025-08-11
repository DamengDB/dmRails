module ActiveRecord
  module ConnectionAdapters
    module Dm
      module DatabaseStatements
        def query(sql, name = nil) # :nodoc:
          if sql == data_source_sql(type: "BASE TABLE")
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
          pk_name = pk
          ast = arel.instance_variable_get("@ast")
          relation = ast.instance_variable_get("@relation")
          table_name = relation.instance_variable_get("@name")
          sql, binds = to_sql_and_binds(arel, binds)
          value = exec_insert(sql, name, binds, pk, sequence_name)
          id_value || last_inserted_id(value, table_name, pk_name)
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

        def last_inserted_id(result, table_name, pk_name)
          last_idresult = @connection.last_id
          rowid_dict = {'A'=> 0, 'B'=> 1, 'C'=> 2, 'D'=> 3, 'E'=> 4, 'F'=> 5, 'G'=> 6, 'H'=> 7, 'I'=> 8, 'J'=> 9,
              'K'=> 10, 'L'=> 11,
              'M'=> 12, 'N'=> 13, 'O'=> 14, 'P'=> 15, 'Q'=> 16, 'R'=> 17, 'S'=> 18, 'T'=> 19, 'U'=> 20,
              'V'=> 21, 'W'=> 22,
              'X'=> 23, 'Y'=> 24, 'Z'=> 25, 'a'=> 26, 'b'=> 27, 'c'=> 28, 'd'=> 29, 'e'=> 30, 'f'=> 31,
              'g'=> 32, 'h'=> 33,
              'i'=> 34, 'j'=> 35, 'k'=> 36, 'l'=> 37, 'm'=> 38, 'n'=> 39, 'o'=> 40, 'p'=> 41, 'q'=> 42,
              'r'=> 43, 's'=> 44,
              't'=> 45, 'u'=> 46, 'v'=> 47, 'w'=> 48, 'x'=> 49, 'y'=> 50, 'z'=> 51, '0'=> 52, '1'=> 53,
              '2'=> 54, '3'=> 55,
              '4'=> 56, '5'=> 57, '6'=> 58, '7'=> 59, '8'=> 60, '9'=> 61, '+'=> 62, '/'=> 63}
          result = 0
          if last_idresult.class == String
            rowid = 0
            last_idresult.each_char do |char|
              rowid = rowid * 64 + rowid_dict[char]
            end
            result = @connection.query("SELECT \"#{pk_name}\" FROM \"#{table_name}\" WHERE ROWID = #{rowid};")
            inserted_id = result.each(as: :array)[0][0]
            return inserted_id
          else
            result = last_idresult
          end
          return result
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
