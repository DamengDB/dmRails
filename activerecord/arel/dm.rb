# frozen_string_literal: true

module Arel # :nodoc: all
  module Visitors
    class Dm < Arel::Visitors::ToSql
      private
        def visit_Arel_Nodes_SelectStatement(o, collector)
          # Oracle does not allow LIMIT clause with select for update
          if o.limit && o.lock
            raise ArgumentError, <<~MSG
              Combination of limit and lock is not supported. Because generated SQL statements
              `SELECT FOR UPDATE and FETCH FIRST n ROWS` generates ORA-02014.
            MSG
          end
          super
        end

        def visit_Arel_Nodes_SelectOptions(o, collector)
          collector = maybe_visit o.offset, collector
          collector = maybe_visit o.limit, collector
          maybe_visit o.lock, collector
        end

        def visit_Arel_Nodes_Limit(o, collector)
          if $parse_type == 'MYSQL'
            collector << "LIMIT "
            collector = visit o.expr, collector
          else
            collector << "FETCH FIRST "
            collector = visit o.expr, collector
            collector << " ROWS ONLY"
          end
        end

        def visit_Arel_Nodes_Offset(o, collector)
          collector << "OFFSET "
          visit o.expr, collector
          collector << " ROWS"
        end

        def visit_Arel_Nodes_Except(o, collector)
          collector << "( "
          collector = infix_value o, collector, " MINUS "
          collector << " )"
        end

        def visit_Arel_Nodes_UpdateStatement(o, collector)
          # Oracle does not allow ORDER BY/LIMIT in UPDATEs.
          if o.orders.any? && o.limit.nil?
            # However, there is no harm in silently eating the ORDER BY clause if no LIMIT has been provided,
            # otherwise let the user deal with the error
            o = o.dup
            o.orders = []
          end

          super
        end

        def visit_Arel_Nodes_BindParam(o, collector)
          collector.add_bind(o.value) { |i| ":a#{i}" }
        end

        def visit_Arel_Nodes_InsertStatement(o, collector)
          collector << "INSERT INTO "
          collector = visit o.relation, collector

          unless o.columns.empty?
            collector << " ("
            o.columns.each_with_index do |x, i|
              collector << ", " unless i == 0
              collector << quote_column_name(x.name)
            end
            collector << ")"
          end

          if o.values
            maybe_visit o.values, collector
          elsif o.select
            maybe_visit o.select, collector
          else
            collector
          end
        end

        def visit_Arel_Nodes_SelectStatement(o, collector)
          if o.with
            collector = visit o.with, collector
            collector << " "
          end

          collector = o.cores.inject(collector) { |c, x|
            visit_Arel_Nodes_SelectCore(x, c)
          }

          unless o.orders.empty?
            collector << " ORDER BY "
            o.orders.each_with_index do |x, i|
              collector << ", " unless i == 0
              collector = visit(x, collector)
            end
          end

          visit_Arel_Nodes_SelectOptions(o, collector)
        end

        def aggregate(name, o, collector)
          if $parse_type == "MYSQL"
            quote_sign = '`'
            dquote_sign = '``'
          else
            quote_sign = '"'
            dquote_sign = '""'
          end
          collector << "#{name}("
          quote_flag = false
          need_quote_flag = true
          if o.expressions.length > 0
            for expression in o.expressions
              if !expression.is_a? Arel::Nodes::SqlLiteral
                need_quote_flag = false
                break
              end
            end
            if o.expressions.length == 1
              if o.expressions[0].to_s != '*'
                quote_flag = true
              end
            else
              quote_flag = true
            end
          end

          if need_quote_flag and quote_flag
            collector << quote_sign
          end

          if o.distinct
            collector << "DISTINCT "
          end
          if need_quote_flag
            collector = inject_join(o.expressions, collector, quote_sign + ", " + quote_sign)
          else
            collector = inject_join(o.expressions, collector, ", ")
          end
          if need_quote_flag and quote_flag
            collector << quote_sign
          end
          collector << ")"
          if o.alias
            collector << " AS "
            collector << quote_sign
            collector << o.alias.to_s.gsub(quote_sign, dquote_sign)
            collector << quote_sign
          else
            collector
          end
        end

        def visit_Arel_Nodes_As(o, collector)
          if $parse_type == "MYSQL"
            quote_sign = '`'
            dquote_sign = '``'
          else
            quote_sign = '"'
            dquote_sign = '""'
          end
          collector = visit o.left, collector
          collector << " AS "
          collector << quote_sign
          collector << o.right.to_s.gsub(quote_sign, dquote_sign)
          collector << quote_sign
        end

        def is_distinct_from(o, collector)
          collector << "DECODE("
          collector = visit [o.left, o.right, 0, 1], collector
          collector << ")"
        end
    end
  end
end
