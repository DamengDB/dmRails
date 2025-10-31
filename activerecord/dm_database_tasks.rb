module ActiveRecord
  module Tasks
    class DmDatabaseTasks

      def quote_table_name(name)
        if @config["parse_type"] == "mysql"
          quote_sign = '`'
          dquote_sign = '``'
        else
          quote_sign = '"'
          dquote_sign = '""'
        end
        name.gsub(quote_sign, dquote_sign) if name.include?(quote_sign)
        name = quote_sign + "#{name}" + quote_sign
        name
      end

      def prepare_command_options
        args = {
          "server"    => "--server",
          "username"  => "--user",
          "password"  => "--password",
        }.map { |opt, arg| "#{arg}=#{configuration[opt]}" if configuration[opt] }.compact
        args
      end

      delegate :connection, :establish_connection, to: ActiveRecord::Base

      def initialize(config)
        @config = config
      end

      def get_schema_name()
        if @config["schema"] != nil or @config["database"] != nil
          if @config["schema"] != nil
            schema_name = @config["schema"]
          else
            schema_name = @config["database"]
          end
        else
          raise "At least one schema or database needs to be specified"
        end
        schema_name
      end

      def create
        schema_name = get_schema_name()
        establish_connection @config.merge("database" => nil, "schema" => nil)
        connection.execute "CREATE SCHEMA #{quote_table_name(schema_name)}"
      end

      def drop
        schema_name = get_schema_name()
        establish_connection(@config.merge("database" => nil, "schema" => nil))
        if @config["parse_type"] == "mysql"
          connection.execute "DROP SCHEMA #{quote_table_name(schema_name)}"
        else
          connection.execute "DROP SCHEMA #{quote_table_name(schema_name)} CASCADE"
        end
      end

      def purge
        drop
        create
      end

      def structure_dump(filename, extra_flags)
        set_psql_env

        search_path = \
          case ActiveRecord::Base.dump_schemas
          when :schema_search_path
            configuration["schema_search_path"]
          when :all
            nil
          when String
            ActiveRecord::Base.dump_schemas
          end

        args = ["-s", "-x", "-O", "-f", filename]
        args.concat(Array(extra_flags)) if extra_flags
        unless search_path.blank?
          args += search_path.split(",").map do |part|
            "--schema=#{part.strip}"
          end
        end
        args << configuration["database"]
        run_cmd("dm_dump", args, "dumping")
        remove_sql_header_comments(filename)
        File.open(filename, "a") { |f| f << "SET search_path TO #{connection.schema_search_path};\n\n" }
      end

      def structure_load(filename, extra_flags)
        set_psql_env
        args = ["-v", ON_ERROR_STOP_1, "-q", "-f", filename]
        args.concat(Array(extra_flags)) if extra_flags
        args << configuration["database"]
        run_cmd("psql", args, "loading")
      end
    end
  end
end

