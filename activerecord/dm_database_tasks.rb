module ActiveRecord
  module Tasks
    class DmDatabaseTasks
      ACCESS_DENIED_ERROR = 1045

      def prepare_command_options
        args = {
          "server"    => "--server",
          "username"  => "--user",
          "password"  => "--password",
        }.map { |opt, arg| "#{arg}=#{configuration[opt]}" if configuration[opt] }.compact
        args
      end

      delegate :connection, :establish_connection, to: ActiveRecord::Base

      def initialize(configuration)
        @configuration = configuration
      end

      def create
        system_password = ENV.fetch("PASSWORD") {
          print "Please provide the SYSTEM password for your Dm installation\n>"
          $stdin.gets.strip
        }
        establish_connection(@config.merge(username: "SYSDBA", password: system_password))
        connection.execute "CREATE USER #{@config[:username]} IDENTIFIED BY #{@config[:password]}"
      end

      def drop
        establish_connection(@config)
        connection.drop_database configuration["database"]
      end

      def purge
        drop
        connection.execute("PURGE RECYCLEBIN") rescue nil
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

