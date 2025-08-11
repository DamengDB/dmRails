require 'apartment/adapters/abstract_adapter'

gem "dmRuby"
require "dm"

module Apartment
  module Tenant

    def self.dm_adapter(config)
      Apartment.use_schemas ?
        Adapters::DmSchemaAdapter.new(config) :
        Adapters::DmAdapter.new(config)
    end
  end

  module Adapters
    class DmAdapter < AbstractAdapter

      def initialize(config)
        super

        @default_tenant = config[:database]
      end

      protected

      def rescue_from
        Dm::Error
      end
    end

    class DmSchemaAdapter < AbstractAdapter
      def initialize(config)
        super

        @default_tenant = config[:database]
        reset
      end

      #   Reset current tenant to the default_tenant
      #
      def reset
        Apartment.connection.execute "SET SCHEMA \"#{default_tenant}\""
      end

      protected

      def connect_to_new(tenant)
        return reset if tenant.nil?

        Apartment.connection.execute "SET SCHEMA \"#{environmentify(tenant)}\""

      rescue ActiveRecord::StatementInvalid => exception
        Apartment::Tenant.reset
        raise_connect_error!(tenant, exception)
      end

      def process_excluded_model(model)
        model.constantize.tap do |klass|
          table_name = klass.table_name.split('.', 2).last

          klass.table_name = "#{default_tenant}.#{table_name}"
        end
      end

      def reset_on_connection_exception?
        true
      end
    end
  end
end
