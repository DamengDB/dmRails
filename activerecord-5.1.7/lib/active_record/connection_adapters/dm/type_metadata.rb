module ActiveRecord
  module ConnectionAdapters
    module Dm
      class TypeMetadata < DelegateClass(SqlTypeMetadata) # :nodoc:
        undef to_yaml if method_defined?(:to_yaml)

        attr_reader :extra

        def initialize(type_metadata, virtual: nil)
          super(type_metadata)
          @type_metadata = type_metadata
          @virtual = virtual
        end

        def ==(other)
          other.is_a?(Dm::TypeMetadata) &&
            attributes_for_hash == other.attributes_for_hash
        end
        alias eql? ==

        def hash
          attributes_for_hash.hash
        end

        protected
        def attributes_for_hash
          [self.class, @type_metadata, virtual]
        end

        end
    end
  end
end
