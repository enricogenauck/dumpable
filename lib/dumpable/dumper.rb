module Dumpable
  class Dumper
    attr_accessor :dumpee, :options, :id_padding, :dumps

    # ---------------------------------------------------------------------------
    def initialize(dumpee, options={})
      @dumpee = dumpee
      @options = Dumpable.config.merge(options || {})
      @id_padding = @options[:id_padding] || (@dumpee.class.respond_to?(:dumpable_options) && @dumpee.class.dumpable_options[:id_padding]) || Dumpable.config.id_padding
      @dumps = @options[:dumps] || (@dumpee.class.respond_to?(:dumpable_options) && @dumpee.class.dumpable_options[:dumps])
      @lines = []
    end

    # ---------------------------------------------------------------------------
    def dump
      recursive_dump(@dumpee, @dumps)
      @lines << generate_insert_query(@dumpee)
    end

    # ---------------------------------------------------------------------------
    def self.dump(*records_and_collections)
      options = records_and_collections.extract_options!
      lines = []

      # Clear file before we start appending to it
      if (file_name = options[:file]).present?
        File.delete(file_name) if File.exists?(file_name)
      end

      records_and_collections.each do |record_or_collection|
        if record_or_collection.is_a?(Array) || record_or_collection.is_a?(ActiveRecord::Relation) || (record_or_collection.is_a?(Class) && record_or_collection.ancestors.include?(ActiveRecord::Base))
          record_or_collection = record_or_collection.all if record_or_collection.is_a?(Class) && record_or_collection.ancestors.include?(ActiveRecord::Base)
          record_or_collection.each do |object|
            lines << new(object, options).dump
          end
        else
          lines << new(record_or_collection, options).dump
        end

        # Write file incrementally so we don't end up eating GBs of memory for large-scale dumps
        Dumpable::FileWriter.write(lines.flatten.compact, options)
        lines = []
      end
    end

    # ---------------------------------------------------------------------------
    private
    # ---------------------------------------------------------------------------

    # ---------------------------------------------------------------------------
    def recursive_dump(object, dumps)
      if dumps.nil?
        # Base case recursion
      elsif dumps.is_a?(Array)
        dumps.each do |relation|
          recursive_dump(object, relation)
        end
      elsif dumps.is_a?(Hash)
        dumps.each do |key, value|
          recursive_dump(object, key)

          if scoped_query(object, key)
            scope = scoped_query(object, key)
            scope = scope.includes(value) if scope.is_a?(Repo::ActiveRecord_Relation)
            Array(scope).each do |child|
              recursive_dump(child, value)
            end
          else
            unless @options.quiet
              puts "***********************************************************"
              puts "NIL association of object #{ object.inspect }, key #{ key }"
              puts "***********************************************************"
            end
          end
        end
      elsif dumps.is_a?(Symbol) || dumps.is_a?(String)
        # E.g., object is `User`, dumps is `:posts`, so here we'll iterate over every post instance
        # (here named `child_object`) and set its foreign key to correspond with the parent instance (usually
        # the instance that invoked the call to dump, unless we're deeper in recursion when we arrive here)
        reflection = object.class.reflections.symbolize_keys[dumps.to_sym]
        composed_objects = Array(scoped_query(object, dumps)).map do |child_object|
          unless reflection
            raise %{Couldn't find reflection "#{ dumps }" for object #{ object.inspect }}
          end

          if reflection.macro == :belongs_to
            object.send("#{reflection.association_foreign_key}=", object.id + @id_padding)
          elsif [:has_many, :has_one].include? reflection.macro
            # for a has_many through, leave the foreign key as-is
            unless reflection.options[:through].present?
              if reflection.respond_to?(:foreign_key)
                child_object.send("#{reflection.foreign_key}=", object.id + @id_padding)
              else
                child_object.send("#{reflection.primary_key_name}=", object.id + @id_padding)
              end
            end
          end

          child_object
        end

        if composed_objects.present?
          @lines << generate_insert_query(composed_objects)
        end
      end
    rescue => e
      unless @options.quiet
        puts "Error during processing: #{$!}"
        puts "Backtrace:\n\t#{ e.backtrace.join("\n\t") }" # Avoid falling victim to the "... 15 other levels ..." stacktrace
      end
      raise
    end

    # ---------------------------------------------------------------------------
    def scoped_query(object, key)
      unless (reflection = object.class.reflections.symbolize_keys[key.to_sym])
        raise "Couldn't find reflection: #{ key }"
      end

      scope = object.send(key)
      if reflection.macro == :has_many
        scope = scope.limit(@options.limit) if @options.limit
        scope = scope.order(@options.order) if @options.order
      end

      scope
    end

    # ---------------------------------------------------------------------------
    def generate_insert_query(object_or_array)
      object = object_or_array.is_a?(Array) ? object_or_array.first : object_or_array

      skip_columns = Array(@options[:skip_columns] || (object.class.respond_to?(:dumpable_options) && object.class.dumpable_options[:skip_columns])).map(&:to_s)
      cloned_attributes = object.attributes_before_type_cast.clone
      return nil unless cloned_attributes["id"].present?
      cloned_attributes["id"] += @id_padding

      keys = cloned_attributes.map do |key, _|
        skip_columns.include?(key.to_s) ? nil : key
      end.compact

      # Resultant value a la: [ ["1", "bob", "taco"], ["2", "sam", "french fry"] ]
      value_arrays = Array.wrap(object_or_array).map do |dumpable_object|
        keys.map { |key| dump_value_string(dumpable_object[key]) }
      end

      # Resultant value a la: [ %{"1", "bob", "taco"}, %{"2", "sam", "french fry"} ]
      value_arrays = value_arrays.map { |value_array| value_array.join(", ") }

      mysql_keys = keys.map { |key| "`#{ key }`" }.join(", ")
      "INSERT #{ "IGNORE " if @options[:ignore_existing] }INTO #{object.class.table_name} (#{ mysql_keys }) VALUES (#{ value_arrays.join("), (") });"
    end

    # ---------------------------------------------------------------------------
    # http://invisipunk.blogspot.com/2008/04/activerecord-raw-insertupdate.html
    def dump_value_string(value)
      case value.class.to_s
        when "Time"
          "'#{value.strftime("%Y-%m-%d %H:%M:%S")}'"
        when "NilClass"
          "NULL"
        when "Fixnum"
          value
        when "String"
          # String can't end with a backslash or it fouls up mysql parsing, and if we have a
          # single apostrophe, escape it:
          ActiveRecord::Base.sanitize(value)
        when "FalseClass"
          '0'
        when "TrueClass"
          '1'
        when "ActiveSupport::HashWithIndifferentAccess"
          "'#{value.to_yaml.gsub(/'/, "\\\\'")}'"
        else
          "'#{value}'"
      end
    end
  end
end