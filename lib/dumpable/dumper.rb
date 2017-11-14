module Dumpable
  class Dumper
    attr_accessor :dumpee, :options, :id_padding, :dumps

    # ---------------------------------------------------------------------------
    def initialize(dumpee, options={})
      @dumpee = dumpee
      @options = Dumpable.config.merge(options || {})
      @id_padding = @options[:id_padding] || (@dumpee.class.respond_to?(:dumpable_options) && @dumpee.class.dumpable_options[:id_padding]) || Dumpable.config.id_padding
      @dumps = @options[:dumps] || (@dumpee.class.respond_to?(:dumpable_options) && @dumpee.class.dumpable_options[:dumps])
      @objects = {}
      @lines = []
    end

    # ---------------------------------------------------------------------------
    def dump
      recursive_dump(@dumpee, @dumps)

      @objects.values.each do |object_array|
        @lines << generate_insert_query(object_array)
      end

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
        if record_or_collection.is_a?(Array) || record_or_collection.is_a?(ActiveRecord::Relation) ||
            (record_or_collection.is_a?(Class) && record_or_collection.ancestors.include?(ActiveRecord::Base))

          if record_or_collection.is_a?(Class) && record_or_collection.ancestors.include?(ActiveRecord::Base)
            record_or_collection = record_or_collection.all
          end

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

          if @id_padding != 0
            if reflection.macro == :belongs_to
              object.send("#{reflection.association_foreign_key}=", child_object.id + @id_padding)
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
          end

          child_object
        end

        if composed_objects.present?
          capture_objects(composed_objects)
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
    def capture_objects(composed_objects)
      @objects[composed_objects.first.class] ||= []
      @objects[composed_objects.first.class] += Array.wrap(composed_objects).compact
    end

    # ---------------------------------------------------------------------------
    def scoped_query(object, key)
      unless (reflection = object.class.reflections.symbolize_keys[key.to_sym])
        raise "Couldn't find reflection: #{ key }"
      end

      scope = object.send(key)
      if reflection.macro == :has_many
        scope = with_limit_applied(scope, reflection)
        scope = scope.order(@options.order) if @options.order
      end

      scope
    end

    # ---------------------------------------------------------------------------
    # To (maybe) do: as of 11/17, data that amounts to 25mb of dumped records currently eats up about
    # 1.3gb to get built. The lion's share of this can prob be attributed to our storing all of our data in a comparatively
    # complex structure (@objects is a hash of arrays). If we instead stored the contents of @objects as a string, by
    # doing the composition that happens inside here in the #capture_objects method instead, we'd be likely to dramatically reduce
    # the working memory needed to dump complex objects. Haven't done it yet because we'd still need some way to get the keys of the
    # object when we were ready to build the actual INSERT query, requiring some code sharing between this method and #capture_objects.
    def generate_insert_query(object_or_array)
      object = object_or_array.is_a?(Array) ? object_or_array.first : object_or_array
      keys = object.attributes.keys

      # Resultant value a la: [ ["1", "bob", "taco"], ["2", "sam", "french fry"] ]
      value_arrays = Array.wrap(object_or_array).map do |dumpable_object|
        keys.map do |key|
          if @id_padding && key == "id"
            dump_value_string(dumpable_object[:id] + @id_padding)
          else
            dump_value_string(dumpable_object.attributes_before_type_cast[key])
          end
        end
      end

      # The purpose of this inject is solely to split an insert that might exceed Mysql's `max_packet_size` into bite sized
      # increments that will be less than default `max_packet_size`
      # Resultant value a la: [ [ %{"1", "bob", "taco"}, %{"2", "sam", "french fry"} ], [ %{"3", "bill", "tex mex"} ] ]
      result_arrays = [[]]
      array_size = 0
      while (value_array = value_arrays.shift)
        value_string = value_array.join(", ")
        array_size += value_string.size

        if array_size > 3_000_000
          result_arrays << []
          array_size = 0
        end

        result_arrays.last << value_string
      end

      mysql_keys = keys.map { |key| "`#{ key }`" }.join(", ")
      result_arrays.map do |array|
        "INSERT #{ "IGNORE " if @options[:ignore_existing] }INTO #{ object.class.table_name } (#{ mysql_keys }) VALUES (#{ array.join("), (") });"
      end.join("\n")
    end

    # ---------------------------------------------------------------------------
    # http://invisipunk.blogspot.com/2008/04/activerecord-raw-insertupdate.html
    def dump_value_string(value)
      ActiveRecord::Base.sanitize(value)
    end

    # ---------------------------------------------------------------------------
    def with_limit_applied(scope, reflection)
      if @options.limit.nil?
        scope
      elsif @options.limit.is_a?(Integer)
        scope.limit(@options.limit)
      elsif @options.limit.is_a?(Hash)
        if (reflection_limit = @options.limit[reflection.name])
          scope.limit(reflection_limit)
        else
          scope
        end
      else
        raise "Unimplemented limit type passed"
      end
    end
  end
end