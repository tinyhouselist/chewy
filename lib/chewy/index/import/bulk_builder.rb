          require 'pry' #FIXME, remove it
module Chewy
  class Index
    module Import
      # This class purpose is to build ES client-acceptable bulk
      # request body from the passed objects for index and deletion.
      # It handles parent-child relationships as well by fetching
      # existing documents from ES, taking their `_parent` field and
      # using it in the bulk body.
      # If fields are passed - it creates partial update entries except for
      # the cases when the type has parent and parent_id has been changed.
      class BulkBuilder
        RoutingCacheMissError = Class.new(StandardError)

        # @param index [Chewy::Index] desired index
        # @param to_index [Array<Object>] objects to index
        # @param delete [Array<Object>] objects or ids to delete
        # @param fields [Array<Symbol, String>] and array of fields for documents update
        def initialize(index, to_index: [], delete: [], fields: [])
          @index = index
          @to_index = to_index
          @delete = delete
          @fields = fields.map!(&:to_sym)
        end

        # Returns ES API-ready bulk requiest body.
        # @see https://github.com/elastic/elasticsearch-ruby/blob/master/elasticsearch-api/lib/elasticsearch/api/actions/bulk.rb
        # @return [Array<Hash>] bulk body
        def bulk_body
          populate_cache

          @bulk_body ||= @to_index.flat_map(&method(:index_entry)).concat(
            @delete.flat_map(&method(:delete_entry))
          )
        end

        # The only purpose of this method is to cache document ids for
        # all the passed object for index to avoid ids recalculation.
        #
        # @return [Hash[String => Object]] an ids-objects index hash
        def index_objects_by_id
          @index_objects_by_id ||= index_object_ids.invert.stringify_keys!
        end

      private

        def crutches
          @crutches ||= Chewy::Index::Crutch::Crutches.new @index, @to_index
        end

        def parents
          return unless type_root.parent_id

          @parents ||= begin
            ids = @index.map do |object|
              object.respond_to?(:id) ? object.id : object
            end
            ids.concat(@delete.map do |object|
              object.respond_to?(:id) ? object.id : object
            end)
            @index.filter(ids: {values: ids}).order('_doc').pluck(:_id, :_parent).to_h
          end
        end

        def index_entry(object)
          entry = {}
          entry[:_id] = index_object_ids[object] if index_object_ids[object]

          data = data_for(object)
          parent = parents[entry[:_id].to_s]

          entry[:routing] = routing(object) if join_field?
          if parent_changed?(data, parent)
            entry[:data] = data
            delete = delete_entry(object).first
            index = {index: entry}
            [delete, index]
          elsif @fields.present?
            return [] unless entry[:_id]
            entry[:data] = {doc: data_for(object, fields: @fields)}
            [{update: entry}]
          else
            entry[:data] = data
            [{index: entry}]
          end
        end

        def delete_entry(object)
          entry = {}
          entry[:_id] = entry_id(object)
          entry[:_id] ||= object.as_json

          return [] if entry[:_id].blank?

          parent = parents[entry[:_id].to_s]

          entry[:routing] = existing_routing(object.id) if join_field?
          entry[:parent] = parent[:parent_id] if parent && parent[:parent_id]

          [{delete: entry}]
        end

        def populate_cache
          @cache ||= load_cache
        end

        def load_cache
          return {} unless join_field?

          @index
            .filter(ids: {values: ids_for_cache})
            .order('_doc')
            .pluck(:_id, :_routing, join_field)
            .map do |id, routing, join|
              [
                id,
                {routing: routing, parent_id: join['parent']}
              ]
            end.to_h
        end

        def existing_routing(id)
          # All objects needed here should be cached in #load_cache,
          # if not, we raise an error.
          binding.pry unless @cache[id.to_s]
          raise RoutingCacheMissError unless @cache[id.to_s]

          @cache[id.to_s][:routing]
        end

        def ids_for_cache
          ids = @to_index.flat_map do |object|
            [find_parent_id(object), object.id] if object.respond_to?(:id)
          end
          ids.concat(@delete.map do |object|
            object.id if object.respond_to?(:id)
          end)
          ids.uniq.compact
        end

        def routing(object)
          # filter out non-model objects, early return on object==nil
          return unless object.respond_to?(:id)

          parent_id = find_parent_id(object)
          if parent_id
            routing(index_objects_by_id[parent_id.to_s]) || existing_routing(parent_id)
          else
            object.id.to_s
          end
        end

        def parents
          @cache
        end

        def find_parent_id(object)
          join = data_for(object)[join_field]
          join['parent'] if join
        end

        def join_field
          return @join_field if defined?(@join_field)

          @join_field = find_join_field
        end

        def find_join_field
          type_settings = @index.mappings_hash[:mappings]
          return unless type_settings
          properties = type_settings[:properties]
          join_fields = properties.find { |_, options| options[:type] == :join }
          return unless join_fields

          join_fields.first.to_s
        end

        def join_field?
          join_field && !join_field.empty?
        end

        def data_for(object, fields: [])
          @index.compose(object, crutches, fields: fields)
        end

        def parent_changed?(data, old_parent)
          return false unless old_parent
          return false unless join_field?
          return false unless @fields.include?(join_field.to_sym)
          return false unless data.key?(join_field)

          # The join field value can be a hash, e.g.:
          # {"name": "child", "parent": "123"} for a child
          # {"name": "parent"} for a parent
          # but it can also be a string: (e.g. "parent") for a parent:
          # https://www.elastic.co/guide/en/elasticsearch/reference/current/parent-join.html#parent-join
          new_join_field_value = data[join_field]
          if new_join_field_value.is_a? Hash
            # If we have a hash in the join field,
            # we're taing the `parent` field that helds the parent id.
            new_parent_id = new_join_field_value["parent"]
            new_parent_id != old_parent[:parent_id]
          else
            # If there is a non-hash value (String or nil), it means that the join field is changed
            # and the current object is no longer a child.
            true
          end
        end

        def entry_id(object)
          if type_root.id
            type_root.compose_id(object)
          else
            id = object.id if object.respond_to?(:id)
            id ||= object[:id] || object['id'] if object.is_a?(Hash)
            id = id.to_s if defined?(BSON) && id.is_a?(BSON::ObjectId)
            id
          end
        end

        def index_object_ids
          @index_object_ids ||= @to_index.each_with_object({}) do |object, result|
            id = entry_id(object)
            result[object] = id if id.present?
          end
        end

        def type_root
          @type_root ||= @index.root
        end
      end
    end
  end
end
