class DataSource < ActiveRecord::Base

  validates :name, :adapter, :host, :dbname, :user, presence: true

  has_many :ignored_tables

  has_one :database_memo, class_name: "DatabaseMemo", foreign_key: :name, primary_key: :name, dependent: :destroy

  after_save :disconnect_data_source!

  class ConnectionBad < IOError
  end

  def self.data_source_adapter_cache
    @data_source_adapter_cache ||= {}
  end

  def self.data_source_tables_cache
    @data_source_tables_cache ||= {}
  end

  def data_source_adapter
    self.class.data_source_adapter_cache[id] ||= Object.const_get("DataSourceAdapters::#{adapter.capitalize}Adapter").new(self)
  end

  def data_source_tables
    table_names = cached_source_table_names
    table_names.map do |schema_name, table_name|
      data_source_table(schema_name, table_name, table_names)
    end
  end

  def reset_data_source_tables!
    Rails.cache.delete(cache_key_source_table_names)
    self.class.data_source_adapter_cache.delete(id)
    self.class.data_source_tables_cache[id] = {}
  end

  def access_logging
    Rails.logger.tagged("DataSource #{name}") { yield }
  end

  private

  def source_table_names
    table_names = access_logging { data_source_adapter.source_table_names }
    table_names.reject {|_, table_name| ignored_table_patterns.match(table_name) }
  end

  def cache_key_source_table_names
    "data_source_source_table_names_#{id}"
  end

  def cached_source_table_names
    key = cache_key_source_table_names
    cache = Rails.cache.read(key)
    return cache if cache
    value = source_table_names
    Rails.cache.write(key, value)
    value
  end

  def data_source_table(schema_name, table_name, table_names=cached_source_table_names)
    return if ignored_table_patterns.match(table_name)
    schema_name, _ = table_names.find {|schema, table| schema == schema_name && table == table_name }
    return nil unless schema_name
    full_table_name = "#{schema_name}.#{table_name}"
    self.class.data_source_tables_cache[id] ||= {}
    source_table = self.class.data_source_tables_cache[id][full_table_name]
    return source_table if source_table
    self.class.data_source_tables_cache[id][full_table_name] = DataSourceTable.new(self, schema_name, table_name)
  rescue ActiveRecord::ActiveRecordError, Mysql2::Error, PG::Error => e
    raise ConnectionBad.new(e)
  end

  def ignored_table_patterns
    @ignored_table_patterns ||= Regexp.union(ignored_tables.pluck(:pattern).map {|pattern| Regexp.new(pattern, true) })
  end

  def disconnect_data_source!
    data_source_adapter.disconnect_data_source!
  end
end
