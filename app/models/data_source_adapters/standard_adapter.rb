module DataSourceAdapters
  class StandardAdapter
    def initialize(data_source)
      @data_source = data_source
    end

    def source_base_class
      return @source_base_class if @source_base_class

      base_class = Class.new(DynamicTable::AbstractTable)
      base_class_name = source_base_class_name

      DynamicTable.send(:remove_const, base_class_name) if DynamicTable.const_defined?(base_class_name)
      DynamicTable.const_set(base_class_name, base_class)
      base_class.establish_connection(connection_config)

      @source_base_class = base_class
    end

    def disconnect_data_source!
      source_base_class.establish_connection.disconnect!
    end

    def quote(value)
      source_base_class.connection.quote(value)
    end

    def fetch_columns(table_name)
      source_base_class.connection.columns(table_name)
    end

    def fetch_rows(table, limit)
      connection = source_base_class.connection
      column_names = table.columns.map {|column| connection.quote_column_name(column.name) }.join(", ")
      rows = connection.select_rows(<<-SQL, "#{table.table_name.classify} Load")
        SELECT #{column_names} FROM #{connection.quote_table_name(table.full_table_name)} LIMIT #{limit};
      SQL
      rows.map {|row|
        table.columns.zip(row).map {|column, value| column.type_cast_from_database(value) }
      }
    rescue ActiveRecord::ActiveRecordError, Mysql2::Error, PG::Error => e
      raise DataSource::ConnectionBad.new(e)
    end

    def fetch_count(table)
      connection = source_base_class.connection
      connection.select_value(<<-SQL).to_i
        SELECT COUNT(*) FROM #{connection.quote_table_name(table.full_table_name)};
      SQL
    rescue ActiveRecord::ActiveRecordError, Mysql2::Error, PG::Error => e
      raise DataSource::ConnectionBad.new(e)
    end

    private

    def source_base_class_name
      "#{@data_source.name.gsub(/[^\w_-]/, '').underscore.classify}_Base"
    end

    def connection_config
      {
        adapter: @data_source.adapter,
        host: @data_source.host,
        port: @data_source.port,
        database: @data_source.dbname,
        username: @data_source.user,
        password: @data_source.password.presence,
        encoding: @data_source.encoding.presence,
        pool: @data_source.pool.presence,
      }.compact
    end
  end
end
