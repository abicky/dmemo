module DataSourceAdapters
  module DynamicTable
    class AbstractTable < ActiveRecord::Base
      self.abstract_class = true
    end
  end
end
