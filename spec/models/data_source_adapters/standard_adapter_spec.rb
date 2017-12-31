require "rails_helper"

describe DataSourceAdapters::StandardAdapter, type: :model do
  let(:data_source_adapter) { FactoryGirl.create(:data_source).data_source_adapter }

  describe "#source_base_class" do
    it "return source base class" do
      expect(data_source_adapter.source_base_class).to eq(DataSourceAdapters::DynamicTable::Dmemo_Base)
    end
  end
end
