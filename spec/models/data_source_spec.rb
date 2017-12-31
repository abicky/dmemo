require "rails_helper"

describe DataSource, type: :model do
  let(:data_source) { FactoryGirl.create(:data_source) }

  describe "#source_base_class" do
    it "return source base class" do
      expect(data_source.source_base_class).to eq(DataSource::DynamicTable::Dmemo_Base)
    end
  end

  describe "#data_source_tables" do
    it "returns data source tables" do
      expect(data_source.data_source_tables.size).to be > 0

      table = data_source.data_source_tables.find { |t| t.schema_name == "public" && t.table_name == "data_sources" }
      expect(table).to be_present
      expect(table.columns.map(&:name)).to match_array(%w(
        id name description adapter host port dbname user password encoding pool created_at updated_at
      ))
    end
  end
end
