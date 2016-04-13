class TableMemosController < ApplicationController
  def index
    redirect_to database_memo_path(params[:database_memo_id])
  end

  def show
    @table_memo = TableMemo.find(params[:id])
  end
end