
require 'net/http'
require 'json'
require 'csv'

class MerchantHandler
  attr_reader :id, :iban, :discount, :transactions, :total_amount_gross, :total_fee_gross, :total_amount_net, :final_discount

  def self.call(**attrs)
    new(**attrs).call
  end

  def initialize(id:, iban:, discount:, transactions:)
    @id = id
    @iban = iban
    @discount = discount
    @transactions = transactions
    @total_amount_gross = 0
    @total_fee_gross = 0
  end

  def call
    calculate_total_gross
    calculate_total_amount_net
    self
  end

  private

  def calculate_total_gross
    transactions.each do |transaction|
      @total_amount_gross += transaction['amount']
      @total_fee_gross += transaction['fee']
    end
  end

  def calculate_total_amount_net
    @total_amount_net ||= @total_amount_gross - @total_fee_gross + calculate_final_discount
  end

  def calculate_final_discount
    @final_discount ||= total_fee_gross * discount_proportion
  end

  def discount_proportion
    if discount?
      discount['fees_discount'] / 100
    else
      0
    end
  end

  def discount?
    transactions.size >= discount['minimum_transaction_count']
  end
end

class MarchantApi
  API = 'https://simpledebit.gocardless.io/merchants/'.freeze

  class << self
    def get_merchant(merchant_id)
      uri = build_uri(merchant_id)
      res = get(uri)
      parse_response(res).transform_keys(&:to_sym)
    end

    def get_merchants
      uri = build_uri
      res = get(uri)
      parse_response(res)
    end

    private

    def parse_response(res)
      JSON.parse(res)
    end

    def get(uri)
      Net::HTTP.get(uri)
    end

    def build_uri(path = '')
      URI(API + path)
    end
  end
end

class CSVBuilder
  HEADER = %w[iban amount_in_pence].freeze
  TIMESTAMP_FORMAT = '%d%m%y_%H%M%S'.freeze

  def self.build(merchants)
    timestamp = Time.now.strftime(TIMESTAMP_FORMAT)
    filepath = "./#{timestamp}.csv"

    CSV.open(filepath, 'wb') do |csv|
      csv << HEADER
      merchants.each do |merchant|
        csv << [merchant.iban, merchant.total_amount_net]
      end
    end
  end
end

class MerchantsProcesser
  def self.call(merchant_ids)
    merchant_ids.map do |merchant_id|
      merchant_info = MarchantApi.get_merchant(merchant_id)
      MerchantHandler.call(**merchant_info)
    end
  end
end

merchant_ids = MarchantApi.get_merchants
merchants = MerchantsProcesser.call(merchant_ids)
CSVBuilder.build(merchants)
