#
# LOAD PRODUCTLIST DATA
#
#
#
#
#

$LOAD_PATH.unshift File.expand_path(File.dirname(__FILE__) + '/../StockTips/lib')

require 'csv'
require 'data_mapper'
#require  'dm-migrations'

DataMapper::Logger.new($stdout, :info)
DataMapper.setup(:default, 'mysql://alpha:alpha@192.168.10.197/Analysis')

require 'dm_product'
require 'dm_closing_price'

DataMapper.finalize

# ---------------------------------------------------------------------------
# Parse command line
# ---------------------------------------------------------------------------

if ARGV.length < 2
	puts "Usage: ruby dm_load_index.rb <file> <INDEX>"
	exit
end

filename = ARGV[0]
index_name = ARGV[1]


# ---------------------------------------------------------------------------
# Read CSV file into colnames and row_array
# ---------------------------------------------------------------------------
first = true
row_array = []
colnames = []
CSV.foreach(filename) do |row|
  if first
  	first = false
  	colnames = row
  else
  	row_array << row
  end
end


# ---------------------------------------------------------------------------
# Map this specific layout
# ---------------------------------------------------------------------------

def mapArrayToHash(rowarr, field_names)
	a = [] # an array of hashes
	rowarr.each do |row|
		hash = Hash.new
		0.upto(field_names.length-1) do |index|
			#puts "#{index} #{field_names[index]} VALUE= #{row[index]} }"
			hash[field_names[index].downcase.to_sym] = row[index]
		end
		a << hash
	end
	a
end


indx = Product.all(:name => index_name)
if indx.length == 0
	puts "#{index_name} not defined!"
	exit 1
end

puts "PRODUCT ID IS #{indx[0].id}"

m = mapArrayToHash(row_array, ["Date","Open","High","Low","Close","Volume","Adj Close"] )
m.each do |x|
	# p x.inspect
	puts "testing date #{x[:date]} #{x[:open]} #{x[:high]} #{x[:low]} #{x[:close]} #{x[:volume]} for NQ"
	i = ClosingPrice.first_or_create(
			{
				:product_id => indx[0].id,
				:dt => x[:date]
			},
			{
				:opn => x[:open],
				:cls => x[:close],
				:high => x[:high],
				:low => x[:low],
				:volume => (x[:volume].to_i)
			}).update(
				:opn => x[:open],
				:cls => x[:close],
				:high => x[:high],
				:low => x[:low],
				:volume => (x[:volume].to_i))

end
