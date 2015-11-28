require 'csv'
require 'mysql2'
require_relative 'db_helper'

module Parser

  def self.parse(con, csv_file)

    file_size = `wc -l #{csv_file}`.to_f
    count = 0

    CSV.foreach(csv_file, encoding: "iso-8859-1:utf-8",
        headers: true, col_sep: "\t", quote_char: "|") do |row|

      DBHelper::insert_row(con, row)

      count = count+1
      system "clear"

      puts "#{((count/file_size)*100).round(2)}%"
    end
  end
end
