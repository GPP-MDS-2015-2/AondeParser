require 'csv'
require 'mysql2'
require_relative 'parser'
require_relative 'db_helper'

namespace :data do

  task :parse, :folder do |t, args|
    con = ActiveRecord::Base.connection
    
    folder_name = args[:folder]
    folder = Dir.glob("#{folder_name}/*.csv")

    total_files = folder.count
    current_file = 1

    folder.each do |csv_file|
      puts "Processing file #{current_file} of #{total_files}"
      Parse::parse(csv_file)

      Rake::Task['data:preprocess'].invoke
      Rake::Task['data:dump'].invoke

      current_file = current_file + 1
    end
  end

  task :preprocess do |t|
  	ActiveRecord::Base.connection.execute("INSERT INTO public_agency_graph (id_public_agency, year, value) \
  		SELECT public_agency_id, EXTRACT(YEAR FROM payment_date), SUM(value) \
  		FROM expenses GROUP BY public_agency_id, EXTRACT(YEAR FROM payment_date)")
  end

  task :dump do |t|
    db_name = Rails.configuration.database_configuration[Rails.env]["database"]
    dump_file = csv_file.gsub(".csv", ".sql")
    `mysqldump --no-create-info --replace --complete-insert -u root #{db_name} > #{dump_file}`
  end
end
