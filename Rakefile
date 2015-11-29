# encoding: utf-8
require_relative 'parser'
require_relative 'db_helper'

namespace :data do

  conf = Rails.configuration.database_configuration

  task :prepare do |t|
    Rails.env = 'parser'
    Rake::Task['db:drop'].invoke
    Rake::Task['db:create'].invoke
    Rake::Task['db:migrate'].invoke
  end

  task :parse, [:dest_env, :folder] do |t, args|
    Rake::Task['data:prepare'].invoke

    folder_name = args[:folder]
    folder = Dir.glob("#{folder_name}/*.csv")

    total_files = folder.count
    current_file = 1

    folder.each do |csv_file|
      ActiveRecord::Base.establish_connection(conf['parser'])
      con = ActiveRecord::Base.connection

      puts "Processing file #{current_file} of #{total_files}"
      Parser::parse(con, csv_file)

      sql_file = csv_file.gsub(".csv", ".sql")
      Rake::Task['data:dump'].invoke(sql_file)
      Rake::Task['data:import'].invoke(args[:dest_env], sql_file)
      Rake::Task['data:preprocess'].invoke(args[:dest_env])

      current_file = current_file + 1
    end
  end

  task :dump, [:file] do |t, args|
    db_name = conf['parser']["database"]
    dump_file = args[:file]
    `mysqldump --no-create-info --replace --complete-insert -u root #{db_name} > #{dump_file}`
  end

  task :import, [:env, :file] do |t, args|
    user = conf[args[:env]]['username']
    password = conf[args[:env]]['password']
    database = conf[args[:env]]['database']
    `mysql -u#{user} -p#{password} #{database} < #{args[:file]}`
  end

  task :preprocess, [:env, :file] do |t, args|
    Rails.env = args[:env]
    ActiveRecord::Base.establish_connection(conf[args[:env]])
    con = ActiveRecord::Base.connection

    DBHelper::create_preprocess_data_table(con)
    con.execute("REPLACE INTO public_agency_graph (id_public_agency, year, value) \
  		SELECT public_agency_id, EXTRACT(YEAR FROM payment_date), SUM(value) \
  		FROM expenses GROUP BY public_agency_id, EXTRACT(YEAR FROM payment_date)")

    con.execute("REPLACE INTO function_graph (function_id, description, year, value) \
  		SELECT function_id, (SELECT description FROM functions WHERE id=function_id), EXTRACT(YEAR FROM payment_date), SUM(value) \
  		FROM expenses GROUP BY function_id, EXTRACT(YEAR FROM payment_date)")
  end
end
