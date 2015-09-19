require 'csv'
require 'mysql'


file_name = "201506_GastosDiretos.csv"
file_size = `wc -l #{file_name}`.to_f


def create_tables(con)
	con.query("CREATE TABLE IF NOT EXISTS \
        superior_public_agencies(id VARCHAR(12) PRIMARY KEY, name VARCHAR(50))")
	con.query("CREATE TABLE IF NOT EXISTS \
        public_agencies(id VARCHAR(12) PRIMARY KEY, name VARCHAR(50), \
        superior_public_agency_id VARCHAR(12), views_amount INT, CONSTRAINT superior_public_agencies_public_agencies \
		FOREIGN KEY(superior_public_agency_id) REFERENCES superior_public_agencies(id))")
	con.query("CREATE TABLE IF NOT EXISTS \
        budgets(id INT PRIMARY KEY AUTO_INCREMENT, year YEAR(4), value DECIMAL(13,2), public_agency_id VARCHAR(12), \
        CONSTRAINT public_agencies_budgets FOREIGN KEY(public_agency_id) REFERENCES public_agencies(id))")
	con.query("CREATE TABLE IF NOT EXISTS \
        programs(id VARCHAR(12) PRIMARY KEY, name VARCHAR(100), description VARCHAR(100), \
        public_agency_id VARCHAR(12), CONSTRAINT public_agencies_programs \
		FOREIGN KEY(public_agency_id) REFERENCES public_agencies(id))")
	con.query("CREATE TABLE IF NOT EXISTS \
        type_expenses(id VARCHAR(12) PRIMARY KEY, description VARCHAR(50))")
	con.query("CREATE TABLE IF NOT EXISTS \
        companies(id VARCHAR(12) PRIMARY KEY, name VARCHAR(50))")
	con.query("CREATE TABLE IF NOT EXISTS \
        functions(id VARCHAR(12) PRIMARY KEY, description VARCHAR(50))")
	con.query("CREATE TABLE IF NOT EXISTS \
        expenses(id INT PRIMARY KEY AUTO_INCREMENT, document_number VARCHAR(15), \
        payment_date Date, program_id VARCHAR(12), type_expense_id VARCHAR(12), company_id VARCHAR(12), function_id VARCHAR(12), \
        CONSTRAINT programs_expenses FOREIGN KEY(program_id) REFERENCES programs(id), \
		CONSTRAINT type_expenses_expenses FOREIGN KEY(type_expense_id) REFERENCES type_expenses(id), \
		CONSTRAINT functions_expenses FOREIGN KEY(function_id) REFERENCES functions(id))")
end


begin
    con = Mysql.new 'localhost', 'root', 'root', 'aonde_dev'    
    create_tables(con)

    count = 0

	CSV.foreach(file_name, encoding: "iso-8859-1:utf-8",
			headers: true, col_sep: "\t", quote_char: "|") do |row|
		con.query("INSERT IGNORE INTO superior_public_agencies(id, name) VALUES(\"#{row["Código Órgão Superior"]}\", \"#{row["Nome Órgão Superior"]}\")")
		con.query("INSERT IGNORE INTO public_agencies(id, name, superior_public_agency_id) VALUES(\"#{row["Código Órgão"]}\", \"#{row["Nome Órgao"]}\", \"#{row["Código Órgão Superior"]}\")")
		con.query("INSERT IGNORE INTO programs(id, name, description, public_agency_id) VALUES(\"#{row["Código Programa"]}\", \"#{row["Nome Programa"]}\", \"#{row["Linguagem Cidadã"]}\",\"#{row["Código Órgão"]}\")")
		con.query("INSERT IGNORE INTO type_expenses(id, description) VALUES(\"#{row["Código Elemento Despesa"]}\", \"#{row["Nome Elemento Despesa"]}\")")
		con.query("INSERT IGNORE INTO companies(id, name) VALUES(\"#{row["Código Favorecido"]}\", \"#{row["Nome Favorecido"].gsub("\"", "\'")}\")")
		con.query("INSERT IGNORE INTO functions(id, description) VALUES(\"#{row["Código Função"]}\", \"#{row["Nome Função"]}\")")
		con.query("INSERT IGNORE INTO expenses(document_number, payment_date, program_id, function_id, type_expense_id, company_id) \
			VALUES(\"#{row["Número Documento"]}\", \"#{row["Data Pagamento"]}\", \"#{row["Código Programa"]}\", \"#{row["Código Função"]}\", \"#{row["Código Elemento Despesa"]}\", \"#{row["Código Favorecido"]}\")")
	
	count = count+1
	system "clear"
	puts "#{((count/file_size)*100).round(2)}%"
	end

rescue Mysql::Error => e
    puts e.errno
    puts e.error
    
ensure
    con.close if con
end