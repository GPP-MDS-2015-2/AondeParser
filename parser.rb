require 'csv'
require 'mysql'


file_name = ARGV[0]
file_size = `wc -l #{file_name}`.to_f


def create_tables(con)
	con.query("CREATE TABLE IF NOT EXISTS \
        superior_public_agencies(id INTEGER(11) PRIMARY KEY, name VARCHAR(50))")
	con.query("CREATE TABLE IF NOT EXISTS \
        public_agencies(id INTEGER(11) PRIMARY KEY, name VARCHAR(50), \
        superior_public_agency_id INTEGER(11), views_amount INT DEFAULT 0, CONSTRAINT superior_public_agencies_public_agencies \
		FOREIGN KEY(superior_public_agency_id) REFERENCES superior_public_agencies(id))")
	con.query("CREATE TABLE IF NOT EXISTS \
        programs(id INTEGER(11) PRIMARY KEY, name VARCHAR(100), description VARCHAR(100))")
	con.query("CREATE TABLE IF NOT EXISTS \
        type_expenses(id INTEGER(11) PRIMARY KEY, description VARCHAR(50))")
	con.query("CREATE TABLE IF NOT EXISTS \
        companies(id INTEGER(11) PRIMARY KEY, name VARCHAR(50))")
	con.query("CREATE TABLE IF NOT EXISTS \
        functions(id INTEGER(11) PRIMARY KEY, description VARCHAR(50))")
	con.query("CREATE TABLE IF NOT EXISTS \
        expenses(id INT PRIMARY KEY AUTO_INCREMENT, document_number VARCHAR(15), \
        payment_date Date NOT NULL, value NUMERIC(13,2), public_agency_id INTEGER(11), program_id INTEGER(11), type_expense_id INTEGER(11), company_id INTEGER(11), function_id INTEGER(11), \
        CONSTRAINT public_agencies_programs FOREIGN KEY(public_agency_id) REFERENCES public_agencies(id), \
        CONSTRAINT programs_expenses FOREIGN KEY(program_id) REFERENCES programs(id), \
		CONSTRAINT type_expenses_expenses FOREIGN KEY(type_expense_id) REFERENCES type_expenses(id), \
		CONSTRAINT functions_expenses FOREIGN KEY(function_id) REFERENCES functions(id))")
end

def create_preprocess_data_table(con)
	con.query("CREATE TABLE IF NOT EXISTS \
		public_agency_graph(id_public_agency INTEGER(11), year INTEGER(4), value NUMERIC (13,2), \
			CONSTRAINT public_agency_graph_PK PRIMARY KEY (id_public_agency, year), \
			CONSTRAINT public_agency_graph_public_agencies FOREIGN KEY (id_public_agency) REFERENCES public_agencies(id))")
end


begin
    con = Mysql.new 'localhost', 'root', 'root', 'aonde_parser'    
    create_tables(con)

    count = 0

	CSV.foreach(file_name, encoding: "iso-8859-1:utf-8",
			headers: true, col_sep: "\t", quote_char: "|") do |row|
		con.query("INSERT IGNORE INTO superior_public_agencies(id, name) VALUES(\"#{row["Código Órgão Superior"]}\", \"#{row["Nome Órgão Superior"]}\")")
		con.query("INSERT IGNORE INTO public_agencies(id, name, superior_public_agency_id) VALUES(\"#{row["Código Órgão"]}\", \"#{row["Nome Órgao"]}\", \"#{row["Código Órgão Superior"]}\")")
		con.query("INSERT IGNORE INTO programs(id, name, description) VALUES(\"#{row["Código Programa"]}\", \"#{row["Nome Programa"]}\", \"#{row["Linguagem Cidadã"]}\")")
		con.query("INSERT IGNORE INTO type_expenses(id, description) VALUES(\"#{row["Código Elemento Despesa"]}\", \"#{row["Nome Elemento Despesa"]}\")")
		con.query("INSERT IGNORE INTO companies(id, name) VALUES(\"#{row["Código Favorecido"]}\", \"#{row["Nome Favorecido"].gsub("\"", "\'")}\")")
		con.query("INSERT IGNORE INTO functions(id, description) VALUES(\"#{row["Código Função"]}\", \"#{row["Nome Função"]}\")")
		con.query("INSERT IGNORE INTO expenses(public_agency_id, document_number, value, payment_date, program_id, function_id, type_expense_id, company_id) \
			VALUES(\"#{row["Código Órgão"]}\", \"#{row["Número Documento"]}\", \"#{row["Valor"]}\", STR_TO_DATE('#{row["Data Pagamento"]}', '%d/%m/%Y'), \"#{row["Código Programa"]}\", \"#{row["Código Função"]}\", \"#{row["Código Elemento Despesa"]}\", \"#{row["Código Favorecido"]}\")")
	
		count = count+1
		system "clear"
		puts "#{((count/file_size)*100).round(2)}%"
	end

	create_preprocess_data_table(con)

rescue Mysql::Error => e
    puts e.errno
    puts e.error
    
ensure
    con.close if con
end