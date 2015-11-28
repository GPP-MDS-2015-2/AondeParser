module DBHelper

  def self.insert_row(con, row)
    con.execute("INSERT IGNORE INTO superior_public_agencies(id, name) VALUES(\"#{row["Código Órgão Superior"]}\", \"#{row["Nome Órgão Superior"]}\")")
    con.execute("INSERT IGNORE INTO public_agencies(id, name, superior_public_agency_id) VALUES(\"#{row["Código Órgão"]}\", \"#{row["Nome Órgao"]}\", \"#{row["Código Órgão Superior"]}\")")
    con.execute("INSERT IGNORE INTO programs(id, name, description) VALUES(\"#{row["Código Programa"]}\", \"#{row["Nome Programa"]}\", \"#{row["Linguagem Cidadã"]}\")")
    con.execute("INSERT IGNORE INTO type_expenses(id, description) VALUES(\"#{row["Código Elemento Despesa"]}\", \"#{row["Nome Elemento Despesa"]}\")")
    con.execute("INSERT IGNORE INTO companies(id, name) VALUES(\"#{row["Código Favorecido"]}\", \"#{row["Nome Favorecido"].gsub("\"", "\'")}\")")
    con.execute("INSERT IGNORE INTO functions(id, description) VALUES(\"#{row["Código Função"]}\", \"#{row["Nome Função"]}\")")
    con.execute("INSERT IGNORE INTO expenses(public_agency_id, document_number, value, payment_date, program_id, function_id, type_expense_id, company_id) \
    		VALUES(\"#{row["Código Órgão"]}\", \"#{row["Número Documento"]}\", \"#{row["Valor"]}\", STR_TO_DATE('#{row["Data Pagamento"]}', '%d/%m/%Y'), \"#{row["Código Programa"]}\", \"#{row["Código Função"]}\", \"#{row["Código Elemento Despesa"]}\", \"#{row["Código Favorecido"]}\")")
  end

  def self.create_preprocess_data_table(con)
    con.execute("CREATE TABLE IF NOT EXISTS public_agency_graph(id_public_agency \
        INTEGER(11), year INTEGER(4), value NUMERIC (13,2), \
        CONSTRAINT public_agency_graph_PK PRIMARY KEY (id_public_agency, year), \
        CONSTRAINT public_agency_graph_public_agencies FOREIGN KEY (id_public_agency) REFERENCES public_agencies(id))")
  end
end
