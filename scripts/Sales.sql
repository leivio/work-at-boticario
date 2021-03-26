                DO $$
                BEGIN
                  if exists(select * from information_schema.tables where table_name = 'sales') then  
                    truncate table sales;  
                  else 
                    create table sales (id_marca int, marca varchar(80), id_linha int, linha varchar(80),data_venda date, qtd_venda int);
                  end if;
                END; 
                $$ 