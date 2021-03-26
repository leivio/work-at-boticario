-- a. Tabela1: Consolidado de vendas por ano e mês;
create view vw_vendas_ano_mes
as
select sum(qtd_venda) vendas, extract(year from data_venda)::CHAR(4) || '-' || extract(MONTH from data_venda)::CHAR(2) as Total  from sales
group by extract(year from data_venda)::CHAR(4) || '-' || extract(MONTH from data_venda)::CHAR(2)
order by 2 desc
-- b. Tabela2: Consolidado de vendas por marca e linha;
create view vw_vendas_marca_Linha
as
select sum(qtd_venda) vendas, Marca, linha from sales
group by Marca, linha
order by 2, 3 desc
-- c. Tabela3: Consolidado de vendas por marca, ano e mês;
create view vw_vendas_marca_ano_mes
as
select sum(qtd_venda) vendas, Marca, extract(year from data_venda)::CHAR(4) || '-' || extract(MONTH from data_venda)::CHAR(2) as Total  from sales
group by marca, extract(year from data_venda)::CHAR(4) || '-' || extract(MONTH from data_venda)::CHAR(2)
order by 3 desc
-- d. Tabela4: Consolidado de vendas por linha, ano e mês;
create view vw_vendas_linha_ano_mes
as
select sum(qtd_venda) vendas, Linha, extract(year from data_venda)::CHAR(4) || '-' || extract(MONTH from data_venda)::CHAR(2) as Total  from sales
group by Linha, extract(year from data_venda)::CHAR(4) || '-' || extract(MONTH from data_venda)::CHAR(2)
order by 3 desc


Palavras a serem pesquisadas: “Boticário” e o nome da linha com mais vendas no mês 12 de 2019 (conforme item 2.d.);


