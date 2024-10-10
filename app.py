import duckdb as dk
import pandas as pd



# Caminho para o arquivo Excel Finalizado (alterar para comentário o bloco de  testes e descomentar esse bloco)
#dcb003 = r"h:\EXPEDICAO\00 Painel de Controle\Programação\DCB003\DCB003.xlsx"
#pdb988 = r"h:\EXPEDICAO\00 Painel de Controle\Programação\PDB988\PDB988.xlsx"
#ftb101 = r"H:\EXPEDICAO\00 Painel de Controle\Programação\FTB101\FTB101.xlsx"
#cubagem = r"H:\EXPEDICAO\18 Cubagens - Cabotagem  & Rodoviário\CUBAGEM.xlsx"

# Caminhos para os arquivos em testes
dcb003 = r"/media/roberto/500 gb/Projetos Python/Ocupação/Programação/DCB003/DCB003.xlsx"
pdb988 = r"/media/roberto/500 gb/Projetos Python/Ocupação/Programação/PDB988/PDB988.xlsx"
ftb101 = r"/media/roberto/500 gb/Projetos Python/Ocupação/Programação/FTB101/FTB101.xlsx"
cubagem = r"/media/roberto/500 gb/Projetos Python/Ocupação/18 Cubagens - Cabotagem  & Rodoviário/CUBAGEM.xlsx"

# Carregar os arquivos Excel
dfdcb003 = pd.read_excel(dcb003, decimal=",")
dfpdb988 = pd.read_excel(pdb988, decimal=",")
dfftb101 = pd.read_excel(ftb101, decimal=",")
dfcubagem = pd.read_excel(cubagem, decimal=",")

# Conectar ao DuckDB
con = dk.connect()

# Registrar os DataFrames no DuckDB
con.register("dcb003", dfdcb003)
con.register("pdb988", dfpdb988)
con.register("ftb101", dfftb101)
con.register("cubagem", dfcubagem)

# Executar as consultas SQL para cada DataFrame
df_nfaturaveis_e_paletes_final = con.sql("""
WITH subconsulta AS
(
    SELECT
        dcb003.item,
        UPPER(dcb003.dep) AS Dep,
        dcb003.quantidade,
        UPPER(dcb003.loc) AS Loc,
        'Demais depósitos (DCL/REJ/VCL/CQ/FOR)' AS "Data",
        cubagem.PALETIZAÇÃO,
        dcb003.quantidade / cubagem.PALETIZAÇÃO AS paletes
    FROM
        dcb003
    LEFT JOIN
        cubagem ON dcb003.item = cubagem.item
    WHERE
        (dcb003.dep LIKE 'CQ' OR dcb003.dep LIKE 'REJ' OR dcb003.dep LIKE 'VCL' OR dcb003.dep LIKE 'DCL' OR dcb003.dep LIKE 'FOR')
        AND (dcb003.loc LIKE 'A2%')
)
SELECT
    "Data",
    SUM(Quantidade) AS Produtos,
    CAST(SUM(Paletes) AS INT) AS Paletes
FROM
    subconsulta
GROUP BY
    "Data"
""")

df_vaivem = con.sql("""
WITH subquery AS
(
    SELECT
        dcb003.item,
        UPPER(dcb003.dep) AS Dep,
        dcb003.quantidade AS Qtd,
        UPPER(dcb003.loc) AS Loc,
        cubagem.PALETIZAÇÃO,
        dcb003.quantidade / cubagem.PALETIZAÇÃO AS paletes
    FROM
        dcb003
    LEFT JOIN
        cubagem ON dcb003.item = cubagem.item
    WHERE
        dcb003.loc LIKE 'TR-V%'
)
SELECT
    'Vai Vem' AS "Data",
    SUM(Qtd) AS Produtos,
    CAST(SUM(Paletes) AS INT) AS Paletes
FROM
    subquery
GROUP BY
    Loc
""")

df_cif = con.sql("""
WITH subquery AS
(   
    SELECT
        ftb101.Item,
        ftb101."Qt.Pecas" AS Qtd,
        UPPER(ftb101.Depósito) AS Dep,
        ftb101.Frete,
        ftb101."Qt.Pecas" / cubagem.PALETIZAÇÃO AS Paletes
    FROM
        ftb101
    INNER JOIN
        cubagem ON ftb101.Item = cubagem.Item
    WHERE
        UPPER(ftb101.Depósito) IN ('ACA', 'EXP')
)
SELECT
    'CIF' AS "Data",
    SUM(Qtd) AS Produtos,
    CAST(SUM(Paletes) AS INT) AS Paletes
FROM
    subquery
WHERE Frete LIKE 'CIF'
GROUP BY
    Frete
""")

df_fob = con.sql("""
WITH subquery AS
(   
    SELECT
        ftb101.Item,
        ftb101."Qt.Pecas" AS Qtd,
        UPPER(ftb101.Depósito) AS Dep,
        ftb101.Frete,
        ftb101."Qt.Pecas" / cubagem.PALETIZAÇÃO AS Paletes
    FROM
        ftb101
    INNER JOIN
        cubagem ON ftb101.Item = cubagem.Item
    WHERE
        UPPER(ftb101.Depósito) IN ('ACA', 'EXP')
)
SELECT
    'FOB' AS "Data",
    SUM(Qtd) AS Produtos,
    CAST(SUM(Paletes) AS INT) AS Paletes
FROM
    subquery
WHERE Frete LIKE 'FOB'
GROUP BY
    Frete
""")

df_exp = con.sql("""
WITH subquery AS
(
    SELECT
        dcb003.item,
        UPPER(dcb003.dep) AS Dep,
        dcb003.quantidade,
        UPPER(dcb003.loc) AS Loc,
        dcb003.quantidade / cubagem.PALETIZAÇÃO AS Paletes
    FROM
        dcb003
    LEFT JOIN
        cubagem ON dcb003.item = cubagem.item
    WHERE
        dep LIKE 'EXP'
)
SELECT
    Dep,
    SUM(Quantidade) AS Produtos,
    CAST(SUM(Paletes) AS INT) AS Paletes
FROM
    subquery
GROUP BY
    Dep
""")

df_pl3_final = con.sql("""
WITH subconsulta AS
(    
    SELECT
        dcb003.item,
        UPPER(dcb003.dep) AS Dep,
        dcb003.quantidade,
        UPPER(dcb003.loc) AS Loc,
        dcb003.quantidade / cubagem.PALETIZAÇÃO AS Paletes
    FROM
        dcb003
    LEFT JOIN
        cubagem ON dcb003.item = cubagem.item
    WHERE
        dep LIKE 'PL3' OR loc LIKE 'pl3'
)
SELECT
    Dep,
    CAST(SUM(Quantidade)AS INT) AS Produtos,
    CAST(SUM(Paletes) AS INT) AS Paletes
FROM
    subconsulta
GROUP BY
    Dep
""")


df_exibir = con.sql("""
WITH subquery AS (
    SELECT * FROM df_nfaturaveis_e_paletes_final
    UNION ALL
    SELECT * FROM df_vaivem
    UNION ALL
    SELECT * FROM df_cif
    UNION ALL
    SELECT * FROM df_fob
    UNION ALL
    SELECT * FROM df_exp
    UNION ALL
    SELECT * FROM df_pl3_final
)
SELECT
    "Data",
    SUM(Produtos) AS Produtos,
    SUM(Paletes) AS Paletes
FROM
    subquery
GROUP BY
    "Data"

UNION ALL

SELECT
    'TOTAL' AS "Data",  -- Adiciona a linha de total
    CAST(SUM(Produtos) AS INT) AS Produtos,  -- Total de Produtos
    CAST(SUM(Paletes) AS INT) AS Paletes   -- Total de Paletes
FROM
    subquery
""").show()

df_final = con.sql("""
WITH subquery AS (
    SELECT * FROM df_nfaturaveis_e_paletes_final
    UNION ALL
    SELECT * FROM df_vaivem
    UNION ALL
    SELECT * FROM df_cif
    UNION ALL
    SELECT * FROM df_fob
    UNION ALL
    SELECT * FROM df_exp
    UNION ALL
    SELECT * FROM df_pl3_final
)
SELECT
    "Data",
    SUM(Produtos) AS Produtos,
    SUM(Paletes) AS Paletes
FROM
    subquery
GROUP BY
    "Data"

UNION ALL

SELECT
    'TOTAL' AS "Data",  -- Adiciona a linha de total
    CAST(SUM(Produtos) AS INT) AS Produtos,  -- Total de Produtos
    CAST(SUM(Paletes) AS INT) AS Paletes   -- Total de Paletes
FROM
    subquery
""").df()



legenda = {
    'Categoria': ['EXP', 'CIF', 'FOB', 'Ñ FATURÁVEL', 'VAI-VEM', 'PL3'],
    'Descrição': [
        'PA não Faturado',
        'Faturado não embarcado CIF',
        'Faturado não embarcado FOB',
        'Demais depósitos (DCL/REJ/VCL/CQ/FOR)',
        'PRODUTOS EM TRANPORTE',
        'DESCASADOS EM PL3'
    ]
}



df_legenda = pd.DataFrame(legenda)

html1 = df_final.to_html(index=False, decimal=',')
html2 = df_legenda.to_html(index=False, decimal=',')

def tabela1():
    return html1

def tabela2():
    return html2