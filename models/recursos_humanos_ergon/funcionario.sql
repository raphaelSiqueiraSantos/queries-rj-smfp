SELECT 
    SAFE_CAST(REGEXP_REPLACE(numero, r'\.0$', '') AS STRING) AS id_vinculo,
    SAFE_CAST(nome AS STRING) AS nome,
    SAFE_CAST(flex_campo_03 AS STRING) AS nome_social,
    SAFE_CAST(tratamento AS STRING) AS nome_tratamento,
    SAFE_CAST(sexo AS STRING) AS sexo,
    SAFE_CAST(raca AS STRING) AS raca_cor,
    SAFE_CAST(deficiente AS STRING) AS deficiente,
    SAFE_CAST(tipodefic AS STRING) AS tipo_deficiencia,
    SAFE_CAST(dtnasc AS DATE) AS data_nascimento,
    SAFE_CAST(flex_campo_69 AS STRING) AS pais_nascimento,
    SAFE_CAST(cidnasc AS STRING) AS municipio_nascimento,
    SAFE_CAST(cidadeender_cod AS STRING) AS municipio_moradia,
    SAFE_CAST(ufnasc AS STRING) AS sigla_uf_nascimento,
    SAFE_CAST(g_sanguineo AS STRING) AS grupo_sanguineo,
    SAFE_CAST(pai AS STRING) AS nome_pai,
    SAFE_CAST(mae AS STRING) AS nome_mae,
    SAFE_CAST(estcivil AS STRING) AS estado_civil,
    SAFE_CAST(REGEXP_REPLACE(escolaridade, r'\.0$', '') AS STRING) AS escolaridade,
    SAFE_CAST(REGEXP_REPLACE(nacionalidade, r'\.0$', '') AS STRING) AS nacionalidade,
    SAFE_CAST(tipologender AS STRING) AS tipo_logradouro,
    SAFE_CAST(nomelogender AS STRING) AS logradouro,
    SAFE_CAST(numender AS STRING) AS numero_porta,
    SAFE_CAST(complender AS STRING) AS complemento_numero_porta,
    SAFE_CAST(bairroender AS STRING) AS bairro,
    SAFE_CAST(cidadeender AS STRING) AS municipio,
    SAFE_CAST(ufender AS STRING) AS sigla_uf,
    SAFE_CAST(cepender AS STRING) AS cep,
    SAFE_CAST(flex_campo_89 AS STRING) AS cep_exterior,
    SAFE_CAST(numtel_celular AS STRING) AS celular,
    SAFE_CAST(telefone AS STRING) AS telefone,
    SAFE_CAST(ramal AS STRING) AS ramal,
    SAFE_CAST(e_mail AS STRING) AS email,
    SAFE_CAST(flex_campo_90 AS STRING) AS email_alternativo,
    SAFE_CAST(anoprimemp AS STRING) AS ano_primeiro_emprego,
    SAFE_CAST(flex_campo_70 AS DATE) AS data_primeiro_emprego,
    SAFE_CAST(ufempant AS STRING) AS sigla_uf_emprego_anterior,
    SAFE_CAST(flex_campo_80 AS DATE) AS data_primeira_habitacao,
    SAFE_CAST(flex_campo_51 AS STRING) AS deficiente_fisico,
    SAFE_CAST(flex_campo_52 AS STRING) AS deficiente_visual,
    SAFE_CAST(flex_campo_53 AS STRING) AS deficiente_auditivo,
    SAFE_CAST(flex_campo_54 AS STRING) AS deficiente_mental,
    SAFE_CAST(flex_campo_55 AS STRING) AS deficiente_intelectual,
    SAFE_CAST(REGEXP_REPLACE(numrg, r'\.0$', '') AS STRING) AS id_rg,
    SAFE_CAST(tiporg AS STRING) AS tipo_rg,
    SAFE_CAST(orgaorg AS STRING) AS emissor_rg,
    SAFE_CAST(ufrg AS STRING) AS sigla_uf_rg,
    SAFE_CAST(expedrg AS DATE) AS data_expedicao_rg,
    SAFE_CAST(REGEXP_REPLACE(cpf, r'\.0$', '') AS STRING) AS id_cpf,
    SAFE_CAST(REGEXP_REPLACE(numcartpro, r'\.0$', '') AS STRING) AS id_ctps,
    SAFE_CAST(sercartpro AS STRING) AS serie_ctps,
    SAFE_CAST(ufcartpro AS STRING) AS sigla_uf_ctps,
    SAFE_CAST(REGEXP_REPLACE(numtitel, r'\.0$', '') AS STRING) AS id_titulo_eleitor,
    SAFE_CAST(zonatitel AS STRING) AS zona_eleitoral,
    SAFE_CAST(sectitel AS STRING) AS secao_eleitoral,
    SAFE_CAST(uftitel AS STRING) AS sigla_uf_titulo_eleitor,
    SAFE_CAST(REGEXP_REPLACE(numdocmili, r'\.0$', '') AS STRING) AS id_documento_militar,
    SAFE_CAST(serdocmili AS STRING) AS serie_documento_militar,
    SAFE_CAST(catmili AS STRING) AS categoria_militar,
    SAFE_CAST(orgaomili AS STRING) AS emissor_documento_militar,
    SAFE_CAST(ufdocmili AS STRING) AS sigla_uf_documento_militar,
    SAFE_CAST(REGEXP_REPLACE(cnh, r'\.0$', '') AS STRING) AS id_cnh,
    SAFE_CAST(catcnh AS STRING) AS categoria_cnh,
    SAFE_CAST(REGEXP_REPLACE(validcnh, r'\.0$', '') AS STRING) AS validade_cnh,
    SAFE_CAST(ufcnh AS STRING) AS sigla_uf_cnh,
    SAFE_CAST(flex_campo_26 AS STRING) AS orgao_emissor_cnh,
    SAFE_CAST(flex_campo_27 AS DATE) AS data_expedicao_cnh,
    SAFE_CAST(REGEXP_REPLACE(pispasep, r'\.0$', '') AS STRING) AS id_pispasep,
    SAFE_CAST(datapis AS DATE) AS data_inicio_pis,
    SAFE_CAST(REGEXP_REPLACE(flex_campo_40, r'\.0$', '') AS STRING) AS id_nis,
    SAFE_CAST(bancopis AS STRING) AS numero_banco_nis,
    SAFE_CAST(REGEXP_REPLACE(identprof, r'\.0$', '') AS STRING) AS id_identidade_profissional,
    SAFE_CAST(REGEXP_REPLACE(tipoidprof, r'\.0$', '') AS STRING) AS tipo_identidade_profissional,
    SAFE_CAST(REGEXP_REPLACE(flex_campo_06, r'\.0$', '') AS STRING) AS id_ric,
    SAFE_CAST(flex_campo_07 AS STRING) AS emissor_ric,
    SAFE_CAST(flex_campo_08 AS DATE) AS data_emissao_ric,
    SAFE_CAST(flex_campo_28 AS STRING) AS expedicao_documento_orgao_classe,
    SAFE_CAST(flex_campo_30 AS STRING) AS emissor_documento_orgao_classe,
    SAFE_CAST(uf_identprof AS STRING) AS sigla_uf_documento_orgoao_classe,
    SAFE_CAST(flex_campo_02 AS STRING) AS possui_impedimento,
    SAFE_CAST(banco AS STRING) AS numero_banco,
    SAFE_CAST(agencia AS STRING) AS agencia_banco,
    SAFE_CAST(conta AS STRING) AS conta_banco,
    SAFE_CAST(flex_campo_36 AS DATE) AS data_ultima_atualizacao_bancaria,
    SAFE_CAST(REGEXP_REPLACE(tipodoc_cert, r'\.0$', '') AS STRING) AS tipo_certidao,
    SAFE_CAST(REGEXP_REPLACE(num_cert, r'\.0$', '') AS STRING) AS numero_certidao,
    SAFE_CAST(REGEXP_REPLACE(livro_a_cert, r'\.0$', '') AS STRING) AS livro_certidao,
    SAFE_CAST(REGEXP_REPLACE(folha_cert, r'\.0$', '') AS STRING) AS folha_certidao,
    SAFE_CAST(REGEXP_REPLACE(cartorio_cert, r'\.0$', '') AS STRING) AS cartorio_ceridao,
    SAFE_CAST(REGEXP_REPLACE(uf_cart, r'\.0$', '') AS STRING) AS sigla_uf_cartorio_certidao,
    SAFE_CAST(REGEXP_REPLACE(municipio_cart_cod, r'\.0$', '') AS STRING) AS municipio_emissor_certidao,
    SAFE_CAST(municipio_cart AS STRING) AS municipio_cartorio_emissor_certudai,
    SAFE_CAST(REGEXP_REPLACE(matricula_cert, r'\.0$', '') AS STRING) AS matricula_certidao,
    SAFE_CAST(flex_campo_16 AS STRING) AS trabalho_voluntario,
    SAFE_CAST(flex_campo_17 AS STRING) AS interesse_trabalho_voluntario,
    SAFE_CAST(flex_campo_18 AS STRING) AS interesse_projeto_olimpico,
    SAFE_CAST(flex_campo_19 AS STRING) AS interesse_projeto_sociais,
    SAFE_CAST(REGEXP_REPLACE(flex_campo_56, r'\.0$', '') AS STRING) AS recebe_beneficio_previdenciario,
    SAFE_CAST(flex_campo_58 AS STRING) AS nome_posse_alteracao_sexo,
    SAFE_CAST(flex_campo_59 AS STRING) AS sexo_posse_alteracao_sexo,
    SAFE_CAST(flex_campo_60 AS DATE) AS data_alteracao_sexo,
    SAFE_CAST(REGEXP_REPLACE(flex_campo_09, r'\.0$', '') AS STRING) AS id_rne,
    SAFE_CAST(chegbrasil AS STRING) AS ano_chegada_brasil,
    SAFE_CAST(flex_campo_10 AS STRING) AS emissor_rne,
    SAFE_CAST(flex_campo_11 AS DATE) AS data_expedicao_rne,
    SAFE_CAST(flex_campo_12 AS DATE) AS data_chegada_estrangeiro,
    SAFE_CAST(flex_campo_13 AS DATE) AS data_naturalizacao,
    SAFE_CAST(flex_campo_14 AS STRING) AS estrangeiro_casado_brasileiro,
    SAFE_CAST(flex_campo_15 AS STRING) AS estrangeiro_filho_brasileiro,
    SAFE_CAST(flex_campo_20 AS STRING) AS login_facebook,
    SAFE_CAST(flex_campo_21 AS STRING) AS login_twitter,
    SAFE_CAST(flex_campo_22 AS STRING) AS login_google_plus,
    SAFE_CAST(flex_campo_23 AS STRING) AS login_instagram,
    SAFE_CAST(flex_campo_24 AS STRING) AS login_linkedin,
    SAFE_CAST(flex_campo_25 AS STRING) AS login_myspace,
FROM rj-smfp.recursos_humanos_ergon_staging.funcionario AS t