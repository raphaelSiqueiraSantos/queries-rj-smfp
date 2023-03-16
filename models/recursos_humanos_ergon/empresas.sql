SELECT 
    SAFE_CAST(REGEXP_REPLACE(TRIM(empresa), r'\.0$', '') AS INT64) AS id_empresa,
    SAFE_CAST(REGEXP_REPLACE(TRIM(nome), r'\.0$', '') AS STRING) AS nome_empresa,
    SAFE_CAST(REGEXP_REPLACE(TRIM(fantasia), r'\.0$', '') AS STRING) AS nome_empresa_fantasia,
    SAFE_CAST(REGEXP_REPLACE(TRIM(razao), r'\.0$', '') AS STRING) AS razao_social,
    SAFE_CAST(REGEXP_REPLACE(TRIM(cgc), r'\.0$', '') AS STRING) AS cnpj,
    SAFE_CAST(REGEXP_REPLACE(TRIM(numender), r'\.0$', '') AS STRING) AS numero_endereco,
    SAFE_CAST(REGEXP_REPLACE(TRIM(complemento), r'\.0$', '') AS STRING) AS complemento_endereco,
    SAFE_CAST(REGEXP_REPLACE(TRIM(cep), r'\.0$', '') AS STRING) AS cep,
    SAFE_CAST(REGEXP_REPLACE(TRIM(ddd), r'\.0$', '') AS STRING) AS ddd_telefone,
    SAFE_CAST(REGEXP_REPLACE(TRIM(fone), r'\.0$', '') AS STRING) AS telefone,
    SAFE_CAST(REGEXP_REPLACE(TRIM(email), r'\.0$', '') AS STRING) AS email,
    SAFE_CAST(REGEXP_REPLACE(TRIM(ativ_econ), r'\.0$', '') AS STRING) AS atividade_economica,
    SAFE_CAST(REGEXP_REPLACE(TRIM(cnae), r'\.0$', '') AS STRING) AS cnae,
FROM rj-smfp.recursos_humanos_ergon_staging.empresas AS t
WHERE
    data_particao < CURRENT_DATE('America/Sao_Paulo')