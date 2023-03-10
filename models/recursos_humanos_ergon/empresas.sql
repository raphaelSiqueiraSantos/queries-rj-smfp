SELECT 
    SAFE_CAST(TRIM(empresa) AS STRING) AS id_empresa,
    SAFE_CAST(TRIM(nome) AS STRING) AS nome_empresa,
    SAFE_CAST(TRIM(fantasia) AS STRING) AS sigla,
    SAFE_CAST(TRIM(razao) AS STRING) AS razao_social,
    SAFE_CAST(TRIM(cgc) AS STRING) AS CNPJ,
    SAFE_CAST(TRIM(ativ_econ) AS STRING) AS atividade_economica,
FROM rj-smfp.recursos_humanos_ergon_staging.empresa AS t
