
{{
    config(
        materialized='incremental',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        }
    )
}}

SELECT 
    SAFE_CAST(REGEXP_REPLACE(TRIM(numfunc), r'\.0$', '') AS STRING) AS id_funcionario,
    SAFE_CAST(REGEXP_REPLACE(TRIM(numvinc), r'\.0$', '') AS STRING) AS id_vinculo,
    SAFE_CAST(REGEXP_REPLACE(TRIM(vantagem), r'\.0$', '') AS STRING) AS nome_vantagem,
    SAFE_CAST(TRIM(dtini) AS DATE) AS data_inicio,
    SAFE_CAST(TRIM(dtfim) AS DATE) AS data_final,
    SAFE_CAST(REGEXP_REPLACE(TRIM(valor), r'\.0$', '') AS STRING) AS valor_vantagem,
    SAFE_CAST(REGEXP_REPLACE(TRIM(info), r'\.0$', '') AS STRING) AS informacao_atributo,
    SAFE_CAST(REGEXP_REPLACE(TRIM(tipo_incorporacao), r'\.0$', '') AS STRING) AS tipo_incorporacao_cargo_fiducia,
    SAFE_CAST(REGEXP_REPLACE(TRIM(perc_inc_funcao), r'\.0$', '') AS STRING) AS percentual_incorporacao_cargo_fiducia,
    SAFE_CAST(REGEXP_REPLACE(TRIM(inc_tabelavenc), r'\.0$', '') AS STRING) AS incide_tabela_vencimentos,
    SAFE_CAST(REGEXP_REPLACE(TRIM(inc_referencia), r'\.0$', '') AS STRING) AS incide_tabela_simbolo,
    SAFE_CAST(REGEXP_REPLACE(TRIM(obs), r'\.0$', '') AS STRING) AS observacoes,
    SAFE_CAST(REGEXP_REPLACE(TRIM(valor2), r'\.0$', '') AS STRING) AS valor_complementar,
    SAFE_CAST(REGEXP_REPLACE(TRIM(info2), r'\.0$', '') AS STRING) AS informacao_complementar,
    SAFE_CAST(REGEXP_REPLACE(TRIM(valor3), r'\.0$', '') AS STRING) AS valor_complementar_2,
    SAFE_CAST(REGEXP_REPLACE(TRIM(info3), r'\.0$', '') AS STRING) AS informacao_complementar_2,
    SAFE_CAST(REGEXP_REPLACE(TRIM(valor4), r'\.0$', '') AS STRING) AS valor_coplementar_3,
    SAFE_CAST(REGEXP_REPLACE(TRIM(info4), r'\.0$', '') AS STRING) AS informacao_complementar_3,
    SAFE_CAST(REGEXP_REPLACE(TRIM(valor5), r'\.0$', '') AS STRING) AS valor_complementar_4,
    SAFE_CAST(REGEXP_REPLACE(TRIM(info5), r'\.0$', '') AS STRING) AS informacao_complementar_4,
    SAFE_CAST(REGEXP_REPLACE(TRIM(valor6), r'\.0$', '') AS STRING) AS valor_complementar_5,
    SAFE_CAST(REGEXP_REPLACE(TRIM(info6), r'\.0$', '') AS STRING) AS informacao_complementar_5,
    SAFE_CAST(REGEXP_REPLACE(TRIM(flex_campo_05), r'\.0$', '') AS STRING) AS valor_incorporado,
    SAFE_CAST(REGEXP_REPLACE(TRIM(emp_codigo), r'\.0$', '') AS STRING) AS id_empresa,
    SAFE_CAST(REGEXP_REPLACE(TRIM(chavevant), r'\.0$', '') AS STRING) AS id_vantagem,
    SAFE_CAST(data_particao AS DATE) data_particao,
FROM rj-smfp.recursos_humanos_ergon_staging.vantagens AS t
WHERE
    data_particao < CURRENT_DATE('America/Sao_Paulo')

{% if is_incremental() %}

{% set max_partition = run_query("SELECT gr FROM (SELECT IF(max(data_particao) > CURRENT_DATE('America/Sao_Paulo'), CURRENT_DATE('America/Sao_Paulo'), max(data_particao)) as gr FROM " ~ this ~ ")").columns[0].values()[0] %}

AND
    data_particao > ("{{ max_partition }}")

{% endif %}
