SELECT
SAFE_CAST(codigo_egpweb AS STRING) as codigo_meta_egpweb,
SAFE_CAST(ano AS INT64) as ano,
SAFE_CAST(orgao_ar AS STRING) as orgao_egpweb,
SAFE_CAST(orgao_resp AS STRING) as orgao_responsavel,
SAFE_CAST(false AS STRING) as codigo_meta,
SAFE_CAST(nod AS STRING) as codigo_meta_desdobrada,
SAFE_CAST(e_c AS STRING) as tipo_meta,
SAFE_CAST(descricao_meta_ar AS STRING) as descricao_meta,
SAFE_CAST(descricao_meta_ar_desdobrada AS STRING) as descricao_meta_desdobrada,
SAFE_CAST(fonte AS STRING) as fonte,
SAFE_CAST(valor_de_referencia AS STRING) as valor_referencia,
SAFE_CAST(data_de_referencia AS STRING) as data_referencia,
SAFE_CAST(meta AS STRING) as meta,
SAFE_CAST(relacionada_a_meta_estrategica AS STRING) as relacao_estrategia,
SAFE_CAST(indicacao_para_auditoria_pela_cgm AS STRING) as indicacao_auditoria,
SAFE_CAST(gerencia AS STRING) as gerencia,
SAFE_CAST(chave_meta_ar AS STRING) as chave_meta,
SAFE_CAST(codigo_metaar AS STRING) as codigo_meta_ar,
SAFE_CAST(SAFE_CAST(ref_resultado AS FLOAT64) AS INT64) as referencia_resultado,
SAFE_CAST(resultado AS STRING) as resultado,
SAFE_CAST(ref_chance_meta_desdobrada AS INT64) as referencia_chance_meta_desdobrada, 
SAFE_CAST(chance_meta_desdobrada AS INT64) as chance_meta_desdobrada,
SAFE_CAST(ref_resultado2_performance AS INT64) as referencia_resultado_orgao_2,
SAFE_CAST(resultado2_performance AS STRING) as resultado_orgao_2,
SAFE_CAST(ref_chance2_performance AS INT64) as referencia_chance_meta_desdobrada_orgao_2,
SAFE_CAST(chance2_performance AS INT64) as chance_meta_desdobrada_orgao_2,
SAFE_CAST(resultado_orgao1 AS STRING) as resultado_completo_orgao_1,
SAFE_CAST(resultado_orgao2_performance AS STRING) as resultado_completo_orgao_2,
SAFE_CAST(caso_chance AS INT64) as caso_chance_orgao_1,
SAFE_CAST(caso_chance_orgao_2_performance AS INT64) as caso_chance_orgao_2,
SAFE_CAST(pior_caso_chance AS INT64) as pior_caso_chance,
SAFE_CAST(cor_chance_meta AS INT64) as chance_meta_orgao_1,
SAFE_CAST(cor_chance_meta_orgao_2 AS INT64) as chance_meta_orgao_2,
SAFE_CAST(resultado_performance_prefeito AS BOOL) as resultado_peformance_orgao_1,
SAFE_CAST(resultado_performance2_prefeito AS BOOL) as resultado_performance_orgao_2
FROM `planejamento_gestao_acordo_resultados_staging.meta_desdobrada`