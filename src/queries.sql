--Consulta total dos voos realizados e cancelados no trimestre 
select 
    case
        when extract(MONTH from max(Partida_Prevista)) = 1 then 'Janeiro'
        when extract(MONTH from max(Partida_Prevista)) = 2 then 'Fevereiro'
        when extract(MONTH from max(Partida_Prevista)) = 3 then 'Março'
    end as Mes, 
    Situacao_Voo, 
    count(*) as Total_Voos
from `project-gcp-421011.voos_Anac_2024.voos`
where 
    Partida_Prevista between '2024-01-01' and '2024-03-31'
group by
    extract(MONTH from Partida_Prevista), 
    Situacao_Voo
order by 
    extract(MONTH from max(Partida_Prevista)), 
    Situacao_Voo;


--TOP 10 Consulta de Contagem de Voos por Empresa Aérea
select Sigla_ICAO_Empresa_Aerea, count(*) as Total_Voos
from `project-gcp-421011.voos_Anac_2024.voos`
where Partida_Prevista between '2024-01-01' and '2024-03-31'
group by 1
order by Total_Voos desc
limit 10;

--Percentual de Voos Cancelados por Empresa Aérea
select Sigla_ICAO_Empresa_Aerea, 
       100.0 * sum(case when Situacao_Voo = 'CANCELADO' then 1 else 0 end) / count(*) as Percentual_Cancelados
from `project-gcp-421011.voos_Anac_2024.voos`
where Partida_Prevista between '2024-01-01' and '2024-03-31'
group by Sigla_ICAO_Empresa_Aerea
order by Percentual_Cancelados desc
limit 5;

--análise de atrasos na partida por aeroporto de origem
select 
  sigla_icao_aeroporto_origem, 
  avg(timestamp_diff(partida_real, partida_prevista, hour)) as tempo_medio_atraso
from 
`project-gcp-421011.voos_Anac_2024.voos`
where 
  partida_prevista between '2024-01-01' and '2024-03-31'
group by 
  sigla_icao_aeroporto_origem
order by 
  tempo_medio_atraso desc;


  --análise de atrasos na partida por aeroporto de destino
select 
  sigla_icao_aeroporto_destino, 
  avg(timestamp_diff(partida_real, partida_prevista, hour)) as tempo_medio_atraso
from 
`project-gcp-421011.voos_Anac_2024.voos`
where 
  partida_prevista between '2024-01-01' and '2024-03-31'
group by 
  sigla_icao_aeroporto_destino
order by 
  tempo_medio_atraso desc
limit 5

--quantidade de voos diários no mês de março para o aeroporto de Congonhas em São Paulo
select
  format_timestamp('%Y-%m-%d', Partida_Prevista) AS data,
  count(Numero_Voo) AS total_voos
from
  `project-gcp-421011.voos_Anac_2024.voos`
where
  Sigla_ICAO_Aeroporto_Origem = 'SBSP'
  and Partida_Prevista between '2024-03-01' and '2024-03-31'
group by
  data
order by 
  data;