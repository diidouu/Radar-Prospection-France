SELECT
    COUNT(DISTINCT siren) AS nombre_entreprises,
    numerodepartement,
    region_nom_officiel

FROM {{ ref('augmentation_capital') }}

WHERE dateparution >= current_date - INTERVAL 30 DAY

GROUP BY numerodepartement, region_nom_officiel

ORDER BY nombre_entreprises DESC