SELECT 
    id, 
    dateparution, 
    commercant, 
    registre[1] AS siren, 
    numerodepartement, 
    region_nom_officiel, 
    ville, 
    (modificationsgenerales ->> '$.descriptif') AS descriptif

FROM
    {{ ref('annonces') }}

WHERE 
    familleavis = 'modification' AND (modificationsgenerales ->> '$.descriptif') ILIKE '%capital%'