SELECT
    commercant,
    ville,
    numerodepartement,
    dateparution,
    descriptif,
    siren,
    ('https://www.bodacc.fr/pages/annonces-commerciales-detail/?q.id=id:' || id) AS url_complete

FROM {{ ref('augmentation_capital') }}

WHERE dateparution >= CURRENT_DATE - INTERVAL 30 DAY

QUALIFY ROW_NUMBER() OVER (PARTITION BY siren ORDER BY dateparution DESC) = 1

ORDER BY dateparution DESC