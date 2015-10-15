CREATE TABLE hypercube AS
SELECT
    -- Dimensions
    geo, type, misc, nature, time,
    -- Measures
    SUM(consumption) AS consumption,
    SUM(amount) AS amount,
    COUNT(DISTINCT c.id) AS nclients,
    COUNT(DISTINCT k.id) AS ncontracts,
    COUNT(*) AS ninvoices
FROM clients AS c
JOIN contracts AS k ON c.id = k.client
JOIN invoices AS i ON i.contrat = k.id
GROUP BY type, geo, misc, nature, time;