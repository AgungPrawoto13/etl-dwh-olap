-- query to calculate stock quantity
SELECT 
    d.material_name,
    SUM(f.qty) AS total_stock
FROM datamart_stok.fact_stocks AS f
JOIN datamart_stok.dim_material AS d
    ON f.material_id = d.material_id  
GROUP BY d.material_name
ORDER BY total_stock DESC;

-- query to show stock entities
select 
distinct
f.qty,
d.entities_name 
from datamart_stok.fact_stocks f
join datamart_stok.dim_entity d on f.entity_id = d.entity_id
order by f.qty desc

