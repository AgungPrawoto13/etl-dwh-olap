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

-- query to show stock material
create materialized view datamart_stok.mv_stock_summary
engine = SummingMergeTree()
order by (material_id)
as
select
	material_id,
	sum(qty) as total_qty
from datamart_stok.fact_stocks
group by 
	material_id;

INSERT INTO datamart_stok.mv_stock_summary
SELECT
    material_id,
    sum(qty) AS total_qty
FROM datamart_stok.fact_stocks
GROUP BY material_id;


SELECT 
m.material_id,
m.material_name, 
s.total_qty
FROM datamart_stok.mv_stock_summary s
JOIN datamart_stok.dim_material m ON s.material_id = m.material_id
ORDER BY m.material_id  DESC;

