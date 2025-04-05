CREATE TABLE IF NOT EXISTS stg.delivery_system_deliveries (
	id 			int4 GENERATED ALWAYS AS IDENTITY NOT NULL,
    order_id 	TEXT NOT NULL,
    order_ts 	TEXT NOT NULL,
    delivery_id TEXT NOT NULL,
    courier_id 	TEXT NOT NULL,
    address 	TEXT NOT NULL,
    delivery_ts TEXT NOT NULL,
    rate 		TEXT NOT NULL,	--INT CHECK (rate >= 1 AND rate <= 5),
    sum 		TEXT NOT NULL, 	--DECIMAL(10, 2) NOT NULL,
    tip_sum 	TEXT NOT NULL,	--DECIMAL(10, 2) NOT NULL,
	CONSTRAINT delivery_system_delivery_delivery_id UNIQUE (delivery_id),
	CONSTRAINT delivery_system_delivery_pkey PRIMARY KEY (id)
);


SELECT *  FROM stg.delivery_system_deliveries;

SELECT dd.delivery_id, count(dd.*) FROM stg.delivery_system_deliveries dd GROUP BY dd.delivery_id HAVING count(dd.delivery_id)>1;

/*
{'order_id': '67c470301c73132b9d05fff8', 'order_ts': '2025-03-02 14:50:24.637000', 'delivery_id': 'nkqvdql4hg6pos145xf3t0c', 'courier_id': 'lzxrn5kjsn493wjp2pxq05m',
'address': 'Ул. Старая, 6, кв. 467', 'delivery_ts': '2025-03-02 15:57:33.704000', 'rate': 5, 'sum': 840, 'tip_sum': 84}
*/

