CREATE TABLE IF NOT EXISTS dds.dm_orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    restaurant_id INTEGER NOT NULL,
    timestamp_id INTEGER NOT NULL,
    order_key VARCHAR NOT NULL UNIQUE,
    order_status VARCHAR NOT NULL,
    CONSTRAINT fk_user
        FOREIGN KEY (user_id) REFERENCES dds.dm_users(id),
    CONSTRAINT fk_restaurant
        FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id),
    CONSTRAINT fk_timestamp
        FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id)
);


/*
CREATE TABLE IF NOT Edds.dm_orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    restaurant_id INTEGER NOT NULL,
    timestamp_id INTEGER NOT NULL,
    order_key VARCHAR NOT NULL UNIQUE,
    order_status VARCHAR NOT NULL
);
 
ALTER TABLE dds.dm_orders
ADD CONSTRAINT fk_user
FOREIGN KEY (user_id) REFERENCES dds.dm_users(id);
 
ALTER TABLE dds.dm_orders
ADD CONSTRAINT fk_restaurant
FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id);
 
ALTER TABLE dds.dm_orders
ADD CONSTRAINT fk_timestamp
FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id);

*/