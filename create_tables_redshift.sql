CREATE TABLE public.driver_details
(
 driver_id  integer NOT NULL,
 driver_first_name varchar(255),
 CONSTRAINT PK_driver_id PRIMARY KEY ( driver_id )
);

CREATE TABLE public.store_details
(
 store_num  integer NOT NULL,
 address_l2 varchar(255),
 CONSTRAINT PK_repair_order PRIMARY KEY ( store_num )
);

CREATE TABLE public.delivery_details
(
 driver_id  integer NOT NULL,
 service_time  decimal,
 route_date timestamp,
 store_num integer NOT NULL,
 item_cases decimal,
 actual_arrival timestamp,
 CONSTRAINT FK_166 FOREIGN KEY ( driver_id ) REFERENCES driver_details ( driver_id ),
 CONSTRAINT FK_167 FOREIGN KEY ( store_num ) REFERENCES store_details ( store_num )
);

