\c ecommerce_db;

CREATE TYPE public.orderstatus AS ENUM (
    'PENDING', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'REFUNDED'
);
CREATE TYPE public.storestatus AS ENUM ( 'ACTIVE', 'MAINTENANCE' );
CREATE TYPE public.subscriptionstatus AS ENUM ( 'ACTIVE', 'CANCELED', 'PAST_DUE' );
CREATE TYPE public.subscriptiontier AS ENUM ( 'BASIC', 'PRO', 'ENTERPRISE' );


-- -- ******************* Creating Application Tables Start *******************
-- -- We can create these tables using the alembic head upgrade command

CREATE TABLE IF NOT EXISTS public.alembic_version (
    version_num varchar(32) NOT NULL,
    CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
);


CREATE TABLE IF NOT EXISTS public.merchants (
    merchant_id uuid NOT NULL,
    company_name varchar(255) NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT merchants_pkey PRIMARY KEY (merchant_id)
);

CREATE TABLE IF NOT EXISTS public.customers (
    customer_id uuid NOT NULL,
    merchant_id uuid NOT NULL,
    email varchar(255) NOT NULL,
    hashed_password varchar(255) NOT NULL,
    shipping_address varchar(500) NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT customers_pkey PRIMARY KEY (customer_id)
);

CREATE TABLE IF NOT EXISTS public.stores (
    store_id uuid NOT NULL,
    merchant_id uuid NOT NULL,
    domain_name varchar(255) NOT NULL,
    "name" varchar(100) NOT NULL,
    region varchar(50) NOT NULL,
    status public."storestatus" NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT stores_pkey PRIMARY KEY (store_id)
);

CREATE TABLE IF NOT EXISTS public.subscriptions (
    subscription_id uuid NOT NULL,
    merchant_id uuid NOT NULL,
    plan_tier public."subscriptiontier" NOT NULL,
    status public."subscriptionstatus" NOT NULL,
    billing_cycle varchar(50) NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT subscriptions_pkey PRIMARY KEY (subscription_id)
);

CREATE TABLE IF NOT EXISTS public.orders (
    order_id uuid NOT NULL,
    merchant_id uuid NOT NULL,
    store_id uuid NOT NULL,
    customer_id uuid NOT NULL,
    status public."orderstatus" NOT NULL,
    total_amount numeric(10, 2) NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT orders_pkey PRIMARY KEY (order_id),
    CONSTRAINT orders_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES public.customers(customer_id),
    CONSTRAINT orders_merchant_id_fkey FOREIGN KEY (merchant_id) REFERENCES public.merchants(merchant_id),
    CONSTRAINT orders_store_id_fkey FOREIGN KEY (store_id) REFERENCES public.stores(store_id)
);

CREATE TABLE IF NOT EXISTS public.products (
    product_id uuid NOT NULL,
    merchant_id uuid NOT NULL,
    store_id uuid NOT NULL,
    sku varchar(100) NOT NULL,
    "name" varchar(100) NOT NULL,
    category varchar(100) NOT NULL,
    price numeric(10, 2) NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT products_pkey PRIMARY KEY (product_id)
);

CREATE TABLE IF NOT EXISTS public.order_lines (
    order_line_id uuid NOT NULL,
    order_id uuid NOT NULL,
    product_id uuid NOT NULL,
    quantity int4 NOT NULL,
    unit_price numeric(10, 2) NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT order_lines_pkey PRIMARY KEY (order_line_id)
);

-- ******************* Creating Application Tables End *******************


-- ******************* Inserting Alembic revision code/number *******************

INSERT INTO public.alembic_version
VALUES ('40da1343998e')
;
