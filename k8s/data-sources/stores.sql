-- Third demo source: the table whose topic gets Tableflow enabled live. Idempotent; applied by `make postgres`.
CREATE TABLE IF NOT EXISTS public.stores (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  city TEXT NOT NULL,
  country TEXT NOT NULL,
  employees INT NOT NULL DEFAULT 0
);
INSERT INTO public.stores (name, city, country, employees)
SELECT * FROM (VALUES ('Flagship','Madrid','ES',42), ('Harbour','Oslo','NO',18), ('Old Town','Milan','IT',25)) AS v(name, city, country, employees)
WHERE NOT EXISTS (SELECT 1 FROM public.stores);
GRANT SELECT ON public.stores TO cdc;
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'dbz_publication_stores') THEN
    CREATE PUBLICATION dbz_publication_stores FOR TABLE public.stores;
  END IF;
END $$;
