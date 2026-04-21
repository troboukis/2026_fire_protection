ALTER TABLE public.current_fires ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS public_read_current_fires ON public.current_fires;
