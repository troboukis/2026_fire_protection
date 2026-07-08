BEGIN;

-- Remove legacy duplicate public-read policies reported by the Supabase linter.
DROP POLICY IF EXISTS anon_select_procurement ON public.procurement;

-- Stop blanket function execution grants from exposing privileged helpers.
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC, anon, authenticated;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC, anon, authenticated;

GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO service_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT EXECUTE ON FUNCTIONS TO service_role;

-- Frontend read RPCs are intentionally public, but must not execute with owner privileges.
ALTER FUNCTION public.get_analysis_top_authorities(integer, integer, integer) SECURITY INVOKER;
ALTER FUNCTION public.get_contract_analysis(integer) SECURITY INVOKER;
ALTER FUNCTION public.get_contracts_page(text, text, text, date, date, numeric, integer, integer) SECURITY INVOKER;
ALTER FUNCTION public.get_diavgeia_page(text, date, date, text, integer, integer) SECURITY INVOKER;
ALTER FUNCTION public.get_direct_award_distribution() SECURITY INVOKER;
ALTER FUNCTION public.get_environment_ministry_dashboard(integer) SECURITY INVOKER;
ALTER FUNCTION public.get_featured_beneficiaries(integer, integer) SECURITY INVOKER;
ALTER FUNCTION public.get_hero_section_data(integer, integer) SECURITY INVOKER;
ALTER FUNCTION public.get_homepage_funding(integer, integer) SECURITY INVOKER;
ALTER FUNCTION public.get_latest_contract_cards(integer) SECURITY INVOKER;
ALTER FUNCTION public.get_latest_funding_year_municipality_spend() SECURITY INVOKER;
ALTER FUNCTION public.get_municipality_contract_count(text, integer) SECURITY INVOKER;
ALTER FUNCTION public.get_municipality_contract_summary(text, integer) SECURITY INVOKER;
ALTER FUNCTION public.get_municipality_contracts(text, integer, integer, integer) SECURITY INVOKER;
ALTER FUNCTION public.get_municipality_diavgeia_decisions(text, integer) SECURITY INVOKER;
ALTER FUNCTION public.get_municipality_featured_beneficiaries(text, integer, integer) SECURITY INVOKER;
ALTER FUNCTION public.get_municipality_map_funding_per_100k(integer) SECURITY INVOKER;
ALTER FUNCTION public.get_municipality_map_spend_per_100k(integer) SECURITY INVOKER;
ALTER FUNCTION public.get_region_contract_count(text, integer) SECURITY INVOKER;
ALTER FUNCTION public.get_region_contract_summary(text, integer) SECURITY INVOKER;
ALTER FUNCTION public.get_region_contracts(text, integer, integer, integer) SECURITY INVOKER;
ALTER FUNCTION public.normalize_procedure_type(text) SECURITY INVOKER;

GRANT EXECUTE ON FUNCTION public.get_analysis_top_authorities(integer, integer, integer) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_contract_analysis(integer) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_contracts_page(text, text, text, date, date, numeric, integer, integer) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_diavgeia_page(text, date, date, text, integer, integer) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_direct_award_distribution() TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_environment_ministry_dashboard(integer) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_featured_beneficiaries(integer, integer) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_hero_section_data(integer, integer) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_homepage_funding(integer, integer) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_latest_contract_cards(integer) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_latest_funding_year_municipality_spend() TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_municipality_contract_count(text, integer) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_municipality_contract_summary(text, integer) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_municipality_contracts(text, integer, integer, integer) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_municipality_diavgeia_decisions(text, integer) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_municipality_featured_beneficiaries(text, integer, integer) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_municipality_map_funding_per_100k(integer) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_municipality_map_spend_per_100k(integer) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_region_contract_count(text, integer) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_region_contract_summary(text, integer) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_region_contracts(text, integer, integer, integer) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.normalize_procedure_type(text) TO anon, authenticated, service_role;

REVOKE ALL ON FUNCTION public.sync_current_fire_coordinates_from_firms()
FROM PUBLIC, anon, authenticated;

GRANT EXECUTE ON FUNCTION public.sync_current_fire_coordinates_from_firms()
TO service_role;

COMMIT;
