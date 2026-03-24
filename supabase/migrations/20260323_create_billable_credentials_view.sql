-- ============================================================
-- View: billable_credentials
--
-- Returns all provider/payer/state combinations that are
-- currently billable. A credential is billable when ALL five
-- conditions are met:
--
--   1. Group enrollment is Approved for the payer+state
--   2. Individual credential is Approved with effective_date <= today
--   3. Provider status is Active
--   4. Provider is licensed in the state
--   5. Credential is linked to the Vitality group
--
-- Usage for deliverable routing:
--   SELECT * FROM billable_credentials
--     WHERE state = 'TX' AND payer = 'UHC';
--
-- Portable: works on Supabase (PostgreSQL) and AWS RDS/Aurora.
-- ============================================================

CREATE OR REPLACE VIEW billable_credentials AS
SELECT
    pc.id              AS credential_id,
    p.id               AS provider_id,
    p.first_name,
    p.last_name,
    p.personal_npi,
    p.email,
    pc.payer,
    pc.state,
    pc.effective_date,
    pc.reference_number,
    ge.id              AS group_enrollment_id,
    ge.status           AS group_status,
    cl.linked_date
FROM provider_credentials pc
JOIN providers p
    ON p.id = pc.provider_id
JOIN group_enrollments ge
    ON ge.payer = pc.payer
   AND ge.state = pc.state
JOIN credential_linking cl
    ON cl.credential_id = pc.id
WHERE pc.status        = 'Approved'
  AND pc.effective_date <= CURRENT_DATE
  AND p.status          = 'Active'
  AND pc.state          = ANY(p.licensed_states)
  AND ge.status         = 'Approved'
  AND cl.linked         = true;

-- Grant access so the app can query via the anon/authenticated roles
GRANT SELECT ON billable_credentials TO anon, authenticated;

COMMENT ON VIEW billable_credentials IS
  'Fully billable provider/payer/state combinations. '
  'All five conditions met: group approved, credential approved+effective, '
  'provider active, licensed in state, linked to group. '
  'Use for deliverable auto-assignment routing.';
