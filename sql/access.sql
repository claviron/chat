-- Access to the `chat` schema for one database role. Run as a superuser:
--
--   grant an existing role:
--     psql -d example_db -v owner=chat_owner -v role=chat_service -f sql/access.sql
--   create a login role for chat, then grant it:
--     psql -d example_db -v owner=chat_owner -v role=chat_service -v create=1 -f sql/access.sql
--
-- claviron-chat owns only `chat.*` tables (see ARCHITECTURE.md, Crate
-- Boundary), so this grants row access there and nothing else. The role
-- reads and writes rows; it cannot create, alter, drop or truncate a table
-- (no CREATE on the schema, no TRUNCATE, and only its owner may ALTER
-- or DROP), and it sees no other schema. The schema belongs to the migration owner and is created by the
-- migrations; a created role is a peer-auth LOGIN with no superuser,
-- CREATEROLE, CREATEDB or membership. Idempotent.

\set ON_ERROR_STOP on

\if :{?role}
\else
  \echo 'usage: psql -d <database> -v owner=<migration-owner> -v role=<service-role> [-v create=1] -f access.sql'
  \quit
\endif

\if :{?owner}
\else
  \echo 'owner is required: specify the role that creates chat tables'
  \quit
\endif

\if :{?create}
  SELECT format('CREATE ROLE %I LOGIN', :'role')
   WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'role') \gexec
  SELECT format('ALTER ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION', :'role') \gexec
  SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'role') \gexec
\endif

BEGIN;

GRANT USAGE ON SCHEMA chat TO :"role";
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES    IN SCHEMA chat TO :"role";
GRANT USAGE, SELECT, UPDATE          ON ALL SEQUENCES IN SCHEMA chat TO :"role";
GRANT EXECUTE                        ON ALL FUNCTIONS IN SCHEMA chat TO :"role";

-- Tables and sequences later chat migrations create as the owner follow.
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner" IN SCHEMA chat
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO :"role";
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner" IN SCHEMA chat
  GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO :"role";
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner" IN SCHEMA chat
  GRANT EXECUTE ON FUNCTIONS TO :"role";

COMMIT;

SELECT :'role' AS role,
       has_schema_privilege(:'role', 'chat', 'USAGE')  AS usage,
       has_schema_privilege(:'role', 'chat', 'CREATE') AS "create (must be f)",
       count(*) FILTER (WHERE has_table_privilege(:'role', c.oid, 'SELECT,INSERT,UPDATE,DELETE')) AS dml_tables,
       count(c.oid) AS tables
  FROM pg_class c
 WHERE c.relnamespace = 'chat'::regnamespace AND c.relkind IN ('r', 'p');
