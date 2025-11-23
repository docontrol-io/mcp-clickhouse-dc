-- Initialize test role for multi-tenancy testing
CREATE ROLE IF NOT EXISTS test_company;
GRANT ALL ON *.* TO test_company;

