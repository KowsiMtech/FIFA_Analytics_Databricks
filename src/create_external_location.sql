-- Databricks notebook source
CREATE EXTERNAL LOCATION fifa_bronze
URL 'abfss://fifa-data@fifastoragectl.dfs.core.windows.net/bronze'
WITH (STORAGE CREDENTIAL fifa_credential);

CREATE EXTERNAL LOCATION fifa_silver
URL 'abfss://fifa-data@fifastoragectl.dfs.core.windows.net/silver'
WITH (STORAGE CREDENTIAL fifa_credential);

CREATE EXTERNAL LOCATION fifa_gold
URL 'abfss://fifa-data@fifastoragectl.dfs.core.windows.net/gold'
WITH (STORAGE CREDENTIAL fifa_credential);

-- COMMAND ----------



CREATE SCHEMA IF NOT EXISTS fifa_catalog.bronze
MANAGED LOCATION 'abfss://fifa-data@fifastoragectl.dfs.core.windows.net/bronze';

CREATE SCHEMA IF NOT EXISTS fifa_catalog.silver
MANAGED LOCATION 'abfss://fifa-data@fifastoragectl.dfs.core.windows.net/silver';

CREATE SCHEMA IF NOT EXISTS fifa_catalog.gold
MANAGED LOCATION 'abfss://fifa-data@fifastoragectl.dfs.core.windows.net/gold';

-- COMMAND ----------

