# Databricks notebook source
params = {"p_datasource": "Ergast API", "p_env": "Development"}
filenames = {
    "circuits": "1.ingest_circuits_file",
    "races": "2.ingest_races_file",
    "constructors": "3.ingest_constructors_file",
    "drivers": "4.ingest_drivers_file",
    "results": "5.ingest_results_file",
    "pit_stops": "6.ingest_pitstops_file",
    "lap_times": "7.ingest_lap_times_file",
    "qualifying": "8.ingest_qualifying_file"
}

# COMMAND ----------

for filename in filenames.values():
    v_result = dbutils.notebook.run(filename, 
                                    0, 
                                    params )
    print(v_result)

# COMMAND ----------

