# dados-bigtable-solution-for-msp

gcloud config set project {project-id}

create venv and activate

pip install requirements.txt

ajustar arquivo .env && .def

python up_services.py
python generate_and_publish_msgs.py
python etl_from_cbt_to_gbq.py

bq --location=US query --use_legacy_sql=false --external_table_definition=tmp_sensor_data::def 'select * from tmp_sensor_data'

python down_services.py
