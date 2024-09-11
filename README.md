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

# documentação de apoio 
https://docs.google.com/document/d/1dVxvhJOVxAk9Y1D04rRLgdZy18G0xMrl-qxsAyomNBI/edit#heading=h.dogl5r63nm0g

https://drive.google.com/file/d/1tEVfIW6bCIt8hCES3DYaoVwm98h2qK6y/view?usp=sharing
