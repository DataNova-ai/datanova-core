from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime
import subprocess
import papermill as pm

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 28),
    'retries': 1,
}

dag = DAG(
    dag_id='github_ingestion_dag',
    default_args=default_args,
    description='Clona el repositorio datanova-core, ejecuta análisis y lo envía por correo',
    schedule_interval='@daily',
    catchup=False,
)

# 1. Clonar el repositorio
clonar_repo = BashOperator(
    task_id='clonar_repositorio_github',
    bash_command='rm -rf /tmp/datanova-core && git clone https://github.com/DataNova-ai/datanova-core.git /tmp/datanova-core',
    dag=dag,
)

# 2. Ejecutar el notebook
def ejecutar_notebook():
    input_nb = '/tmp/datanova-core/BasedeDatosClientes.ipynb'
    output_nb = '/tmp/datanova-core/output/output_BasedeDatosClientes.ipynb'
    pm.execute_notebook(input_nb, output_nb)

ejecutar_colab = PythonOperator(
    task_id='ejecutar_notebook_csv',
    python_callable=ejecutar_notebook,
    dag=dag,
)

# 3. Convertir a HTML
def convertir_a_html():
    subprocess.run([
        'jupyter', 'nbconvert',
        '--to', 'html',
        '/tmp/datanova-core/output/output_BasedeDatosClientes.ipynb',
        '--output-dir', '/tmp/datanova-core/output'
    ])

convertir = PythonOperator(
    task_id='convertir_a_html',
    python_callable=convertir_a_html,
    dag=dag,
)

# 4. Enviar por email al cliente
enviar_email = EmailOperator(
    task_id='enviar_resultado_al_cliente',
    to='info.fapinco@yahoo.com',
    subject='📊 Análisis de Datos - DataNova',
    html_content="""
    <p>Estimado cliente,</p>
    <p>Adjunto encontrará el análisis de datos solicitado.</p>
    <p>Gracias por confiar en DataNova.</p>
    """,
    files=['/tmp/datanova-core/output/output_BasedeDatosClientes.html'],
    dag=dag,
)

# Flujo
clonar_repo >> ejecutar_colab >> convertir >> enviar_email
