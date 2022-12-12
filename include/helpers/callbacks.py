from airflow.utils.email import send_email

def success_email(context):
    dag_run = context.get('dag_run')

    msg = "DAG ran Successfully"
    subject = f"DAG {dag_run} has Completed"
    send_email(to='manmeet.rangoola@astronomer.io', subject=subject, html_content=msg)