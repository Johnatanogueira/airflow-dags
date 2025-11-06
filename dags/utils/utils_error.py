import json
from urllib import request, parse

def send_slack_error( 
  context,
  channel = 'airflow-errors',
  url_hook = 'https://hooks.slack.com/services/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'
  ):
  try:
    task_instance = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    task_id = task_instance.task_id
    execution_date = context.get('execution_date')
    error_message = context.get('exception')
    
    message = f"""
-----------------------------------------------------------------
:red_circle: ERROR :red_circle:
  
dag_id: {dag_id}
task_id: {task_id}
execution_date: {execution_date}
error_message: {error_message}
-----------------------------------------------------------------"""
    payload = json.dumps({
      "channel": channel,
      "username": dag_id.upper(),
      "icon_emoji": ":slack:",
      "text": message
    }).encode('utf-8')
    
    # data = parse.urlencode(payload).encode()
    # req =  request.Request(url_hook, data=data, method='POST')
    req = request.Request(url = url_hook, data=payload, headers={'Content-Type': 'application/json'}, method="POST")

    resp = request.urlopen(req)
    return resp
  except Exception as e:
    print(e)
    return resp
