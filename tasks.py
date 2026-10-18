from celery_app import celery_app
import time

@celery_app.task(bind=True, name=tasks.somar) 
def somar (self, a, b):
    time.sleep(3) #simula o tempo que demora pra requisição
    return a + b
    redis_client.lpush("tarefas_ids", tarefa.id)
    redis_client.ltrim("tarefas_ids", 0, 49)

@celery_app.task(bind=True, name=tasks.fatorial)
def fatorial(self, n):
    if n < 0:
        raise ValueError("Número negativo não é permitido")
    resultado = 1
    
    for i in range (2, n + 1):
        resultado *= i
    return resultado
    redis_client.lpush("tarefas_ids", tarefa.id)
    redis_client.ltrim("tarefas_ids", 0, 49)