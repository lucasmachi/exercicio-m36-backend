
from celery_app import celery_app
import time


@celery_app.task(bind=True, name="tasks.somar")
def somar(self, a, b):
    time.sleep(3)  # Simula o tempo de execução
    return a + b


@celery_app.task(bind=True, name="tasks.fatorial")
def fatorial(self, n):
    if n < 0:
        raise ValueError("Número negativo não é permitido")

    resultado = 1

    for i in range(2, n + 1):
        resultado *= i

    return resultado
