from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel
from typing import Optional
import secrets
from sqlalchemy import create_engine, Column, Integer, String, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import os
import redis
from tasks import fatorial, somar
from celery_app import celery_app
from celery.result import AsyncResult
from kafka_producer import enviar_evento
import logging.config
import yaml
from elasticsearch import Elasticsearch
from datetime import datetime


es = Elasticsearch(hosts=["http://elasticsearch:9200"])
with open("logging.yaml", "r") as f:
    config = yaml.safe_load(f)
    logging.config.dictConfig(config)

logger = logging.getLogger(__name__)
logger.info("API inicializada com sucesso")


app = FastAPI(
    title="API Tarefas",
    description="API para gerenciar tarefas",
    version="1.0.0",
    contact={
        "name": "Lucas Machi",
        "email": "lucascolafati@gmail.com"
    }
)

MEU_USUARIO = os.getenv("MEU_USUARIO")
MINHA_SENHA =  os.getenv("MINHA_SENHA")
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")


redis_client = redis.Redis(
    host=REDIS_HOST,
    port=int(REDIS_PORT),
    db=0,
    decode_responses=True
)

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
security = HTTPBasic()
Base = declarative_base()

#ESSA é uma config do elasticsearch, na prática em outras empresas provavelmente vão ser outras ferramentas

ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", "tarefas-logs")
es_client = Elasticsearch([ELASTICSEARCH_URL])

class TarefasDB(Base):
    __tablename__ = "tabela_tarefas"
    id = Column(Integer, index = True, primary_key=True)
    nome = Column(String, index = True)
    descricao = Column(String, index = True)
    concluida = Column(Boolean, default = False)

class Tarefa(BaseModel):
    nome: str
    descricao: str

Base.metadata.create_all(bind=engine)

def sessao_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def autenticar_usuario(credentials: HTTPBasicCredentials = Depends(security)):
    is_user_correct = secrets.compare_digest(credentials.username, MEU_USUARIO)
    is_password_correct = secrets.compare_digest(credentials.password, MINHA_SENHA)
    
    if not (is_user_correct and is_password_correct):
        raise HTTPException (
            status_code=401,
            detail="Usuário ou senha inválidos",
            headers={"WWW-Authenticate": "Basic"}
        )


#criando duas rotas post calcular soma e fatorial


@app.post("/calcular/soma")
def calcular_soma(a: int, b: int):
    tarefa = somar.delay(a, b)

    redis_client.lpush("tarefas_ids", tarefa.id)
    redis_client.ltrim("tarefas_ids", 0, 49)

    return {
        "task_id": tarefa.id,
        "message": "Tarefa de soma enviada para execução"
    }



@app.post("/calcular/fatorial")
def calcular_fatorial(n: int):
    tarefa = fatorial.delay(n)

    redis_client.lpush("tarefas_ids", tarefa.id)
    redis_client.ltrim("tarefas_ids", 0, 49)

    return {
        "task_id": tarefa.id,
        "message": "Tarefa de fatorial enviada para execução"
    }


#criando endpoint pra ver as tarefas na fila

@app.get("/tarefas/recentes")
def listar_tarefas_recentes():
    ids = redis_client.lrange("tarefas_ids", 0, -1)
    tarefas = []

    for task_id in ids:
        resultado = AsyncResult(task_id, app=celery_app)

        tarefas.append({
            "task_id": task_id,
            "status": resultado.status,
            "resultado": resultado.result if resultado.successful() else None
        })

    return {"tarefas": tarefas}




@app.post("/adiciona")
def adicionar_tarefa(tarefa: Tarefa, db: Session = Depends(sessao_db), credentials: HTTPBasicCredentials = Depends(autenticar_usuario)):
    verifica_tarefa_existe = db.query(TarefasDB).filter(TarefasDB.nome == tarefa.nome, TarefasDB.descricao == tarefa.descricao).first()
    if verifica_tarefa_existe:
        raise HTTPException(status_code=400, detail="Essa tarefa já existe no banco de dados!")
    
    nova_tarefa = TarefasDB(nome = tarefa.nome, descricao = tarefa.descricao, concluida = False)
    db.add(nova_tarefa)
    db.commit()
    db.refresh(nova_tarefa)

    enviar_evento("tarefas_eventos", {
        "acao": "criar",
        "tarefa": tarefa.dict()
    })

    return {"mensagem": "Tarefa adicionada com sucesso", "tarefa": nova_tarefa}



@app.get("/tarefas")
def listar_tarefa(
    page: int = 1,
    limit: int = 10,
    db: Session = Depends(sessao_db),
    credentials: HTTPBasicCredentials = Depends(autenticar_usuario)
):
    if page < 1 or limit < 1:
        raise HTTPException(
            status_code=400,
            detail="Página ou limite não podem ter valores menores que 1"
        )

    tarefas_listadas = (
        db.query(TarefasDB)
        .offset((page - 1) * limit)
        .limit(limit)
        .all()
    )

    total_tarefas = db.query(TarefasDB).count()

    response = {
        "page": page,
        "limit": limit,
        "total": total_tarefas,
        "tarefas": [
            {
                "id": tarefa.id,
                "nome": tarefa.nome,
                "descricao": tarefa.descricao
            }
            for tarefa in tarefas_listadas
        ]
    }

    log = {
        "timestamp": datetime.utcnow().isoformat(),
        "endpoint": "/tarefas",
        "usuario": credentials.username,
        "page": page,
        "limit": limit,
        "status": "success" if tarefas_listadas else "not_found",
        "total_tarefas": total_tarefas
    }

    try:
        es_client.index(
            index=ELASTICSEARCH_INDEX,
            document=log
        )
    except Exception:
        logger.exception("Erro ao enviar log para o Elasticsearch")

    if not tarefas_listadas:
        response["message"] = "Não existe nenhuma tarefa"

    return response



@app.put("/atualiza/{id_tarefa}")
def atualiza_tarefa(id_tarefa: int, tarefa: Tarefa, db: Session = Depends(sessao_db), credentials: HTTPBasicCredentials = Depends(autenticar_usuario)):
    tarefa_atualizada = db.query(TarefasDB).filter(TarefasDB.id == id_tarefa).first()
    if not tarefa_atualizada:
        raise HTTPException(status_code=404, detail="Essa tarefa não existe no banco de dados!")

    tarefa_atualizada.nome = tarefa.nome
    tarefa_atualizada.descricao = tarefa.descricao
    db.commit()
    db.refresh(tarefa_atualizada)


    return {"message": "Livro atualizado com sucesso!"}

#Rota só pra marcar como concluida
@app.patch("/atualiza/{id_tarefa}/concluir")
def marcar_concluida(id_tarefa: int, db: Session = Depends(sessao_db), credentials: HTTPBasicCredentials = Depends(autenticar_usuario)):
    tarefa_concluida = db.query(TarefasDB).filter(TarefasDB.id == id_tarefa).first()
    if not tarefa_concluida:
        raise HTTPException(status_code=404, detail="Essa tarefa não existe no banco de dados!")

    tarefa_concluida.concluida = True
    db.commit()
    db.refresh(tarefa_concluida)

    return {"mensagem": "Tarefa marcada como concluída", "tarefa": tarefa_concluida}


@app.delete("/deleta/{id_tarefa}")
def deleta_tarefa(id_tarefa: int, db: Session = Depends(sessao_db), credentials: HTTPBasicCredentials = Depends(autenticar_usuario)):
    tarefa_deletada = db.query(TarefasDB).filter(TarefasDB.id == id_tarefa).first()
    if not tarefa_deletada:
        raise HTTPException(status_code=404, detail="Essa tarefa não existe no banco de dados!")

    db.delete(tarefa_deletada)
    db.commit()


    return {"message": "Tarefa deletada com sucesso!"}

@app.get("/")
def hello_world():
    logger.info("Alguém acessou a raiz da API")
    return {"Hello": "world!"}